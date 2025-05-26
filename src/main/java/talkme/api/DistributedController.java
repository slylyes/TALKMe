package talkme.api;

import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import talkme.config.ConfigurationManager;
import talkme.http.HttpClient;
import talkme.parser.ParquetParser;
import talkme.query.MoteurStockage;
import talkme.query.Query;
import talkme.table.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Path("/distributed")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DistributedController {

    private final ConfigurationManager configManager = ConfigurationManager.getInstance();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @POST
    @Path("/table")
    public Response createTableAcrossNodes(@RequestBody Table table) {
        // Create a list to hold all completable futures
        List<CompletableFuture<Response>> futures = new ArrayList<>();

        // Forward table creation request to all nodes asynchronously
        for (ConfigurationManager.NodeConfig node : configManager.getNodes()) {
            CompletableFuture<Response> future = CompletableFuture.supplyAsync(() -> {
                try {
                    Table result = HttpClient.post(node, "/api/table", table, Table.class);
                    return Response.status(Response.Status.CREATED)
                            .entity(new StatusMessage("Table created on node " + node.getId())).build();
                } catch (Exception e) {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(new StatusMessage("Failed to create table on node " + node.getId() + ": " + e.getMessage())).build();
                }
            }, executorService);

            futures.add(future);
        }

        // Combine all futures and wait for them to complete
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        try {
            // Wait for all futures to complete with a timeout
            allFutures.get(30, TimeUnit.SECONDS);

            // Collect all responses
            List<Response> responses = futures.stream()
                    .map(CompletableFuture::join)
                    .toList();

            // If any node failed, return error
            for (Response response : responses) {
                if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
                    return response;
                }
            }

            return Response.status(Response.Status.CREATED)
                    .entity(new StatusMessage("Table successfully created across all nodes")).build();
        } catch (TimeoutException e) {
            return Response.status(Response.Status.GATEWAY_TIMEOUT)
                    .entity(new StatusMessage("Timeout waiting for nodes to respond")).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new StatusMessage("Error processing node responses: " + e.getMessage())).build();
        }
    }

    @POST
    @Path("/upload")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response uploadFileAcrossNodes(
            @QueryParam("tableName") String tableName,
            @QueryParam("limit") Integer limit,
            File parquetFile) {

        if (tableName == null || tableName.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Table name is required")).build();
        }

        // Limit is now optional - if provided, must be positive
        if (limit != null && limit <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("If provided, limit must be greater than 0")).build();
        }

        if (parquetFile == null || !parquetFile.exists()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Invalid file uploaded")).build();
        }

        try {
            // Parse the parquet file once on this node
            // If limit is null, the parser will read the entire file
            ParquetParser parser = new ParquetParser(parquetFile, limit);
            List<String> columnNames = parser.getColumnNames();
            List<List<Object>> allData = parser.getNextBatch();
            parser.close();

            // Determine how many nodes we have
            List<ConfigurationManager.NodeConfig> nodes = configManager.getNodes();
            int nodeCount = nodes.size();

            if (nodeCount == 0) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new StatusMessage("No nodes configured")).build();
            }

            // Calculate how many rows each node should get
            int rowCount = 0;
            if (!allData.isEmpty() && allData.get(0) != null) {
                rowCount = allData.get(0).size();
            }

            if (rowCount == 0) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new StatusMessage("No data found in file")).build();
            }

            int rowsPerNode = rowCount / nodeCount;
            int remainderRows = rowCount % nodeCount;

            List<CompletableFuture<Response>> futures = new ArrayList<>();

            for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
                final int currentNodeIndex = nodeIndex;
                ConfigurationManager.NodeConfig node = nodes.get(nodeIndex);

                CompletableFuture<Response> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        // Calculate start and end indices for this node's data portion
                        int startRow = currentNodeIndex * rowsPerNode;
                        int endRow = startRow + rowsPerNode;

                        // Add remainder rows to the last node
                        if (currentNodeIndex == nodeCount - 1) {
                            endRow += remainderRows;
                        }

                        // Extract this node's portion of data
                        List<List<Object>> nodeData = new ArrayList<>();
                        for (List<Object> column : allData) {
                            List<Object> nodeColumn = new ArrayList<>(column.subList(startRow, endRow));
                            nodeData.add(nodeColumn);
                        }

                        // Create a data package to send to the node
                        Map<String, Object> dataPackage = new HashMap<>();
                        dataPackage.put("tableName", tableName);
                        dataPackage.put("columns", columnNames);
                        dataPackage.put("data", nodeData);

                        System.out.println("Sending data to node " + node.getId() +
                                          ": rows " + startRow + "-" + (endRow - 1));

                        // Send the data portion to this node
                        StatusMessage result = HttpClient.post(
                                node,
                                "/api/insert-data",
                                dataPackage,
                                StatusMessage.class);

                        return Response.status(Response.Status.OK)
                                .entity(new StatusMessage("Data processed on node " + node.getId() +
                                                         " (" + (endRow - startRow) + " rows)")).build();
                    } catch (Exception e) {
                        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                .entity(new StatusMessage("Failed to process data on node " + node.getId() +
                                                        ": " + e.getMessage())).build();
                    }
                }, executorService);

                futures.add(future);
            }

            // Combine all futures and wait for them to complete
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
            );

            try {
                // Wait for all futures to complete with a timeout
                allFutures.get(60, TimeUnit.SECONDS);

                // Collect all responses
                List<Response> responses = futures.stream()
                        .map(CompletableFuture::join)
                        .toList();

                // Check if any node failed
                for (Response response : responses) {
                    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                        return response;
                    }
                }

                // Create a response that includes information about how many rows were distributed
                String limitInfo = limit != null ?
                    " with a limit of " + limit + " rows" :
                    " (full file - " + rowCount + " rows)";

                return Response.status(Response.Status.OK)
                        .entity(new StatusMessage("Data successfully distributed across " + nodeCount +
                                                 " nodes" + limitInfo)).build();
            } catch (TimeoutException e) {
                return Response.status(Response.Status.GATEWAY_TIMEOUT)
                        .entity(new StatusMessage("Timeout waiting for nodes to process data")).build();
            } catch (Exception e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new StatusMessage("Error processing node responses: " + e.getMessage())).build();
            }
            
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new StatusMessage("Failed to process file: " + e.getMessage())).build();
        }
    }
    
    @GET
    @Path("/filter")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterDataAcrossNodes(@RequestBody Query query) {
        System.out.println("Received distributed filter query for: " +
                          (query.getTable() != null ? query.getTable().getName() : "unknown table"));

        List<ConfigurationManager.NodeConfig> nodes = configManager.getNodes();
        
        // Create a list to store futures for all node responses
        List<CompletableFuture<List<Map<String, Object>>>> futures = new ArrayList<>();

        // Start all node queries in parallel
        for (ConfigurationManager.NodeConfig node : nodes) {
            CompletableFuture<List<Map<String, Object>>> future = CompletableFuture.supplyAsync(() -> {
                try {
                    System.out.println("Sending query to node: " + node.getId());
                    // Use POST to send the query to each node
                    return HttpClient.post(node, "/data/filter", query, List.class);
                } catch (Exception e) {
                    System.err.println("Error querying node " + node.getId() + ": " + e.getMessage());
                    return new ArrayList<Map<String, Object>>();
                }
            }, executorService);
            
            futures.add(future);
        }

        // Create a single future that completes when all node queries complete
        CompletableFuture<List<List<Map<String, Object>>>> allResults = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(future -> {
                            try {
                                return future.get(5, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                System.err.println("Error retrieving results: " + e.getMessage());
                                return new ArrayList<Map<String, Object>>();
                            }
                        })
                        .collect(Collectors.toList()));

        try {
            // Wait for all results with timeout
            List<List<Map<String, Object>>> nodeResults = allResults.get(60, TimeUnit.SECONDS);
            
            // Merge results from all nodes
            List<Map<String, Object>> combinedResults = new ArrayList<>();
            for (List<Map<String, Object>> nodeResult : nodeResults) {
                if (nodeResult != null) {
                    combinedResults.addAll(nodeResult);
                }
            }

            System.out.println("Combined results from all nodes: " + combinedResults.size() + " rows");

            // Process group by if needed
            if (query.getGroupBy() != null && !query.getGroupBy().isEmpty()) {
                System.out.println("Applying distributed group by with " + query.getGroupBy().size() + " columns");

                // Create a temporary MoteurStockage to perform the group by operation
                Table tempTable = null;
                MoteurStockage tempMoteur = new MoteurStockage(tempTable);

                // Perform the group by operation on the combined results
                List<Map<String, Object>> groupedResults = tempMoteur.groupBy(
                    combinedResults,
                    query.getColumns(),
                    query.getGroupBy(),
                    query.getAggregates()
                );
                
                if (!query.getOrderBy().isEmpty() && query.getOrderBy() != null) {
                    groupedResults = tempMoteur.orderBy(groupedResults, query.getOrderBy(), query.getOrderDirection());
                }
                
                if (query.getLimit() != null && query.getLimit() > 0 && query.getLimit() < groupedResults.size()) {
                    groupedResults = groupedResults.subList(0, query.getLimit());
                }

                System.out.println("After distributed group by: " + groupedResults.size() + " rows");
                return Response.ok(groupedResults).build();
            } else if (!query.getAggregates().isEmpty() && query.getAggregates() != null) {
                // Check if we need to handle aggregates without group by
                Table tempTable = null;
                MoteurStockage tempMoteur = new MoteurStockage(tempTable);

                List<Map<String, Object>> groupedResults = tempMoteur.aggregationFonction(
                        combinedResults,
                        query.getColumns(),
                        query.getGroupBy(),
                        query.getAggregates()
                );

                if (!query.getOrderBy().isEmpty() && query.getOrderBy() != null) {
                    groupedResults = tempMoteur.orderBy(groupedResults, query.getOrderBy(), query.getOrderDirection());
                }
                
                if (query.getLimit() != null && query.getLimit() > 0 && query.getLimit() < groupedResults.size()) {
                    groupedResults = groupedResults.subList(0, query.getLimit());
                }

                System.out.println("After distributed aggregation: " + groupedResults.size() + " rows");
                return Response.ok(groupedResults).build();
            }

            // Apply ordering if needed
            if (!query.getOrderBy().isEmpty() && query.getOrderBy() != null) {
                Table tempTable = null;
                MoteurStockage tempMoteur = new MoteurStockage(tempTable);
                combinedResults = tempMoteur.orderBy(combinedResults, query.getOrderBy(), query.getOrderDirection());
            }
            
            // Apply limit if needed
            if (query.getLimit() != null && query.getLimit() > 0 && query.getLimit() < combinedResults.size()) {
                combinedResults = combinedResults.subList(0, query.getLimit());
            }

            return Response.ok(combinedResults).build();
            
        } catch (TimeoutException e) {
            return Response.status(Response.Status.GATEWAY_TIMEOUT)
                    .entity(new StatusMessage("Timeout waiting for nodes to respond")).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new StatusMessage("Error processing query: " + e.getMessage())).build();
        }
    }
}

