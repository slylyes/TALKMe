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

@Path("/api")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DistributedController {

    private final ConfigurationManager configManager = ConfigurationManager.getInstance();
    
    // Use a cached thread pool that can grow/shrink as needed with a maximum size
    private final ExecutorService executorService = Executors.newCachedThreadPool(
        new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = defaultFactory.newThread(r);
                thread.setName("node-request-" + thread.getId());
                return thread;
            }
        }
    );
    
    // Add a separate executor for CPU-intensive tasks like data processing
    private final ExecutorService dataProcessingExecutor = Executors.newFixedThreadPool(
        Math.max(2, Runtime.getRuntime().availableProcessors() - 1),
        r -> {
            Thread t = new Thread(r);
            t.setName("data-processor-" + t.getId());
            return t;
        }
    );
    
    // Implement shutdown hook to properly close thread pools
    {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdownExecutors();
        }));
    }
    
    // Method to cleanly shut down the executors
    private void shutdownExecutors() {
        try {
            System.out.println("Shutting down thread pools...");
            executorService.shutdown();
            dataProcessingExecutor.shutdown();
            
            // Wait for tasks to terminate
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            if (!dataProcessingExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                dataProcessingExecutor.shutdownNow();
            }
            System.out.println("Thread pools terminated");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
            dataProcessingExecutor.shutdownNow();
        }
    }

    @POST
    @Path("/table")
    public Response createTableAcrossNodes(@RequestBody Table table) {
        // Create a list to hold all completable futures
        List<CompletableFuture<Response>> futures = new ArrayList<>();
        List<ConfigurationManager.NodeConfig> nodes = configManager.getNodes();
        
        if (nodes.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new StatusMessage("No nodes configured")).build();
        }

        // Forward table creation request to all nodes asynchronously
        for (ConfigurationManager.NodeConfig node : nodes) {
            CompletableFuture<Response> future = CompletableFuture.supplyAsync(() -> {
                try {
                    Table result = HttpClient.post(node, "/internal/table", table, Table.class);
                    return Response.status(Response.Status.CREATED)
                            .entity(new StatusMessage("Table created on node " + node.getId())).build();
                } catch (Exception e) {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(new StatusMessage("Failed to create table on node " + node.getId() + ": " + e.getMessage())).build();
                }
            }, executorService);

            futures.add(future);
        }

        return processNodeResponses(futures, "Table successfully created across all nodes", 30);
    }

    // Helper method to process node responses and consolidate error handling
    private Response processNodeResponses(List<CompletableFuture<Response>> futures, String successMessage, int timeoutSeconds) {
        // Combine all futures and wait for them to complete
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        try {
            // Wait for all futures to complete with a timeout
            allFutures.get(timeoutSeconds, TimeUnit.SECONDS);

            // Collect all responses
            List<Response> responses = futures.stream()
                    .map(CompletableFuture::join)
                    .toList();

            // Check if any node failed
            for (Response response : responses) {
                if (response.getStatus() >= 400) {
                    return response;
                }
            }

            return Response.status(Response.Status.OK)
                    .entity(new StatusMessage(successMessage)).build();
        } catch (TimeoutException e) {
            // Try to cancel any pending futures
            futures.forEach(f -> f.cancel(true));
            
            return Response.status(Response.Status.GATEWAY_TIMEOUT)
                    .entity(new StatusMessage("Timeout waiting for nodes to respond after " + timeoutSeconds + " seconds")).build();
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
            // Use the data processing executor for CPU-intensive file parsing
            CompletableFuture<List<List<Object>>> dataFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    // Parse the parquet file once on this node
                    ParquetParser parser = new ParquetParser(parquetFile, limit);
                    List<List<Object>> allData = parser.getNextBatch();
                    parser.close();
                    return allData;
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, dataProcessingExecutor);
            
            CompletableFuture<List<String>> columnsFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    ParquetParser parser = new ParquetParser(parquetFile, limit);
                    List<String> columnNames = parser.getColumnNames();
                    parser.close();
                    return columnNames;
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, dataProcessingExecutor);
            
            List<ConfigurationManager.NodeConfig> nodes = configManager.getNodes();
            int nodeCount = nodes.size();

            if (nodeCount == 0) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new StatusMessage("No nodes configured")).build();
            }
            
            List<String> columnNames;
            List<List<Object>> allData;
            
            try {
                // Wait for parsing to complete with timeout
                columnNames = columnsFuture.get(30, TimeUnit.SECONDS);
                allData = dataFuture.get(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                return Response.status(Response.Status.GATEWAY_TIMEOUT)
                        .entity(new StatusMessage("Timeout parsing Parquet file")).build();
            } catch (ExecutionException e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new StatusMessage("Failed to process file: " + e.getCause().getMessage())).build();
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
                                "/internal/insert-data",
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

            // Process responses with the helper method
            String successMessage = "Data successfully distributed across " + nodeCount +
                                    " nodes" + (limit != null ? 
                                    " with a limit of " + limit + " rows" :
                                    " (full file - " + rowCount + " rows)");
            
            return processNodeResponses(futures, successMessage, 60);
            
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
        
        if (nodes.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new StatusMessage("No nodes configured")).build();
        }
        
        // Create a list to store futures for all node responses
        List<CompletableFuture<List<Map<String, Object>>>> futures = new ArrayList<>();

        // Start all node queries in parallel with timeout for each individual request
        for (ConfigurationManager.NodeConfig node : nodes) {
            CompletableFuture<List<Map<String, Object>>> future = CompletableFuture.supplyAsync(() -> {
                try {
                    System.out.println("Sending query to node: " + node.getId());
                    // Use POST to send the query to each node
                    List<?> rawResponse = HttpClient.post(node, "/data/filter", query, List.class);
                    
                    // Properly convert each item in the list to Map<String, Object>
                    List<Map<String, Object>> typedResult = new ArrayList<>();
                    for (Object item : rawResponse) {
                        if (item instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> mapItem = (Map<String, Object>) item;
                            typedResult.add(mapItem);
                        }
                    }
                    
                    return typedResult;
                } catch (Exception e) {
                    System.err.println("Error querying node " + node.getId() + ": " + e.getMessage());
                    // Return empty list instead of throwing to allow partial results
                    return new ArrayList<Map<String, Object>>();
                }
            }, executorService)
            // Add timeout for each individual node request
            .orTimeout(15, TimeUnit.SECONDS)
            .exceptionally(e -> {
                if (e instanceof TimeoutException) {
                    System.err.println("Timeout querying node: " + e.getMessage());
                } else {
                    System.err.println("Error in node query: " + e.getMessage());
                }
                return new ArrayList<>();
            });
            
            futures.add(future);
        }

        // Create a single future that completes when all node queries complete
        CompletableFuture<List<List<Map<String, Object>>>> allResults = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));

        try {
            // Wait for all results with timeout
            List<List<Map<String, Object>>> nodeResults = allResults.get(60, TimeUnit.SECONDS);
            
            // Use the data processing executor for CPU-intensive operations on the results
            return CompletableFuture.supplyAsync(() -> {
                // Merge results from all nodes
                List<Map<String, Object>> combinedResults = new ArrayList<>();
                for (List<Map<String, Object>> nodeResult : nodeResults) {
                    if (nodeResult != null) {
                        combinedResults.addAll(nodeResult);
                    }
                }

                System.out.println("Combined results from all nodes: " + combinedResults.size() + " rows");
                
                // Process using MoteurStockage for all post-processing
                Table tempTable = null;
                MoteurStockage tempMoteur = new MoteurStockage(tempTable);
                List<Map<String, Object>> processedResults = combinedResults;

                // Process group by if needed
                if (query.getGroupBy() != null && !query.getGroupBy().isEmpty()) {
                    System.out.println("Applying distributed group by with " + query.getGroupBy().size() + " columns");
                    processedResults = tempMoteur.groupBy(
                        combinedResults,
                        query.getColumns(),
                        query.getGroupBy(),
                        query.getAggregates()
                    );
                } else if (!query.getAggregates().isEmpty() && query.getAggregates() != null) {
                    // Handle aggregates without group by
                    processedResults = tempMoteur.aggregationFonction(
                            combinedResults,
                            query.getColumns(),
                            query.getGroupBy(),
                            query.getAggregates()
                    );
                }

                // Apply ordering if needed
                if (!query.getOrderBy().isEmpty() && query.getOrderBy() != null) {
                    processedResults = tempMoteur.orderBy(processedResults, query.getOrderBy(), query.getOrderDirection());
                }
                
                // Apply limit if needed
                if (query.getLimit() != null && query.getLimit() > 0 && query.getLimit() < processedResults.size()) {
                    processedResults = processedResults.subList(0, query.getLimit());
                }

                System.out.println("Final result set: " + processedResults.size() + " rows");
                return Response.ok(processedResults).build();
            }, dataProcessingExecutor).get(30, TimeUnit.SECONDS);
            
        } catch (TimeoutException e) {
            // Cancel remaining futures
            futures.forEach(f -> f.cancel(true));
            
            return Response.status(Response.Status.GATEWAY_TIMEOUT)
                    .entity(new StatusMessage("Timeout processing query results")).build();
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new StatusMessage("Error processing query: " + e.getMessage())).build();
        }
    }
}
