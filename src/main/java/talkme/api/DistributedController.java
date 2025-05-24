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

@Path("/distributed")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class DistributedController {
    
    private final ConfigurationManager configManager = ConfigurationManager.getInstance();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    
    @POST
    @Path("/table")
    public Response createTableAcrossNodes(@RequestBody Table table) {
        List<Future<Response>> futures = new ArrayList<>();

        // Forward table creation request to all nodes
        for (ConfigurationManager.NodeConfig node : configManager.getNodes()) {
            futures.add(executorService.submit(() -> {
                try {
                    Table result = HttpClient.post(node, "/api/table", table, Table.class);

                    return Response.status(Response.Status.CREATED)
                            .entity(new StatusMessage("Table created on node " + node.getId())).build();
                } catch (Exception e) {
                    e.printStackTrace();
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(new StatusMessage("Failed to create table on node " + node.getId() + ": " + e.getMessage())).build();
                }
            }));
        }

        // Collect responses
        List<Response> responses = futures.stream()
                .map(future -> {
                    try {
                        return future.get(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                .entity(new StatusMessage("Error waiting for node response: " + e.getMessage())).build();
                    }
                })
                .toList();

        // If any node failed, return error
        for (Response response : responses) {
            if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
                return response;
            }
        }

        return Response.status(Response.Status.CREATED)
                .entity(new StatusMessage("Table successfully created across all nodes")).build();
    }
    
    @POST
    @Path("/upload")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response uploadFileAcrossNodes(
            @QueryParam("tableName") String tableName,
            @QueryParam("limit") int limit,
            File parquetFile) {
        
        if (tableName == null || tableName.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Table name is required")).build();
        }

        if (limit <= 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Limit must be greater than 0")).build();
        }
        
        if (parquetFile == null || !parquetFile.exists()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Invalid file uploaded")).build();
        }
        
        try {
            // Parse the parquet file once on this node
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
            if (allData.size() > 0 && allData.get(0) != null) {
                rowCount = allData.get(0).size();
            }
            
            if (rowCount == 0) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new StatusMessage("No data found in file")).build();
            }
            
            int rowsPerNode = rowCount / nodeCount;
            int remainderRows = rowCount % nodeCount;
            
            List<Future<Response>> futures = new ArrayList<>();
            
            for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
                final int currentNodeIndex = nodeIndex;
                ConfigurationManager.NodeConfig node = nodes.get(nodeIndex);
                
                futures.add(executorService.submit(() -> {
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
                }));
            }
            
            // Collect responses
            List<Response> responses = futures.stream()
                    .map(future -> {
                        try {
                            return future.get(30, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                    .entity(new StatusMessage("Error waiting for node response: " + e.getMessage())).build();
                        }
                    })
                    .toList();
            
            // Check if any node failed
            for (Response response : responses) {
                if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                    return response;
                }
            }
            
            return Response.status(Response.Status.OK)
                    .entity(new StatusMessage("Data successfully distributed across all nodes")).build();
            
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new StatusMessage("Failed to process file: " + e.getMessage())).build();
        }
    }
    
    @GET
    @Path("/filter")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response filterDataAcrossNodes(@RequestBody Query query) {

        List<Future<List<Map<String, Object>>>> futures = new ArrayList<>();

        System.out.println("Received distributed filter query for: " +
                           (query.getTable() != null ? query.getTable().getName() : "unknown table"));

        // Forward query to all nodes
        for (ConfigurationManager.NodeConfig node : configManager.getNodes()) {
            futures.add(executorService.submit(() -> {
                try {
                    System.out.println("Sending query to node: " + node.getId());
                    // Use POST to send the query to each node
                    return HttpClient.post(node, "/data/filter", query, List.class);
                } catch (Exception e) {
                    System.err.println("Error querying node " + node.getId() + ": " + e.getMessage());
                    e.printStackTrace();
                    return new ArrayList<Map<String, Object>>();
                }
            }));
        }

        // Merge results from all nodes
        List<Map<String, Object>> combinedResults = new ArrayList<>();
        boolean hasErrors = false;
        StringBuilder errorMessages = new StringBuilder("Errors occurred when querying nodes: ");

        for (Future<List<Map<String, Object>>> future : futures) {
            try {
                List<Map<String, Object>> nodeResults = future.get(30, TimeUnit.SECONDS);
                if (nodeResults != null) {
                    combinedResults.addAll(nodeResults);
                }
            } catch (Exception e) {
                hasErrors = true;
                errorMessages.append(e.getMessage()).append("; ");
                System.err.println("Error getting results from node: " + e.getMessage());
                e.printStackTrace();
            }
        }

        if (hasErrors && combinedResults.isEmpty()) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new StatusMessage(errorMessages.toString())).build();
        }

        System.out.println("Combined results from all nodes: " + combinedResults.size() + " rows");

        System.out.println(query.getAggregates());


        // Check if we need to handle group by
        if (query.getGroupBy() != null && !query.getGroupBy().isEmpty()) {
            System.out.println("Applying distributed group by with " + query.getGroupBy().size() + " columns");

            // Create a temporary MoteurStockage to perform the group by operation
            // Since we need a Table, we'll create a minimal one just for this operation
            Table tempTable = null;
            MoteurStockage tempMoteur = new MoteurStockage(tempTable);

            // Perform the group by operation on the combined results
            List<Map<String, Object>> groupedResults = tempMoteur.groupBy(
                combinedResults,
                query.getColumns(),
                query.getGroupBy(),
                query.getAggregates()
            );
            if (!query.getOrderBy().isEmpty() && query.getOrderBy() != null){
                groupedResults = tempMoteur.orderBy(groupedResults,query.getOrderBy(), query.getOrderDirection());
            }
            System.out.println("After distributed group by: " + groupedResults.size() + " rows");
            return Response.ok(groupedResults).build();
        }else {
            // Check if we need to handle aggregates whith out group by
            if (!query.getAggregates().isEmpty() && query.getAggregates() != null){
                Table tempTable = null;
                MoteurStockage tempMoteur = new MoteurStockage(tempTable);

                List<Map<String, Object>> groupedResults = tempMoteur.aggregationFonction(
                        combinedResults,
                        query.getColumns(),
                        query.getGroupBy(),
                        query.getAggregates()
                );

                if (!query.getOrderBy().isEmpty() && query.getOrderBy() != null){
                    groupedResults = tempMoteur.orderBy(groupedResults, query.getOrderBy(), query.getOrderDirection());
                }

                System.out.println("After distributed aggregation: " + groupedResults.size() + " rows");
                return Response.ok(groupedResults).build();

            }
        }

        if (!query.getOrderBy().isEmpty() && query.getOrderBy() != null){
            Table tempTable = null;
            MoteurStockage tempMoteur = new MoteurStockage(tempTable);
            combinedResults = tempMoteur.orderBy(combinedResults, query.getOrderBy(), query.getOrderDirection());
        }
        return Response.ok(combinedResults).build();
    }
}
