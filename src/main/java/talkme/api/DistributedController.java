package talkme.api;

import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import talkme.config.ConfigurationManager;
import talkme.http.HttpClient;
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
        
        if (parquetFile == null || !parquetFile.exists()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Invalid file uploaded")).build();
        }
        
        List<Future<Response>> futures = new ArrayList<>();
        
        // Process locally on the current node first
        int currentNodeId = configManager.getCurrentNodeId();
        boolean processedLocalNode = false;
        
        // Forward file to each node
        for (ConfigurationManager.NodeConfig node : configManager.getNodes()) {
            // Skip current node as it already has the file
            if (node.getId() == currentNodeId) {
                processedLocalNode = true;
                continue;
            }
            
            futures.add(executorService.submit(() -> {
                try {
                    System.out.println("Uploading file to node: " + node.getId() + " at URL: " + node.getUrl() + "/api/upload");
                    
                    // Actually send the file content to the remote node
                    StatusMessage result = HttpClient.uploadFile(
                            node, 
                            "/api/upload", 
                            parquetFile, 
                            tableName, 
                            limit,
                            StatusMessage.class);
                    
                    if (result == null) {
                        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                .entity(new StatusMessage("Received null response from node " + node.getId())).build();
                    }
                    
                    return Response.status(Response.Status.OK)
                            .entity(new StatusMessage("File processed on node " + node.getId() + ": " + result.getMessage())).build();
                } catch (Exception e) {
                    e.printStackTrace(); // Add stack trace for debugging
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity(new StatusMessage("Failed to process file on node " + node.getId() + ": " + e.getMessage())).build();
                }
            }));
        }
        
        // Collect responses
        List<Response> responses = futures.stream()
                .map(future -> {
                    try {
                        return future.get(30, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace(); // Add stack trace for debugging
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
        
        // If current node wasn't processed yet (because we skipped it in the first loop),
        // process it now using the local controller
        if (!processedLocalNode) {
            try {
                // Use the TableController directly to process the file locally
                TableController tableController = new TableController();
                Response localResponse = tableController.uploadFile(tableName, limit, parquetFile);
                
                if (localResponse.getStatus() != Response.Status.OK.getStatusCode()) {
                    return localResponse;
                }
            } catch (Exception e) {
                e.printStackTrace(); // Add stack trace for debugging
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new StatusMessage("Failed to process file on current node: " + e.getMessage())).build();
            }
        }
        
        return Response.status(Response.Status.OK)
                .entity(new StatusMessage("File successfully processed across all nodes")).build();
    }
    
    @POST
    @Path("/filter")
    @Produces(MediaType.APPLICATION_JSON)
    public List<List<Object>> filterDataAcrossNodes(@RequestBody Query query) {
        List<Future<List<List<Object>>>> futures = new ArrayList<>();
        
        // Forward query to all nodes
        for (ConfigurationManager.NodeConfig node : configManager.getNodes()) {
            futures.add(executorService.submit(() -> {
                try {
                    // Convert to a properly typed array
                    return HttpClient.post(node, "/data/filter", query, List.class);
                } catch (Exception e) {
                    System.err.println("Error querying node " + node.getId() + ": " + e.getMessage());
                    return new ArrayList<List<Object>>();
                }
            }));
        }
        
        // Merge results from all nodes
        List<List<Object>> combinedResults = new ArrayList<>();
        for (Future<List<List<Object>>> future : futures) {
            try {
                List<List<Object>> nodeResults = future.get(20, TimeUnit.SECONDS);
                if (nodeResults != null && !nodeResults.isEmpty()) {
                    combinedResults.addAll(nodeResults);
                }
            } catch (Exception e) {
                System.err.println("Error getting results from node: " + e.getMessage());
            }
        }
        
        return combinedResults;
    }
}

