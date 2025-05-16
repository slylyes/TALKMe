package talkme.api;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import com.fasterxml.jackson.databind.ObjectMapper;
import talkme.parser.ParquetParser;
import talkme.table.ColonnesException;
import talkme.table.Database;
import talkme.table.SameNameException;
import talkme.table.Table;

import javax.ws.rs.Consumes;
import javax.ws.rs.QueryParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;


import static talkme.table.Database.tableMap;

@Path("/api")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableController {
    @POST
    @Path("/table")
    public Response create(@RequestBody Table table) {
        if(table == null || table.getName() == null || table.getName().isEmpty()){
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Nom de table invalide")).build();
        }

        // Ajout de la table dans la Map contenant toutes les tables
        try {
            Database.add(table);
        }catch (SameNameException e){
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Table de même nom existe déjà")).build();
        }

        return Response.status(Response.Status.CREATED)
                .entity(table).build();
    }

    @POST
    @Path("/insert-data")
    public Response insertDistributedData(Map<String, Object> dataPackage) {
        String tableName = (String) dataPackage.get("tableName");
        
        // Validate table existence
        if (!tableMap.containsKey(tableName)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Table does not exist")).build();
        }

        Table table = tableMap.get(tableName);
        
        try {
            @SuppressWarnings("unchecked")
            List<String> columns = (List<String>) dataPackage.get("columns");
            
            @SuppressWarnings("unchecked")
            List<List<Object>> data = (List<List<Object>>) dataPackage.get("data");
            
            int rowCount = 0;
            if (data.size() > 0 && data.get(0) != null) {
                rowCount = data.get(0).size();
            }
            
            table.getMoteurStockage().insert(columns, data);
            
            return Response.status(Response.Status.OK)
                    .entity(new StatusMessage("Successfully inserted " + rowCount + " rows into table " + tableName))
                    .build();
        } catch (ColonnesException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Column mismatch: " + e.getMessage()))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new StatusMessage("Failed to insert data: " + e.getMessage()))
                    .build();
        }
    }
}
