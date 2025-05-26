package talkme.api;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import com.fasterxml.jackson.databind.ObjectMapper;
import talkme.parser.ParquetParser;
import talkme.table.ColonnesException;
import talkme.table.Database;
import talkme.table.SameNameException;
import talkme.table.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.parquet.schema.Type;


import static talkme.table.Database.tableMap;

@Path("/internal")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableController {
    @POST
    @Path("/table")
    public Response create(@RequestBody Table table) {
        if(table == null || table.getName() == null || table.getName().isEmpty()){
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Invalid table name")).build();
        }

        // Ajout de la table dans la Map contenant toutes les tables
        try {
            Database.add(table);
        }catch (SameNameException e){
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Table with same name already exists")).build();
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

    @GET
    @Path("/analyze-parquet")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response analyzeParquetSchema(
            @QueryParam("tableName") String tableName,
            File parquetFile) {

        if (tableName == null || tableName.isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Table name is required")).build();
        }

        if (parquetFile == null || !parquetFile.exists()) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Invalid file uploaded")).build();
        }

        try {
            // Parse the parquet file to extract schema information
            ParquetParser parser = new ParquetParser(parquetFile, null);
            List<String> columnNames = parser.getColumnNames();
            List<Type> columnTypes = parser.getColumnTypes();
            parser.close();

            if (columnNames.isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new StatusMessage("No columns found in the Parquet file")).build();
            }

            // Create table schema structure
            Map<String, Object> tableSchema = new HashMap<>();
            tableSchema.put("name", tableName);

            List<Map<String, String>> columns = new ArrayList<>();
            for (int i = 0; i < columnNames.size(); i++) {
                Map<String, String> column = new HashMap<>();
                column.put("name", columnNames.get(i));

                // Convert Parquet type to our type system
                String type = mapParquetTypeToTableType(columnTypes.get(i));
                column.put("type", type);

                columns.add(column);
            }

            tableSchema.put("columns", columns);

            return Response.status(Response.Status.OK)
                    .entity(tableSchema).build();

        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new StatusMessage("Failed to analyze Parquet file: " + e.getMessage()))
                    .build();
        }
    }

    /**
     * Maps a Parquet type to the corresponding type used in our table system
     *
     * @param parquetType The Parquet type to map
     * @return The corresponding type string for our table system
     */
    private String mapParquetTypeToTableType(Type parquetType) {
        String typeName = parquetType.asPrimitiveType().getPrimitiveTypeName().name();

        return switch (typeName) {
            case "INT32" -> "INT32";
            case "INT64" -> "INT64";
            case "INT96" -> "INT64";
            case "FLOAT" -> "FLOAT";
            case "DOUBLE" -> "DOUBLE";
            case "BOOLEAN" -> "BOOLEAN";
            case "BINARY", "FIXED_LEN_BYTE_ARRAY" -> "BINARY";
            default -> "BINARY"; // Default to BINARY for unknown types
        };
    }
}
