package talkme.util;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ParquetJsonUtil {

    /**
     * Generates a JSON structure from a Parquet schema.
     *
     * @param schema The Parquet schema.
     * @return A map representing the JSON structure.
     */
    public static Map<String, Object> generateJsonStructure(MessageType schema) {
        Map<String, Object> responseJson = new LinkedHashMap<>();

        Map<String, Map<String, Object>> columns = new HashMap<>();
        for (Type field : schema.getFields()) {
            Map<String, Object> columnDetails = new HashMap<>();
            columnDetails.put("type", field.asPrimitiveType().getPrimitiveTypeName().name());
            columnDetails.put("values", new ArrayList<>());
            columns.put(field.getName(), columnDetails);
        }
        responseJson.put("name", "Table");
        responseJson.put("columns", columns);

        return responseJson;
    }
}