package talkme.query;

import io.smallrye.openapi.api.models.responses.APIResponseImpl;
import org.apache.parquet.io.api.Binary;
import talkme.table.ColonnesException;
import talkme.table.Column;
import talkme.table.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MoteurStockage {

    public static  void insert(Table t, List<String> cols, List<List<Object>> data) throws ColonnesException {
        //Vérification de la structure donnée par l'utilisateur
        for (String col: cols){
            if (!t.getColumns().containsKey(col)){
                System.out.println(cols.toString());
                throw new ColonnesException(cols);
            }
        }
        //Ajout des données
        for (int i=0; i<cols.size(); i++){
            t.getColumns().get(cols.get(i)).getValues().addAll(data.get(i));
        }
    }

    public static List<List<Object>> select(Table t, List<String> cols, List<Integer> index) {

        List<List<Object>> result = new ArrayList<>();

        for (Integer i : index) {
            List<Object> row = new ArrayList<>();
            for (String col : cols) {
                row.add(t.getColumns().get(col).getValues().get(i));
            }
            result.add(row);
        }

        return result;
    }


    public static  List<Integer> whereEquals(Column col, String compared, List<Integer> prevSelected){

        List<Integer> selectedIndex= new ArrayList<>();
        List<Object> values= col.getValues();

        for (int i: prevSelected){
            if (compare(values.get(i), convertToParquetType(col.getType(),compared))  == 0){

                selectedIndex.add(i);
            }
        }
        return selectedIndex;
    }



    public static  List<Integer> whereLessThan(Column col, String compared, List<Integer> prevSelected) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = col.getValues();

        for (int i : prevSelected) {
            if (compare(values.get(i), convertToParquetType(col.getType(),compared))  < 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }



    public static  List<Integer> whereGreaterThan(Column col, String compared, List<Integer> prevSelected) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = col.getValues();

        for (int i : prevSelected) {
            if (compare(values.get(i), convertToParquetType(col.getType(),compared)) > 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }

    private static Object convertToParquetType(String type, String val) {
        if (val == null) return null;

        return switch (type.toUpperCase()) {
            case "INT32"   -> Integer.parseInt(val);
            case "INT64"   -> Long.parseLong(val);
            case "FLOAT"   -> Float.parseFloat(val);
            case "DOUBLE"  -> Double.parseDouble(val);
            case "BOOLEAN" -> Boolean.parseBoolean(val);
            case "BINARY"  -> Binary.fromString(val);
            default        -> throw new IllegalArgumentException("Unsupported Parquet type: " + type);
        };
    }

    // Utility method for comparison
    @SuppressWarnings("unchecked")
    private static  int compare(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        throw new IllegalArgumentException("Objects are not comparable");
    }
}