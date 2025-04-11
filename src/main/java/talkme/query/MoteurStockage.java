package talkme.query;

import io.smallrye.openapi.api.models.responses.APIResponseImpl;
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
            if (compare(convertType(col.getType(), (String) values.get(i)), convertType(col.getType(),compared)) == 0){

                selectedIndex.add(i);
            }
        }
        return selectedIndex;
    }



    public static  List<Integer> whereLessThan(Column col, String compared, List<Integer> prevSelected) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = col.getValues();

        for (int i : prevSelected) {
            if (compare(convertType(col.getType(), (String) values.get(i)), convertType(col.getType(),compared)) < 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }



    public static  List<Integer> whereGreaterThan(Column col, String compared, List<Integer> prevSelected) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = col.getValues();

        for (int i : prevSelected) {
            if (compare(convertType(col.getType(), (String) values.get(i)), convertType(col.getType(),compared)) > 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }

    private static Object convertType(String type,String val){

        return switch (type) {
            case "INT64" -> Integer.parseInt(val);
            case "DOUBLE" -> Double.parseDouble(val);
            default -> val;
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