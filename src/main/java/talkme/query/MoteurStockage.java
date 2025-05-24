package talkme.query;


import org.apache.parquet.io.api.Binary;
import talkme.table.ColonnesException;
import talkme.table.Column;
import talkme.table.Table;

import java.util.*;

public class MoteurStockage {
    private final Table table;

    public MoteurStockage(Table table) {
        this.table = table;
    }

    public void insert(List<String> cols, List<List<Object>> data) throws ColonnesException {
        //Vérification de la structure donnée par l'utilisateur
        for (String col: cols){
            if (!table.getColumns().containsKey(col)){
                throw new ColonnesException(cols);
            }
        }
        //Ajout des données
        for (int i=0; i<cols.size(); i++){
            table.getColumns().get(cols.get(i)).getValues().addAll(data.get(i));
        }
    }

    public List<Map<String,Object>> select(List<Integer> index, List<String> colSelect) {


        List<Map<String,Object>> result = new ArrayList<>();

        for (Integer i : index) {
            Map<String,Object> element = new HashMap<>();
            for (String col : colSelect) {
                element.put(col, table.getColumns().get(col).getValues().get(i));
            }
            result.add(element);
        }
        return result;
    }

    public List<Map<String, Object>> groupBy(List<Map<String, Object>> selectValues, List<String> colSelect, List<String> columnsGroupBy, List<Map<String, String>> aggregates) {
        if (columnsGroupBy.isEmpty()){
            return selectValues;
        }

        for (String col : columnsGroupBy) {
            if (!colSelect.contains(col)) {
                throw new IllegalArgumentException("Une des colonnes du groupby n'est pas dans le select.");
            }
        }

        List<Map<String, Object>> resultSet = aggregationFonction(selectValues, colSelect, columnsGroupBy, aggregates);

        return new ArrayList<>(resultSet);
    }

    public List<Map<String, Object>> aggregationFonction(List<Map<String, Object>> selectValues, List<String> colSelect, List<String> columnsGroupBy, List<Map<String, String>> aggregates) {
        if (!aggregates.isEmpty()) {
            for (Map<String, String> agg : aggregates) {
                String column = agg.get("column");

                if ( !colSelect.contains(column) && !column.equals("*") ) {
                    throw new IllegalArgumentException("Une des colonnes des aggrégations n'est pas dans le select.");

                }
            }
        }

        Set<Map<String, Object>> resultSet = new HashSet<>();

        Map<Map<String, Object>, List<Integer>> mapAggregation = new HashMap<>();

        int size = selectValues.size();
        for (int i = 0; i < size; i++) {
            Map<String, Object> row = selectValues.get(i);
            Map<String, Object> rowGroupBy = new HashMap<>();
            for (String col : row.keySet()) {
                if (columnsGroupBy.contains(col)) {
                    rowGroupBy.put(col, row.get(col));
                }
            }
            resultSet.add(rowGroupBy);
            if (!aggregates.isEmpty()) {
                List<Integer> listIdx = mapAggregation.get(rowGroupBy);
                if (listIdx == null) {
                    listIdx = new ArrayList<>();
                }
                listIdx.add(i);
                mapAggregation.put(rowGroupBy, listIdx);
            }
        }

        if (!mapAggregation.isEmpty()) {
            resultSet = dispatcherAggregation(aggregates, mapAggregation, selectValues);
        }
        return new ArrayList<>(resultSet);
    }

    public Set<Map<String, Object>> dispatcherAggregation(List<Map<String, String>> aggregates, Map<Map<String, Object>, List<Integer>> mapAggregation, List<Map<String, Object>> selectValues) {

        for (Map<String, String> agg : aggregates) {
            String function = agg.get("function");
            String column = agg.get("column");
            if (function != null && column != null) {
                switch (function) {
                    case "SUM"   ->  mapAggregation = sumAggregation(mapAggregation, column, selectValues);
                    case "COUNT"   ->  mapAggregation = countAggregation(mapAggregation);
                    case "AVG"   ->  mapAggregation = averageAggregation(mapAggregation, column, selectValues);
                    default        -> throw new IllegalArgumentException("Fonction d'aggregation non pris en charge.");
                }
            }
        }

        return mapAggregation.keySet();
    }

    public Map<Map<String, Object>, List<Integer>> sumAggregation(Map<Map<String, Object>, List<Integer>> mapAggregation, String column, List<Map<String, Object>> selectValues) {
        double sum = 0;
        Map<Map<String, Object>, List<Integer>> mapAggregationCopy = new HashMap<>(mapAggregation);
        for (Map.Entry<Map<String, Object>, List<Integer>> entry : mapAggregationCopy.entrySet()) {
            for (Integer i : entry.getValue()){
                Object value = selectValues.get(i).get(column);
                if (value instanceof Number) {
                    sum += ((Number) value).doubleValue();
                }

            }
            mapAggregation.remove(entry.getKey(), entry.getValue());
            Map<String, Object> newKey = new HashMap<>(entry.getKey());
            newKey.put("sum_"+column, sum);
            mapAggregation.put(newKey, entry.getValue());

            //doubler la map key puis supprimer la map, ajouter la sum et la réajouter a la grosse map
        }
        return mapAggregation;
    }

    public Map<Map<String, Object>, List<Integer>> averageAggregation(Map<Map<String, Object>, List<Integer>> mapAggregation, String column, List<Map<String, Object>> selectValues) {
        double average = 0;
        Map<Map<String, Object>, List<Integer>> mapAggregationCopy = new HashMap<>(mapAggregation);
        for (Map.Entry<Map<String, Object>, List<Integer>> entry : mapAggregationCopy.entrySet()) {
            for (Integer i : entry.getValue()){
                Object value = selectValues.get(i).get(column);
                if (value instanceof Number) {
                    average += ((Number) value).doubleValue();
                }

            }
            average = average / entry.getValue().size();
            mapAggregation.remove(entry.getKey(), entry.getValue());
            Map<String, Object> newKey = new HashMap<>(entry.getKey());
            newKey.put("avg_"+column, average);
            mapAggregation.put(newKey, entry.getValue());

            //doubler la map key puis supprimer la map, ajouter la sum et la réajouter a la grosse map
        }
        return mapAggregation;
    }

    public Map<Map<String, Object>, List<Integer>> countAggregation(Map<Map<String, Object>, List<Integer>> mapAggregation) {
        int count;
        Map<Map<String, Object>, List<Integer>> mapAggregationCopy = new HashMap<>(mapAggregation);
        for (Map.Entry<Map<String, Object>, List<Integer>> entry : mapAggregationCopy.entrySet()) {
            count = entry.getValue().size();
            mapAggregation.remove(entry.getKey(), entry.getValue());
            Map<String, Object> newKey = new HashMap<>(entry.getKey());
            newKey.put("count(*)", count);
            mapAggregation.put(newKey, entry.getValue());

            //doubler la map key puis supprimer la map, ajouter la sum et la réajouter a la grosse map
        }
        return mapAggregation;
    }


    public List<Integer> whereEquals(Column col, String compared, List<Integer> prevSelected){

        List<Integer> selectedIndex= new ArrayList<>();
        List<Object> values= col.getValues();

        for (int i: prevSelected){
            if (compare(values.get(i), convertToParquetType(col.getType(),compared))  == 0){

                selectedIndex.add(i);
            }
        }
        return selectedIndex;
    }

    public List<Integer> whereDifferent(Column col, String compared, List<Integer> prevSelected){

        List<Integer> selectedIndex= new ArrayList<>();
        List<Object> values= col.getValues();

        for (int i: prevSelected){

            if (compare(values.get(i), convertToParquetType(col.getType(),compared))  != 0){


                selectedIndex.add(i);
            }
        }
        return selectedIndex;
    }



    public List<Integer> whereLessThan(Column col, String compared, List<Integer> prevSelected) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = col.getValues();

        for (int i : prevSelected) {
            if (compare(values.get(i), convertToParquetType(col.getType(),compared))  < 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }



    public List<Integer> whereGreaterThan(Column col, String compared, List<Integer> prevSelected) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = col.getValues();

        for (int i : prevSelected) {
            if (compare(values.get(i), convertToParquetType(col.getType(),compared)) > 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }


    private Object convertToParquetType(String type, String val) {

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

    @SuppressWarnings("unchecked")
    private static int compare(Object a, Object b) {
        // Si les deux objets sont Binary, comparer leur contenu UTF-8
        if (a instanceof Binary && b instanceof Binary) {
            return ((Binary) a).toStringUsingUTF8().compareTo(((Binary) b).toStringUsingUTF8());
        }

        // Si un des deux est Binary, transformer en String pour comparaison
        if (a instanceof Binary && b instanceof String) {
            return ((Binary) a).toStringUsingUTF8().compareTo((String) b);
        }
        if (a instanceof String && b instanceof Binary) {
            return ((String) a).compareTo(((Binary) b).toStringUsingUTF8());
        }

        if (a instanceof Number && b instanceof Number) {
            double da = ((Number) a).doubleValue();
            double db = ((Number) b).doubleValue();
            return Double.compare(da, db);
        }

        // Traitement standard Comparable
        if (a instanceof Comparable && b instanceof Comparable && a.getClass().equals(b.getClass())) {
            return ((Comparable<Object>) a).compareTo(b);
        }

        throw new IllegalArgumentException("Objects are not comparable");
    }

}