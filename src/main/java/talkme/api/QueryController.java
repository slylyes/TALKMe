package talkme.api;

import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import talkme.query.MoteurStockage;
import talkme.query.Query;
import talkme.table.Table;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Path("/data")
public class QueryController {

    //List<List<Object>> data = parquetReader.getNextBatch();

    private List<Map<String, Object>> dataMap = new ArrayList<>();

    @GET
    @Path("/filter")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String, Object>> filter(
            @RequestBody Query query
    ){

        List<Integer> filteredIndexes = new ArrayList<>();

        filteredIndexes = handleConditions(query);

        List<List<Object>> data=  MoteurStockage.select(query.getTable(),query.getColumns(), filteredIndexes);

        if (query.groupByActivated()){
            List<String> columns = query.getColumns();
            List<String> groupBy = query.getGroupBy();
            List<Integer> idxCols = new ArrayList<>();

            Boolean valid = true;
            for (String col : groupBy) {
                if (columns.contains(col)) {
                    idxCols.add(columns.indexOf(col));
                }else{
                    valid = false;
                }
            }
            if (valid) {
                data = MoteurStockage.groupBy(data, idxCols);
            }
        }

        dataMap = new ArrayList<>();

        for (List<Object> ligne : data) {
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < query.getColumns().size(); i++) {
                map.put(query.getColumns().get(i), ligne.get(i));
            }
            dataMap.add(map);
        }

        return dataMap;
    }

    private List<Integer> handleConditions(Query query){
        int nbRows = query.getTable().getColumns().get(query.getColumns().get(0)).getValues().size();
        List<Integer> filteredIndexes= IntStream.range(0, nbRows).boxed().toList();

        Table t= query.getTable();
        int nbFilter =  query.getFilters().size();

        for (List<String> condition: query.getFilters()){

            filteredIndexes = switch (condition.get(1)) {
                case "=" ->
                        MoteurStockage.whereEquals(t.getColumns().get(condition.get(0)), condition.get(2), filteredIndexes);
                case "<" ->
                        MoteurStockage.whereLessThan(t.getColumns().get(condition.get(0)), condition.get(2), filteredIndexes);
                case ">" ->
                        MoteurStockage.whereGreaterThan(t.getColumns().get(condition.get(0)), condition.get(2), filteredIndexes);
                case "!=" ->
                        MoteurStockage.whereDifferent(t.getColumns().get(condition.get(0)), condition.get(2), filteredIndexes);
                default -> filteredIndexes;
            };

        }
        return  filteredIndexes;
    }

}
