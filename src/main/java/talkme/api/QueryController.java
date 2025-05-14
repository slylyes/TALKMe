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

        MoteurStockage moteurStockage = query.getTable().getMoteurStockage();

        filteredIndexes = handleConditions(query, moteurStockage);

        dataMap =  moteurStockage.select(filteredIndexes, query.getColumns(),query.getGroupBy(), query.getAggregates());


        return dataMap;
    }

    private List<Integer> handleConditions(Query query, MoteurStockage moteurStockage){
        int nbRows = query.getTable().getColumns().get(query.getColumns().get(0)).getValues().size();
        List<Integer> filteredIndexes= IntStream.range(0, nbRows).boxed().toList();

        Table t= query.getTable();
        int nbFilter =  query.getFilters().size();

        for (List<String> condition: query.getFilters()){

            filteredIndexes = switch (condition.get(1)) {
                case "=" ->
                        moteurStockage.whereEquals(t.getColumns().get(condition.get(0)), condition.get(2), filteredIndexes);
                case "<" ->
                        moteurStockage.whereLessThan(t.getColumns().get(condition.get(0)), condition.get(2), filteredIndexes);
                case ">" ->
                        moteurStockage.whereGreaterThan(t.getColumns().get(condition.get(0)), condition.get(2), filteredIndexes);
                case "!=" ->
                        moteurStockage.whereDifferent(t.getColumns().get(condition.get(0)), condition.get(2), filteredIndexes);
                default -> filteredIndexes;
            };

        }
        return  filteredIndexes;
    }

}
