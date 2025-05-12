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

    private List<List<Object>> data = new ArrayList<>();

    @GET
    @Path("/filter")
    @Produces(MediaType.APPLICATION_JSON)
    public List<List<Object>> filter(
            @RequestBody Query query
            ){
        List<Integer> filteredIndexes = handleConditions(query);
        data=  MoteurStockage.select(query.getTable(),query.getColumns(), filteredIndexes);

        return data;
    }

    private List<Integer> handleConditions(Query query){
        int nbRows = query.getTable().getColumns().get(query.getColumns().get(0)).getValues().size();
        List<Integer> filteredIndexes= IntStream.range(0, nbRows).boxed().toList();
        Table t= query.getTable();

        for (List<String> condition: query.getFilters()){

            if (condition.get(0).equals("and")) {
                filteredIndexes = switch (condition.get(2)) {
                    case "=" ->
                            MoteurStockage.whereEquals(t.getColumns().get(condition.get(1)), condition.get(3), filteredIndexes);
                    case "<" ->
                            MoteurStockage.whereLessThan(t.getColumns().get(condition.get(1)), condition.get(3), filteredIndexes);
                    case ">" ->
                            MoteurStockage.whereGreaterThan(t.getColumns().get(condition.get(1)), condition.get(3), filteredIndexes);
                    default -> filteredIndexes;
                };

            }
        }

        return  filteredIndexes;
    }

}
