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

    private List<List<Object>> data = new ArrayList<>();

    public QueryController() {
        List<Object> names = new ArrayList<>(Arrays.asList("Alice", "Bob", "Charlie", "David"));
        List<Object> ages = new ArrayList<>(Arrays.asList(25, 30, 28, 35));
        data.add(names);
        data.add(ages);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<List<Object>> select(@QueryParam("colonnes") String colonnes) {
        if (colonnes == null) {
            return data; // On return toutes les données si aucune colonne spécifiée
        }

        List<List<Object>> res = new ArrayList<>();
        List<String> col_spec = List.of(colonnes.split(","));
        for(String col: col_spec){
            if(Objects.equals(col, "name")){
                res.add(data.get(0));
            } else if (Objects.equals(col, "age")) {
                res.add(data.get(1));
            }
        }
        return res;

    }

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
