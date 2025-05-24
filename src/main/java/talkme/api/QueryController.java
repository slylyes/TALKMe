package talkme.api;

import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import talkme.query.MoteurStockage;
import talkme.query.Query;
import talkme.table.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Path("/data")
public class QueryController {


    //List<List<Object>> data = parquetReader.getNextBatch();


    @POST
    @Path("/filter")
    @Produces(MediaType.APPLICATION_JSON)

    public Response filter(
            @RequestBody Query query
    ){
        System.out.println("okkkk filter");
        MoteurStockage moteurStockage = query.getTable().getMoteurStockage();

        List<Integer> filteredIndexes = handleConditions(query, moteurStockage);

        //Liste groupby vide car DistributedController s'en occupe
        List<Map<String,Object>>  dataList = moteurStockage.select(filteredIndexes, query.getColumns());

        return Response.status(Response.Status.OK).
                entity(dataList)
                .build();
    }

    private List<Integer> handleConditions(Query query, MoteurStockage moteurStockage){
        int nbRows = query.getTable().getColumns().get(query.getColumns().get(0)).getValues().size();
        List<Integer> filteredIndexes= IntStream.range(0, nbRows).boxed().toList();

        Table t= query.getTable();

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
