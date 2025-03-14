package talkme.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
            if(col == "name"){
                res.add(data.get(0));
            } else if (col == "age") {
                res.add(data.get(1));
            }
        }
        return res;

    }

    @GET
    @Path("/filter")
    @Produces(MediaType.APPLICATION_JSON)
    public List<List<Object>> where(
            @QueryParam("colonne") String colonne,
            @QueryParam("operateur") String operateur,
            @QueryParam("valeur") String valeur
    ){
        /*Exemple de parametres:
            colonne: "age"
            operateur: ">="
            valeur: "20"
         */
        return data;
    }

//    @GET
//    @Path("/filter")
//    @Produces(MediaType.APPLICATION_JSON)
//    //Creer nouvelle classe pour resultat de cette fonction?
//    public List<Map<String, List<List<Object>>>> groupBy(
//            @QueryParam("colonne") String colonne
//    ){
//        return null;
//    }
}
