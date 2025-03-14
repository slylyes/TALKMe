package talkme.api;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import talkme.parser.ParquetParser;
import talkme.table.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import org.apache.parquet.schema.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;

@Path("/api")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableController {
    private static Map<String, Table> tableMap = new HashMap<>();

    //Conversion des types données en String en List de Types
    private List<Type> readTypes(String types) {
        List<Type> res = new ArrayList<>();
        for (String type : types.split(",")) {
            switch (type.trim()) { // trim to avoid whitespace issues
                case "INT64":
                    res.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED).named("INT64"));
                    break;
                case "BINARY":
                    res.add(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("BIN"));
                    break;
                case "DOUBLE":
                    res.add(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED).named("DBL"));
                    break;
                default:
                    System.out.println("Unknown type: " + type);
            }
        }
        return res;
    }

    /*
    * Création d'une table vide
    * Paramètres:
    *   name: nom de la table
    *   columns: colonnes de la table séparées par des virgules
    * Préconditions:
    *   Une table de même nom ne doit pas exister
    * Retourne un Response indiquant si la table a été créée ou si elle existe déjà
     */
    @POST
    @Path("/table")

    public Response create(Table t,
            @QueryParam("name") String name,
            @QueryParam("columns") String columns,
            @QueryParam("columnTypes") String columnTypes
    ){
        //On vérifie si une table de même nom existe déjà
        if(tableMap.containsKey(name)){
            return Response.status(Response.Status.PRECONDITION_FAILED)
                    .entity("Une table de même nom existe déjà!").build();
        }

        Table t = new Table(name);
        // Vérifier si arguments valides ou pas
        System.out.println(List.of(columns.split(",")).toString()+readTypes(columnTypes).toString());
        t.setColumns(List.of(columns.split(",")), readTypes(columnTypes));

        tableMap.put(name, t); // Ajout de la table dans la Map contenant toutes les tables
        return Response.status(Response.Status.CREATED)
                .entity("Nouvelle table créée: "+t.toString()).build();
    }

    /*
    * Remplissage d'une table
    * Arguments:
    *   table: nom de la table à remplir
    *   fileName: nom du fichier à partir duquel récupérer les données à mettre dans la table
    *   offset: offset de lecture du fichier
    * Préconditions:
    *   La table doit déjà exister
    *   Le fichier doit exister
    * Retourne un Response indiquant si la table a pu être remplie ou pas
     */
    @PUT
    @Path("/upload")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response upload(
            @QueryParam("table") String table,
            @QueryParam("filename") String fileName,
            @QueryParam("offset") int offset
    ){

        //On vérifie si la table existe
        if(!tableMap.containsKey(table)){
            return Response.status(Response.Status.PRECONDITION_FAILED)
                    .entity("La table "+table+" n'existe pas.").build();
        }

        Table target = tableMap.get(table);

        try {
            ParquetParser parquetReader = new ParquetParser(fileName, 5, 0, 5);
            List<List<Object>> donnees = parquetReader.getNextBatch();
            target.insert(donnees);
            return Response.status(Response.Status.OK)
                    .entity("La table "+table+" a été remplie avec les données du fichier "+fileName+"\n"+target).build();
        } catch (IOException e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity("Erreur d'ouverture du fichier " + fileName).build();
        }
    }
}
