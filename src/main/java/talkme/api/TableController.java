package talkme.api;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import talkme.parser.ParquetParser;
import talkme.table.Database;
import talkme.table.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import org.apache.parquet.schema.Type;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;

import static talkme.table.Database.tableMap;

@Path("/api")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableController {
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
    public Response create(Table t){
        //On vérifie si une table de même nom existe déjà
        //Vérifier si name est null/vide
        if(t.getName() == null || t.getName().isEmpty()){
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorMessage("Nom de table invalide")).build();
        }

        Database.add(t); // Ajout de la table dans la Map contenant toutes les tables
        return Response.status(Response.Status.CREATED)
                .entity(t).build();
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
    public Response upload(
            @QueryParam("tableName") String table,
            @QueryParam("limite") int limite,
            InputStream parquetFile
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
