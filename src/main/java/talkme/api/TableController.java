package talkme.api;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import talkme.parser.ParquetParser;
import talkme.table.ColonnesException;
import talkme.table.Database;
import talkme.table.SameNameException;
import talkme.table.Table;

import javax.ws.rs.Consumes;
import javax.ws.rs.QueryParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;



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
    public Response create(@RequestBody Table t){
        //On vérifie si une table de même nom existe déjà
        //Vérifier si name est null/vide
        if(t.getName() == null || t.getName().isEmpty()){
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Nom de table invalide")).build();
        }

        // Ajout de la table dans la Map contenant toutes les tables
        try {
            Database.add(t);
        }catch (SameNameException e){
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Table de même nom existe déjà")).build();
        }

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
    @POST
    @Path("/upload")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response uploadFile(
            @QueryParam("tableName") String tableName,
            @QueryParam("limite") int limite,
            File parquetFile) {

        // Validate table existence
        if (!tableMap.containsKey(tableName)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new StatusMessage("Table does not exist")).build();
        }

        Table table = tableMap.get(tableName);

        try {
            ParquetParser parser = new ParquetParser(parquetFile, limite);
            Database.insertInTable(table, parser.getColumnNames(), parser.getNextBatch());
            parser.close();
        } catch (IOException e) {
            return Response.status(Response.Status.BAD_REQUEST).
                    entity(new StatusMessage("Failed to process Parquet file")).build();
        }catch (ColonnesException e){
            return Response.status(Response.Status.BAD_REQUEST).
                    entity(new StatusMessage("Les colonnes des données ne correspondent pas avec celles de la table"+e.getMessage())).build();
        }

        return Response.status(Response.Status.OK).entity(new StatusMessage("File uploaded and processed successfully")).build();
    }

}
