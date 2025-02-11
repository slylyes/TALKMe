package TalkMe.parser;

// This class has to be instantiated with the wanted batch size (which is the amount of rows to be return each time
//getNextBatch is called), and the path to the parquet file to be read.
//
//getNextBatch(): return a new batch of rows as List<List<Object>>
//getColumnNames(): return the name of the columns as List<String>
//getColumnTypes(): return the types of columns as List<String>




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.*;

public class ParquetParser {
    private final ParquetReader<Group> reader;
    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final int batchSize;

    public ParquetParser(String parquetFile, int batchSize) throws IOException {
        Configuration configuration = new Configuration();
        this.batchSize = batchSize;

        // Initialize reader
        this.reader = ParquetReader.builder(new GroupReadSupport(), new Path(parquetFile)).build();

        // Extract schema
        MessageType schema = getSchema(parquetFile, configuration);
        this.columnNames = extractColumnNames(schema);
        this.columnTypes = extractColumnTypes(schema);
    }

    // Function to get column names
    public List<String> getColumnNames() {
        return columnNames;
    }

    // Function to get column types
    public List<String> getColumnTypes() {
        return columnTypes;
    }

    // Function to get the next batch of rows
    public List<List<Object>> getNextBatch() throws IOException {
        List<List<Object>> batch = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            Group record = reader.read();
            if (record == null) break; // No more data

            List<Object> row = new ArrayList<>();
            for (int colIndex = 0; colIndex < columnNames.size(); colIndex++) {
                try {
                    row.add(record.getValueToString(colIndex, 0)); // Read value as String
                } catch (Exception e) {
                    row.add(null); // Handle missing values
                }
            }
            batch.add(row);
        }
        return batch;
    }

    // Extract schema from the Parquet file
    private MessageType getSchema(String parquetFile, Configuration configuration) throws IOException {
        try (ParquetReader<Group> schemaReader = ParquetReader.builder(new GroupReadSupport(), new Path(parquetFile)).build()) {
            return (MessageType) schemaReader.read().getType();
        }
    }

    // Extract column names from schema
    private List<String> extractColumnNames(MessageType schema) {
        List<String> names = new ArrayList<>();
        for (Type field : schema.getFields()) {
            names.add(field.getName());
        }
        return names;
    }

    // Extract column types from schema
    private List<String> extractColumnTypes(MessageType schema) {
        List<String> types = new ArrayList<>();
        for (Type field : schema.getFields()) {
            types.add(field.asPrimitiveType().getPrimitiveTypeName().name());
        }
        return types;
    }

    // Close the reader
    public void close() throws IOException {
        reader.close();
    }

    // Example usage
//    public static void main(String[] args) throws IOException {
//        String parquetFile = "data/yellow_tripdata_2009-01.parquet";
//        int batchSize = 5; // Adjust as needed
//
//        ParquetParser parquetReader = new ParquetParser(parquetFile, batchSize);
//
//        // Fetch column names
//        System.out.println("Column Names: " + parquetReader.getColumnNames());
//
//        // Fetch column types
//        System.out.println("Column Types: " + parquetReader.getColumnTypes());
//
//        // Fetch and display first batch of data
//        List<List<Object>> batch = parquetReader.getNextBatch();
//        System.out.println("\nFirst Batch (" + batch.size() + " rows):");
//        for (List<Object> row : batch) {
//            System.out.println(row);
//        }
//
//        // Fetch and display second batch of data
//        List<List<Object>> nextBatch = parquetReader.getNextBatch();
//        System.out.println("\nSecond Batch (" + nextBatch.size() + " rows):");
//        for (List<Object> row : nextBatch) {
//            System.out.println(row);
//        }
//
//        parquetReader.close();
//    }
}





