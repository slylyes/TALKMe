package talkme.parser;

// This class has to be instantiated with the wanted batch size (which is the amount of rows to be return each time
//getNextBatch is called), and the path to the parquet file to be read.
//
//getNextBatch(): return a new batch of rows as List<List<Object>>
//getColumnNames(): return the name of the columns as List<String>
//getColumnTypes(): return the types of columns as List<String>

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import talkme.table.Table;


public class ParquetParser {
    private final ParquetReader<Group> reader;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final MessageType schema;

    private final int limite;

    public ParquetParser(File parquetFile, int limite) throws IOException {
        this.limite = limite;

        Path filePath = new Path(parquetFile.toURI().toString());
        this.reader = ParquetReader.builder(new GroupReadSupport(), filePath).build();

        Configuration configuration = new Configuration();

        try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration))) {
            schema = fileReader.getFileMetaData().getSchema();
        }

        this.columnNames = extractColumnNames(schema);
        this.columnTypes = extractColumnTypes(schema);
    }

    public MessageType getSchema() {
        return schema;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    public List<List<Object>> getNextBatch() throws IOException {
        List<List<Object>> batch = new ArrayList<>();


        // Initialize column lists (one list per column)
        for (int i = 0; i < columnNames.size(); i++) {
            batch.add(new ArrayList<>());
        }

        // Read records and distribute values into column lists
        for (int i = 0; i < limite; i++) {
            Group record = reader.read();
            if (record == null) break; // No more data

            for (int colIndex = 0; colIndex < columnNames.size(); colIndex++) {
                try {
                    batch.get(colIndex).add(record.getValueToString(colIndex, 0)); // Add value to column list
                } catch (Exception e) {
                    batch.get(colIndex).add(null); // Handle missing values
                }
            }
        }
        return batch; // Now structured as [column1_values[], column2_values[], ...]
    }

    public List<List<Object>> getDirect(Table T) throws IOException {
        List<List<Object>> batch = new ArrayList<>();

        // Initialize column lists (one list per column)
        for (int i = 0; i < columnNames.size(); i++) {
            batch.add(new ArrayList<>());
        }

        // Read records and distribute values into column lists
        for (int i = 0; i < limite; i++) {
            Group record = reader.read();
            if (record == null) break; // No more data

            for (int colIndex = 0; colIndex < columnNames.size(); colIndex++) {
                try {
                    batch.get(colIndex).add(record.getValueToString(colIndex, 0)); // Add value to column list
                } catch (Exception e) {
                    batch.get(colIndex).add(null); // Handle missing values
                }
            }
        }
        return batch; // Now structured as [column1_values[], column2_values[], ...]

    }

    // Extract column names from schema
    private static List<String> extractColumnNames(MessageType schema) {
        List<String> names = new ArrayList<>();
        for (Type field : schema.getFields()) {
            names.add(field.getName());
        }
        return names;
    }

    // Extract column types from schema
    private static List<Type> extractColumnTypes(MessageType schema) {
        List<Type> types = new ArrayList<>();
        for (Type field : schema.getFields()) {
            types.add(field.asPrimitiveType());
        }
        return types;
    }

    public void close() throws IOException {
        reader.close();
    }
}

