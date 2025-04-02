
package talkme.parser;

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
import talkme.table.Column;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class ParquetParser {
    private final ParquetReader<Group> reader;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final int batchSize;
    private int nbRead=0;

    public ParquetParser(File parquetStream, int batchSize) throws IOException {
        Configuration configuration = new Configuration();
        this.batchSize = batchSize;

        //Initialize InputStream as a Hadoop FSDataInputStream
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream fsDataInputStream = new FSDataInputStream(new FileInputStream(parquetStream));

        // Initialize reader using InputStream
        this.reader = ParquetReader.builder(new GroupReadSupport(), new Path(parquetStream.toURI())).build();

        // Extract schema
        MessageType schema = getSchema();
        this.columnNames = extractColumnNames(schema);
        this.columnTypes = extractColumnTypes(schema);
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
        for (int i = 0; i < batchSize; i++) {
            if (nbRead >= batchSize) break;

            Group record = reader.read();
            if (record == null) break; // No more data

            for (int colIndex = 0; colIndex < columnNames.size(); colIndex++) {
                try {
                    batch.get(colIndex).add(record.getValueToString(colIndex, 0)); // Add value to column list
                } catch (Exception e) {
                    batch.get(colIndex).add(null); // Handle missing values
                }
            }
            nbRead++;
        }
        return batch; // Now structured as [column1_values[], column2_values[], ...]
    }


    private MessageType getSchema() throws IOException {
        return (MessageType) reader.read().getType();
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

    public List<Column> getStuctures(){
        List<Column> columns= new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.add(new Column(columnNames.get(i),  columnTypes.get(i)));
        }
        return  columns;
    }

    public void close() throws IOException {
        reader.close();
    }
}
