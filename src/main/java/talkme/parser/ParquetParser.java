package talkme.parser;

// This class has to be instantiated with the wanted batch size (which is the amount of rows to be return each time
//getNextBatch is called), and the path to the parquet file to be read.
//
//getNextBatch(): return a new batch of rows as List<List<Object>>
//getColumnNames(): return the name of the columns as List<String>
//getColumnTypes(): return the types of columns as List<String>

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;


public class ParquetParser {
    private  ParquetFileReader reader;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final MessageType schema;
    private final int limit;
    private  final Path path;


    public ParquetParser(File parquetFile, int limit) throws IOException {
        this.limit = limit;

        Path filePath = new Path(parquetFile.toURI().toString());

        Configuration configuration = new Configuration();
        reader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration));
        schema = reader.getFooter().getFileMetaData().getSchema();

        this.path =filePath;
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
        List<List<Object>> columns = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.add(new ArrayList<>());
        }

        int numColumns = columnNames.size();

        // Iterate per column
        for (int colIndex = 0; colIndex < numColumns; colIndex++) {
            String colName = columnNames.get(colIndex);
            Type colType = columnTypes.get(colIndex);
            ColumnDescriptor colDescriptor = schema.getColumnDescription(new String[]{colName});
            List<Object> columnData = columns.get(colIndex);

            reader.setRequestedSchema(schema);  // fallback, in case you want full schema
            reader.setRequestedSchema(MessageTypeParser.parseMessageType(schema.toString()));  // optional

            // Reset to beginning for every column
            reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()));

            for (PageReadStore rowGroup; (rowGroup = reader.readNextRowGroup()) != null; ) {
                if (columnData.size() >= limit) break;

                ColumnReadStoreImpl columnReadStore = new ColumnReadStoreImpl(
                        rowGroup,
                        new DummyRecordConverter(schema).getRootConverter(),
                        schema,
                        null
                );

                ColumnReader columnReader = columnReadStore.getColumnReader(colDescriptor);
                long rowsInGroup = rowGroup.getRowCount();

                for (int i = 0; i < rowsInGroup && columnData.size() < limit; i++) {
                    if (columnReader.getCurrentDefinitionLevel() == colDescriptor.getMaxDefinitionLevel()) {
                        switch (colDescriptor.getType()) {
                            case INT32 -> columnData.add(columnReader.getInteger());
                            case INT64 -> columnData.add(columnReader.getLong());
                            case DOUBLE -> columnData.add(columnReader.getDouble());
                            case FLOAT -> columnData.add(columnReader.getFloat());
                            case BOOLEAN -> columnData.add(columnReader.getBoolean());
                            case BINARY -> columnData.add(columnReader.getBinary().toStringUsingUTF8());
                            default -> columnData.add("UnsupportedType");
                        }
                    } else {
                        columnData.add(null);
                    }
                    columnReader.consume();
                }
            }
        }

        return columns;
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

