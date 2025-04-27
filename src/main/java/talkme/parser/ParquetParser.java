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
import talkme.table.Table;


public class ParquetParser {
    private  ParquetFileReader reader;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final MessageType schema;
    private final int limite;
    private  final Path pth;


    public ParquetParser(File parquetFile, int limite) throws IOException {
        this.limite = limite;

        Path filePath = new Path(parquetFile.toURI().toString());

        Configuration configuration = new Configuration();
        reader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration));
        schema = reader.getFooter().getFileMetaData().getSchema();

        this.pth=filePath;
        this.columnNames = extractColumnNames(schema);
        this.columnTypes = extractColumnTypes(schema);
    }


    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Type> getColumnTypes() {
        return columnTypes;
    }



//    public List<List<Object>> getNextBatch() throws IOException {
//        List<List<Object>> columns = new ArrayList<>();
//        for (int i = 0; i < columnNames.size(); i++) {
//            columns.add(new ArrayList<>());
//        }
//
//        int numColumns = columnNames.size();
//
//        // Iterate per column
//        for (int colIndex = 0; colIndex < numColumns; colIndex++) {
//            String colName = columnNames.get(colIndex);
//            Type colType = columnTypes.get(colIndex);
//            ColumnDescriptor colDescriptor = schema.getColumnDescription(new String[]{colName});
//            List<Object> columnData = columns.get(colIndex);
//
//            reader.setRequestedSchema(schema);  // fallback, in case you want full schema
//            reader.setRequestedSchema(MessageTypeParser.parseMessageType(schema.toString()));  // optional
//
//            // Reset to beginning for every column
//            reader = ParquetFileReader.open(HadoopInputFile.fromPath(pth, new Configuration()));
//
//            for (PageReadStore rowGroup; (rowGroup = reader.readNextRowGroup()) != null; ) {
//                if (columnData.size() >= limite) break;
//
//                ColumnReadStoreImpl columnReadStore = new ColumnReadStoreImpl(
//                        rowGroup,
//                        new DummyRecordConverter(schema).getRootConverter(),
//                        schema,
//                        null
//                );
//
//                ColumnReader columnReader = columnReadStore.getColumnReader(colDescriptor);
//                long rowsInGroup = rowGroup.getRowCount();
//
//                for (int i = 0; i < rowsInGroup && columnData.size() < limite; i++) {
//                    if (columnReader.getCurrentDefinitionLevel() == colDescriptor.getMaxDefinitionLevel()) {
//                        switch (colDescriptor.getType()) {
//                            case INT32 -> columnData.add(columnReader.getInteger());
//                            case INT64 -> columnData.add(columnReader.getLong());
//                            case DOUBLE -> columnData.add(columnReader.getDouble());
//                            case FLOAT -> columnData.add(columnReader.getFloat());
//                            case BOOLEAN -> columnData.add(columnReader.getBoolean());
//                            case BINARY -> columnData.add(columnReader.getBinary().toStringUsingUTF8());
//                            default -> columnData.add("UnsupportedType");
//                        }
//                    } else {
//                        columnData.add(null);
//                    }
//                    columnReader.consume();
//                }
//            }
//        }
//
//        return columns;
//    }


    public List<List<Object>> getNextBatch() throws IOException {
        List<List<Object>> batch = new ArrayList<>();

        for (int i = 0; i < columnNames.size(); i++) {
            batch.add(new ArrayList<>());
        }

        int rowsRead = 0;

        // Read the file once
        for (PageReadStore rowGroup; (rowGroup = reader.readNextRowGroup()) != null && rowsRead < limite; ) {
            ColumnReadStoreImpl columnReadStore = new ColumnReadStoreImpl(
                    rowGroup,
                    new DummyRecordConverter(schema).getRootConverter(),
                    schema,
                    null
            );

            List<ColumnReader> columnReaders = new ArrayList<>();
            for (String colName : columnNames) {
                ColumnDescriptor descriptor = schema.getColumnDescription(new String[]{colName});
                columnReaders.add(columnReadStore.getColumnReader(descriptor));
            }

            long rowsInGroup = rowGroup.getRowCount();

            for (int i = 0; i < rowsInGroup && rowsRead < limite; i++) {
                for (int colIndex = 0; colIndex < columnReaders.size(); colIndex++) {
                    ColumnReader reader = columnReaders.get(colIndex);
                    ColumnDescriptor descriptor = schema.getColumnDescription(new String[]{columnNames.get(colIndex)});

                    if (reader.getCurrentDefinitionLevel() == descriptor.getMaxDefinitionLevel()) {
                        switch (descriptor.getType()) {
                            case INT32 -> batch.get(colIndex).add(reader.getInteger());
                            case INT64 -> batch.get(colIndex).add(reader.getLong());
                            case FLOAT -> batch.get(colIndex).add(reader.getFloat());
                            case DOUBLE -> batch.get(colIndex).add(reader.getDouble());
                            case BOOLEAN -> batch.get(colIndex).add(reader.getBoolean());
                            case BINARY -> batch.get(colIndex).add(reader.getBinary().toStringUsingUTF8());
                            default -> batch.get(colIndex).add("UnsupportedType");
                        }
                    } else {
                        batch.get(colIndex).add(null);
                    }
                    reader.consume();
                }
                rowsRead++;
            }
        }

        return batch;
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

