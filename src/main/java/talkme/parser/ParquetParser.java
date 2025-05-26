package talkme.parser;

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
    private ParquetFileReader reader;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final MessageType schema;

    private final Integer limit;
    private final Path path;
    private final int batchSize;
    
    // Track the current position in the file
    private long currentRowCount = 0;
    private long totalRowCount = 0;
    private boolean hasMoreData = true;
    private PageReadStore currentRowGroup = null;
    private int currentRowInGroup = 0;

    // Default batch size of 1 million rows
    private static final int DEFAULT_BATCH_SIZE = 1_000_000;

    public ParquetParser(File parquetFile, Integer limit) throws IOException {
        this(parquetFile, limit, DEFAULT_BATCH_SIZE);
    }

    public ParquetParser(File parquetFile, Integer limit, int batchSize) throws IOException {
        this.limit = limit;
        this.batchSize = batchSize;

        Path filePath = new Path(parquetFile.toURI().toString());
        this.path = filePath;

        Configuration configuration = new Configuration();
        reader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration));
        schema = reader.getFooter().getFileMetaData().getSchema();

        this.columnNames = extractColumnNames(schema);
        this.columnTypes = extractColumnTypes(schema);
        
        // Get total row count for information purposes
        this.totalRowCount = reader.getRecordCount();
        
        // Initialize
        resetReader();
    }

    // Reset the reader to start from the beginning
    private void resetReader() throws IOException {
        if (reader != null) {
            reader.close();
        }
        reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()));
        reader.setRequestedSchema(schema);
        currentRowCount = 0;
        currentRowInGroup = 0;
        currentRowGroup = null;
        hasMoreData = true;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    public long getTotalRowCount() {
        return totalRowCount;
    }

    public boolean hasMoreData() {
        return hasMoreData;
    }

    public List<List<Object>> getNextBatch() throws IOException {
        // If we've hit the limit or have no more data, return empty list
        if ((limit != null && currentRowCount >= limit) || !hasMoreData) {
            hasMoreData = false;
            return new ArrayList<>();
        }

        List<List<Object>> columns = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columns.add(new ArrayList<>());
        }

        int rowsProcessed = 0;
        int effectiveBatchSize = (limit != null) ? 
            Math.min(batchSize, limit.intValue() - (int)currentRowCount) : 
            batchSize;

        try {
            while (rowsProcessed < effectiveBatchSize) {
                // If we need a new row group
                if (currentRowGroup == null || currentRowInGroup >= currentRowGroup.getRowCount()) {
                    currentRowGroup = reader.readNextRowGroup();
                    currentRowInGroup = 0;
                    
                    // If no more row groups, we're done
                    if (currentRowGroup == null) {
                        hasMoreData = false;
                        break;
                    }
                }

                // Calculate how many rows to process from this row group
                int rowsToProcess = Math.min(
                    effectiveBatchSize - rowsProcessed, 
                    (int)(currentRowGroup.getRowCount() - currentRowInGroup)
                );

                ColumnReadStoreImpl columnReadStore = new ColumnReadStoreImpl(
                    currentRowGroup,
                    new DummyRecordConverter(schema).getRootConverter(),
                    schema,
                    null
                );

                // Process each column
                for (int colIndex = 0; colIndex < columnNames.size(); colIndex++) {
                    String colName = columnNames.get(colIndex);
                    ColumnDescriptor colDescriptor = schema.getColumnDescription(new String[]{colName});
                    List<Object> columnData = columns.get(colIndex);
                    
                    ColumnReader columnReader = columnReadStore.getColumnReader(colDescriptor);
                    
                    // Skip rows we've already processed in this row group
                    for (int i = 0; i < currentRowInGroup; i++) {
                        columnReader.consume();
                    }
                    
                    // Read the rows for this batch
                    for (int i = 0; i < rowsToProcess; i++) {
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

                // Update tracking variables
                currentRowInGroup += rowsToProcess;
                rowsProcessed += rowsToProcess;
                currentRowCount += rowsToProcess;
                
                // Check if we've hit the limit
                if (limit != null && currentRowCount >= limit) {
                    hasMoreData = false;
                    break;
                }
            }
            
            return columns;
        } catch (Exception e) {
            hasMoreData = false;
            throw e;
        }
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
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }

    // Reset for reuse
    public void reset() throws IOException {
        resetReader();
    }
}
