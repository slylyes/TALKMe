package test.talkme.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

import talkme.table.Column;
import talkme.table.ColonnesException;
import talkme.query.MoteurStockage;
import talkme.table.Table;

class MoteurStockageTest {

    private MoteurStockage moteurStockage;
    private Table table;
    private List<String> columnNames;

    @BeforeEach
    void setup() {
        // Create columns with types and empty value lists
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("col1", "STRING"));
        columns.add(new Column("col2", "INTEGER"));
        
        // Create table with name and columns
        table = new Table("testTable", columns);
        
        // Create MoteurStockage with the table
        moteurStockage = new MoteurStockage(table);
        
        // List of column names for testing
        columnNames = Arrays.asList("col1", "col2");
    }

    @Test
    void testInsertValidData() throws ColonnesException {
        // Create data to insert
        List<List<Object>> data = Arrays.asList(
                Arrays.asList("A", "B", "C"), // Values for col1
                Arrays.asList(1, 2, 3)       // Values for col2
        );
        
        // Insert data
        moteurStockage.insert(columnNames, data);
        
        // Verify the data was inserted correctly
        assertEquals(3, table.getColumns().get("col1").getValues().size(), "col1 should have 3 elements");
        assertEquals(3, table.getColumns().get("col2").getValues().size(), "col2 should have 3 elements");
        
        // Check that the correct data was inserted
        assertEquals(Arrays.asList("A", "B", "C"), table.getColumns().get("col1").getValues(),
                "First row of col1 should contain [A, B, C]");
        assertEquals(Arrays.asList(1, 2, 3), table.getColumns().get("col2").getValues(),
                "First row of col2 should contain [1, 2, 3]");
    }

    @Test
    void testInsertInvalidColumn() {
        // Try to insert data using a non-existent column
        List<String> invalidColumns = Arrays.asList("col1", "col3");
        List<List<Object>> rows = Arrays.asList(
                Arrays.asList("A", "B", "C"),
                Arrays.asList(1.1, 2.2, 3.3)
        );
        
        Exception exception = assertThrows(ColonnesException.class, 
                () -> moteurStockage.insert(invalidColumns, rows));
        
        assertNotNull(exception.getMessage(), "Exception should have a message");
    }
}
