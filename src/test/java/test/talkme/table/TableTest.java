package test.talkme.table;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import talkme.table.Column;
import talkme.table.ColonnesException;
import talkme.query.MoteurStockage;
import talkme.table.Table;

import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    private Table table;
    private MoteurStockage moteur;

    @BeforeEach
    void setup() {
        // Create a map of columns for the table
        Map<String, Column> columns = new HashMap<>();
        columns.put("col1", new Column("col1", "STRING", new ArrayList<>()));
        columns.put("col2", new Column("col2", "INTEGER", new ArrayList<>()));
        
        // Create the table with name and columns
        table = new Table("TestTable", columns);
        moteur = new MoteurStockage();
    }

    @Test
    void testTableCreation() {
        assertEquals("TestTable", table.getName(), "Table name should be TestTable");
        assertEquals(2, table.getColumns().size(), "Table should contain two columns");
        assertTrue(table.getColumns().containsKey("col1"), "Table should contain col1");
        assertTrue(table.getColumns().containsKey("col2"), "Table should contain col2");
    }

    @Test
    void testGetColumnNames() {
        List<String> columnNames = table.getColumnNames();
        assertEquals(2, columnNames.size(), "Table should have two column names");
        assertTrue(columnNames.contains("col1"), "Column names should include col1");
        assertTrue(columnNames.contains("col2"), "Column names should include col2");
    }

    @Test
    void testInsertValidData() throws ColonnesException {
        // Define columns to insert into
        List<String> cols = Arrays.asList("col1", "col2");
        
        // Define data to insert
        List<Object> col1Data = Arrays.asList("A", "B", "C");
        List<Object> col2Data = Arrays.asList(1, 2, 3);
        List<List<Object>> data = Arrays.asList(col1Data, col2Data);
        
        // Insert the data
        moteur.insert(table, cols, data);
        
        // Verify the data was inserted correctly
        List<Object> col1Values = table.getColumns().get("col1").getValues();
        List<Object> col2Values = table.getColumns().get("col2").getValues();
        
        assertEquals(3, col1Values.size(), "Col1 should have 3 values");
        assertEquals(3, col2Values.size(), "Col2 should have 3 values");
        assertEquals("A", col1Values.get(0), "First value of col1 should be A");
        assertEquals(1, col2Values.get(0), "First value of col2 should be 1");
    }

    @Test
    void testInsertInvalidColumn() {
        // Define columns including an invalid one
        List<String> cols = Arrays.asList("col1", "col3");
        
        // Define data to insert
        List<Object> col1Data = Arrays.asList("A", "B", "C");
        List<Object> col3Data = Arrays.asList(4, 5, 6);
        List<List<Object>> data = Arrays.asList(col1Data, col3Data);
        
        // Attempt to insert with invalid column should throw exception
        assertThrows(ColonnesException.class, () -> moteur.insert(table, cols, data), 
                "Should throw exception when inserting into non-existent column");
    }

    @Test
    void testColumnEquality() {
        Column col1 = new Column("col1", "STRING", Arrays.asList("A", "B"));
        Column col2 = new Column("col2", "STRING", Arrays.asList("A", "B"));
        Column col3 = new Column("col3", "INTEGER", Arrays.asList("A", "B"));
        Column col4 = new Column("col4", "STRING", Arrays.asList(1, 2));
        
        assertTrue(col1.equals(col2), "Columns with same type and values should be equal");
        assertFalse(col1.equals(col3), "Columns with different types should not be equal");
        assertFalse(col1.equals(col4), "Columns with different value types should not be equal");
        assertTrue(col1.equals(col1), "Column should be equal to itself");
        assertFalse(col1.equals(null), "Column should not be equal to null");
    }
}
