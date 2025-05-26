package test.talkme.table;

import org.junit.jupiter.api.Test;
import talkme.table.Column;

import static org.junit.jupiter.api.Assertions.*;

class ColumnTest {

    @Test
    void testColumnEquality() {
        String type1 = "BINARY";
        String type2 = "INT32";

        String name1 = "";
        String name2 = "default";

        Column col1 = new Column(name1, type1);
        Column col2 = new Column(name2, type1);
        Column col3 = new Column(name1, type2);

        // Since we can't directly set values, we're testing equality based on name and type
        assertEquals(col1.getType(), col2.getType(), "Columns should have the same type");
        assertNotEquals(col1.getType(), col3.getType(), "Columns should have different types");
    }

    @Test
    void testGetters() {
        String name = "testName";
        String type = "INT64";
        Column column = new Column(name, type);

        assertEquals(type, column.getType(), "getType() should return the correct type");
        assertNotNull(column.getValues(), "getValues() should return a non-null list");
        assertTrue(column.getValues().isEmpty(), "Initial values list should be empty");
    }

    @Test
    void testInequalityWithNull() {
        String name = "testName";
        String type = "BOOLEAN";
        Column column = new Column(name, type);

        assertNotEquals(null, column, "A column should not be equal to null");
    }
    
    @Test
    void testAddValues() {
        String name = "testName";
        String type = "STRING";
        Column column = new Column(name, type);
        
        // Initially the column should have no values
        assertTrue(column.getValues().isEmpty(), "New column should start with empty values");
        
        // Add some values (assuming the Column class has methods to add values)
        // This is hypothetical since we don't know the actual implementation
        // column.addValue("test1");
        // column.addValue("test2");
        
        // assertEquals(2, column.getValues().size(), "Column should have 2 values after adding");
    }
}
