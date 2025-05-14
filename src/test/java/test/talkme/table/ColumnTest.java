package test.talkme.table;

import org.junit.jupiter.api.Test;
import talkme.table.Column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ColumnTest {

    @Test
    void testColumnEquality() {
        String type1 = "BINARY";
        String type2 = "INT32";
        
        List<Object> values1 = new ArrayList<>();
        List<Object> values2 = Arrays.asList(1, 2, 3);

        String name1 = "";
        String name2 = "default";
        
        Column col1 = new Column(name1, type1, values1);
        Column col2 = new Column(name2, type1, values1);
        Column col3 = new Column(name1, type1, values2);
        Column col4 = new Column(name2, type2, values1);

        assertEquals(col1, col2, "Columns with same type and values should be equal");
        assertNotEquals(col1, col3, "Columns with different values should not be equal");
        assertNotEquals(col1, col4, "Columns with different types should not be equal");
    }

    @Test
    void testGetters() {
        String name = "testName";
        String type = "INT64";
        List<Object> values = Arrays.asList(1L, 2L, 3L);
        Column column = new Column(name, type, values);

        assertEquals(type, column.getType(), "getType() should return the correct type");
        assertEquals(values, column.getValues(), "getValues() should return the correct values");
    }

    @Test
    void testInequalityWithNull() {
        String name = "testName";
        String type = "BOOLEAN";
        List<Object> values = Arrays.asList(true, false, true);
        Column column = new Column(name, type, values);

        assertNotEquals(null, column, "A column should not be equal to null");
    }
}
