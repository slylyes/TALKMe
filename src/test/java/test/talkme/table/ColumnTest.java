package test.talkme.table;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import talkme.table.Column;

import static org.junit.jupiter.api.Assertions.*;

class ColumnTest {

    @Test
    void testColumnEquality() {
        Type type1 = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "column1");
        Type type2 = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "column1");

        Column col1 = new Column("col1", type1);
        Column col2 = new Column("col1", type1);
        Column col3 = new Column("col2", type1);
        Column col4 = new Column("col1", type2);

        assertEquals(col1, col2, "Columns with same name and type should be equal");
        assertNotEquals(col1, col3, "Columns with different names should not be equal");
        assertNotEquals(col1, col4, "Columns with different types should not be equal");
    }

    @Test
    void testGetters() {
        Type type = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "column2");
        Column column = new Column("column2", type);

        assertEquals("column2", column.getName(), "getName() should return the correct name");
        assertEquals(type, column.getType(), "getType() should return the correct type");
    }

    @Test
    void testInequalityWithNull() {
        Type type = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "column3");
        Column column = new Column("column3", type);

        assertNotEquals(null, column, "A column should not be equal to null");
    }
}