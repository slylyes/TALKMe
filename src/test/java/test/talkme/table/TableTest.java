package test.talkme.table;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import talkme.table.Column;
import talkme.table.Table;

import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    private Table table;

    @BeforeEach
    void setup() {
        table = new Table("TestTable");
    }

    @Test
    void testSetColumns() {
        List<String> names = List.of("col1", "col2");
        List<Type> types = List.of(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "col1"),
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "col2")
        );

        table.setColumns(names, types);

        List<Column> columns = table.getColumns();
        assertEquals(2, columns.size(), "Table should contain two columns");
        assertEquals("col1", columns.get(0).getName(), "First column name should be col1");
        assertEquals("col2", columns.get(1).getName(), "Second column name should be col2");
    }

    @Test
    void testSetColumnsMismatchedSizes() {
        List<String> names = List.of("col1");
        List<Type> types = List.of(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "col1"),
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "col2")
        );

        Exception exception = assertThrows(IllegalArgumentException.class, () -> table.setColumns(names, types));
        assertEquals("Les listes names et types doivent avoir la même taille !", exception.getMessage(), "Should throw exception for mismatched sizes");
    }

    @Test
    void testInsertValidData() {
        List<String> names = List.of("col1", "col2");
        List<Type> types = List.of(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "col1"),
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "col2")
        );
        table.setColumns(names, types);

        List<List<Object>> rows = List.of(
                List.of("A", "B", "C"), // Data for col1
                List.of(1, 2, 3) // Data for col2
        );

        assertDoesNotThrow(() -> table.insert(rows), "Valid data should be inserted without errors");
    }
}
