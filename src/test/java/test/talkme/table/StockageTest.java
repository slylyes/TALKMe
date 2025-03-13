package test.talkme.table;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import talkme.table.Column;
import talkme.table.Stockage;

class StockageTest {

    private Stockage stockage;
    private List<Column> columns;

    @BeforeEach
    void setup() {
        stockage = new Stockage();
        columns = List.of(
                new Column("col1", new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, "col1")),
                new Column("col2", new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, "col2"))
        );
        stockage.putCols(columns);
    }

    @Test
    void testPutCols() {
        assertNotNull(stockage.getSt(), "Storage map should be initialized");
        assertEquals(2, stockage.getSt().size(), "Storage should contain two columns");
        for (Column col : columns) {
            assertTrue(stockage.getSt().containsKey(col), "Storage should contain column: " + col.getName());
            assertTrue(stockage.getSt().get(col).isEmpty(), "Each column should have an empty list initially");
        }
    }

    @Test
    void testInsertValidData() {
        List<List<Object>> rows = List.of(
                List.of("A", "B", "C"), // Data for col1
                List.of(1, 2, 3) // Data for col2
        );
        stockage.insert(columns, rows);

        assertEquals(3, stockage.getSt().get(columns.get(0)).size(), "col1 should have 3 elements");
        assertEquals(3, stockage.getSt().get(columns.get(1)).size(), "col2 should have 3 elements");

        assertIterableEquals(List.of("A", "B", "C"), stockage.getSt().get(columns.get(0)), "Values in col1 should match");
        assertIterableEquals(List.of(1, 2, 3), stockage.getSt().get(columns.get(1)), "Values in col2 should match");
    }

    @Test
    void testInsertInvalidStructure() {
        List<Column> wrongColumns = List.of(new Column("col3", new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "col3")));
        List<List<Object>> rows = List.of(List.of(1.1, 2.2, 3.3));

        Exception exception = assertThrows(IllegalArgumentException.class, () -> stockage.insert(wrongColumns, rows));
        assertEquals("La structure des donnees fournis ne correspond pas a cette table", exception.getMessage(), "Should throw an exception for mismatched structure");
    }
}
