package talkme.table;

import TalkMe.parser.ParquetParser;
import org.apache.parquet.schema.Type;
import org.checkerframework.checker.units.qual.C;

import java.io.IOException;
import java.util.*;

public class Table {
    private String name;
    private final List<Column> columns = new ArrayList<Column>();
    private long raw;

    public Table(String name){
        this.name = name;
    }

    public long getRaw() {
        return raw;
    }

    public List<Column> getColumns() {
        return columns;
    }
}
