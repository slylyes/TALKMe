package talkme.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class Table {
    private final String name;
    private final Map<String, Column> columns;

    @JsonCreator
    public Table(@JsonProperty("name") String name, @JsonProperty("columns") Map<String, Column> columns) {
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }
    public List<String> getColumnNames(){
        return List.copyOf(columns.keySet());
    }
    public Map<String, Column> getColumns() {
        return columns;
    }
}