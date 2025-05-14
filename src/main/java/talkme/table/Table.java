package talkme.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import talkme.query.MoteurStockage;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Table {
    private final String name;
    private final Map<String, Column> columns;
    @JsonIgnore
    private final MoteurStockage moteurStockage;

    @JsonCreator
    public Table(@JsonProperty("name") String name, @JsonProperty("columns") List<Column> columns) {
        this.name = name;
        this.columns = columns.stream()
                .collect(Collectors.toMap(Column::getName, column -> column));

    }

    public String getName() {
        return name;
    }

    @JsonIgnore
    public List<String> getColumnNames(){
        return List.copyOf(columns.keySet());
    }
    @JsonIgnore
    public Map<String, Column> getColumns() {
        return columns;
    }


    @JsonProperty("columns")
    public List<Column> getColumnsforSerialization() {
        return List.copyOf(columns.values());
    }

    @Override
    public String toString() {
        return "Table{" +
                "columns=" + columns +
                ", name='" + name + '\'' +
                '}';
    }
}

    public MoteurStockage getMoteurStockage() {return moteurStockage;}

}

