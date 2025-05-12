package talkme.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import talkme.table.Table;
import java.util.List;

import static talkme.table.Database.tableMap;

public class Query {

    private final Table t;
    private final List<String> columns;
    private final List<List<String>> filters;
    private final List<String>  groupBy;

    @JsonCreator
    public Query(@JsonProperty("name") String name,@JsonProperty("columns")  List<String> columns,
                 @JsonProperty("filters")  List<List<String>> filters, @JsonProperty("groupBy")  List<String>  groupBy ) {

        if (!tableMap.containsKey(name)) {
            throw new IllegalArgumentException() ;
        }
        this.t = tableMap.get(name);

        if (columns.isEmpty()){
            columns=tableMap.get(name).getColumnNames();
        }
        this.columns=columns;
        this.filters = filters;
        this.groupBy = groupBy;

    }


    // Getters, if needed
    public Table getTable() {
        return t;
    }

    public List<String> getColumns(){
        return columns;
    }
    public List<List<String>> getFilters() {
        return filters;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public Boolean groupByActivated(){
        return !groupBy.isEmpty();
    }
}
