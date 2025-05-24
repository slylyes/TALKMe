package talkme.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import talkme.table.Table;
import java.util.List;
import java.util.Map;

import static talkme.table.Database.tableMap;

public class Query {

    private final Table t;
    private final List<String> columns;
    private final List<List<String>> filters;
    private final List<String>  groupBy;
    private final List<String>  orderBy;
    private final List<Map<String, String>> aggregates;

    @JsonCreator
    public Query(@JsonProperty("name") String name,@JsonProperty("columns")  List<String> columns,
                 @JsonProperty("filters")  List<List<String>> filters,
                 @JsonProperty("groupBy")  List<String>  groupBy,
                 @JsonProperty("aggregates")  List<Map<String, String>> aggregates,
                 @JsonProperty("orderBY")  List<String>  orderBy
                 ) {

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
        this.orderBy = orderBy;
        this.aggregates = aggregates;
    }


    // Getters, if needed
    @JsonIgnore
    public Table getTable() {
        return t;
    }

    @JsonProperty("name")
    public String getName() {
        return t.getName();
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
    public List<String> getOrderBy() {
        return orderBy;
    }

    public List<Map<String, String>> getAggregates() {
        return aggregates;
    }
}
