package talkme.table;

import org.apache.parquet.schema.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.*;

public class Table {
    private final String name;
    private final List<Column> columns = new ArrayList<Column>();
    private final Stockage st= new Stockage();

    public Table(String name){
        this.name = name;
    }



    @JsonProperty("columns")
    public void setColumns(List<Column> columns) {
        this.columns.addAll(columns);
        st.putCols(columns);
    }

    public void insert(List<List<Object>> rows) {
        st.insert(columns,rows);
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString(){
        return name+"\nColumns: "+columns+"\nData: "+st.getSt().toString();
    }
}