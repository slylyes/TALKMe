package talkme.table;

import org.apache.parquet.schema.Type;
import java.util.*;

public class Table {
    private final String name;
    private final List<Column> columns = new ArrayList<Column>();
    private final Stockage st= new Stockage();

    public Table(String name){
        this.name = name;
    }



    public void setColumns(List<String> names, List<Type> types) {
        if (names.size() != types.size()) {
            throw new IllegalArgumentException("Les listes names et types doivent avoir la même taille !");
        }

        for (int i = 0; i < names.size(); i++) {
            columns.add(new Column(names.get(i),  types.get(i)));
        }
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
}
