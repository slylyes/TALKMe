package talkme.table;
import org.apache.parquet.schema.Type;

import java.util.Objects;

public class Column {

    private final String name;
    private final Type type;

    public Column(String name, Type type){
        this.type = type;
        this.name = name;
    }

    public boolean equals(Object o){
        if (this==o) return true;
        if (!(o instanceof Column ot)) return false;

        return (Objects.equals(this.name, ot.getName())) && (this.type == ot.getType());
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }
}
