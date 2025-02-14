package talkme.table;
import org.apache.parquet.schema.Type;

public class Column {

    private final String name;
    private final Type type;

    public Column(String name, Type type){
        this.type = type;
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }
}
