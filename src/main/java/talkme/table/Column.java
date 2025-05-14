package talkme.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Column {
    private String name;
    private String type;
    private List<Object> values;

    // Getters and setters
    @JsonCreator
    public Column(@JsonProperty("name") String name,
                  @JsonProperty("type") String type,
                  @JsonProperty("values") List<Object> values) {
        this.name = name;
        this.type = type;
        this.values = values;
    }

    public boolean equals(Object o){
        if (this==o) return true;
        if (!(o instanceof Column ot)) return false;

        return ((type.equals(ot.getType())) && (values.equals(ot.getValues())));
    }

    @Override
    public String toString() {
        return "Column{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", values=" + values +
                '}';
    }

    public String getName(){ return name;}
    public String getType(){ return type; }
    public List<Object> getValues() {
        return values;
    }
}
