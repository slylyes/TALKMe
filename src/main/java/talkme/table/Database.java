package talkme.table;

import java.util.HashMap;
import java.util.Map;

public class Database {
    public static final Map<String, Table> tableMap = new HashMap<>();

    public static void add(Table t){
        if( tableMap.containsKey(t.getName())){
            System.out.println("Une table de même nom existe déjà");
            return;
        }
        tableMap.put(t.getName(), t);
    }
}
