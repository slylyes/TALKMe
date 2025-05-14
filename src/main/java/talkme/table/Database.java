package talkme.table;

import talkme.query.MoteurStockage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Database {
    public static final Map<String, Table> tableMap = new HashMap<>();
//    private static final MoteurStockage moteur = new MoteurStockage();

    public static void add(Table t) throws SameNameException {
        if( tableMap.containsKey(t.getName())){
            throw new SameNameException();
        }
        tableMap.put(t.getName(), t);
    }

    /*
    public static void insertInTable(Table t, List<String> columnNames, List<List<Object>> data) throws ColonnesException{
        MoteurStockage.insert(t, columnNames, data);
    }
    */
}
