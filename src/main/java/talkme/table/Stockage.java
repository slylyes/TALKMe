package talkme.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Stockage {

    private Map<Column, List<Object>> st;

    public void putCols(List<Column> cols){
        st = new HashMap<>();
        for (Column col: cols){
            st.put(col, new ArrayList<>());
        }
    }


    public void insert(List<Column> cols,List<List<Object>> rows){
            if (!testRowStructure(cols)) throw new IllegalArgumentException("La structure des donnees fournis ne correspond pas a cette table");

            for (int i=0;i<cols.size();i++){
                Column col= cols.get(i);
                st.computeIfAbsent(col, k -> new ArrayList<>()).addAll(rows.get(i));
            }
    }

    public List<List<Object>> select(List<Column> cols){

        List<List<Object>> selected= new ArrayList<>();

        for (Column col: cols){
            selected.add(new ArrayList<>(st.get(col)));
        }
        return  selected;
    }

    public List<Integer> andEquals(Column col,Object compared){
        List<Integer> selectedIndex= new ArrayList<>();
        List<Object> values= st.get(col);

        for (int i=0; i<values.size(); i++){
            if (compare(values.get(i), compared) == 0){
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }

    public List<Integer> andEquals(Column col,Object compared, List<Integer> prevSelected){
        List<Integer> selectedIndex= new ArrayList<>();
        List<Object> values= st.get(col);

        for (int i: prevSelected){
            if (compare(values.get(i), compared) == 0){
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }

    public List<Integer> andLessThan(Column col, Object compared) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = st.get(col);

        for (int i = 0; i < values.size(); i++) {
            if (compare(values.get(i), compared) < 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }

    public List<Integer> andLessThan(Column col, Object compared, List<Integer> prevSelected) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = st.get(col);

        for (int i : prevSelected) {
            if (compare(values.get(i), compared) < 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }

    public List<Integer> andGreaterThan(Column col, Object compared) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = st.get(col);

        for (int i = 0; i < values.size(); i++) {
            if (compare(values.get(i), compared) > 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }

    public List<Integer> andGreaterThan(Column col, Object compared, List<Integer> prevSelected) {
        List<Integer> selectedIndex = new ArrayList<>();
        List<Object> values = st.get(col);

        for (int i : prevSelected) {
            if (compare(values.get(i), compared) > 0) {
                selectedIndex.add(i);
            }
        }

        return selectedIndex;
    }

    // Utility method for comparison
    @SuppressWarnings("unchecked")
    private int compare(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        throw new IllegalArgumentException("Objects are not comparable");
    }

    public boolean testRowStructure(List<Column> struct){
        for (Column col: st.keySet()){
            if (!struct.contains(col)) return false;
        }
        return true;
    }
    public Map<Column, List<Object>> getSt() {
        return st;
    }


}
