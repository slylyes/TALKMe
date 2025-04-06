package talkme.table;

import java.util.List;

public class MoteurStockage {
    public void insert(Table t, List<String> cols, List<List<Object>> data) throws ColonnesException{
        //Vérification de la structure donnée par l'utilisateur
        for (String col: cols){
            if (!t.getColumns().containsKey(col)){
                System.out.println(cols.toString());
                throw new ColonnesException(cols);
            }
        }
        //Ajout des données
        for (int i=0; i<cols.size(); i++){
            t.getColumns().get(cols.get(i)).getValues().addAll(data.get(i));
        }
    }

//    public Map<String, List<Object>> select(Table t, List<String> cols){
//
//        Map<String, List<Object>> selected= new HashMap<>();
//
//        for (String col: cols){
//            selected.put(t.getColumns().get(col));
//        }
//        return  selected;
//    }

//    public List<Integer> whereEquals(Column col,Object compared){
//        List<Integer> selectedIndex= new ArrayList<>();
//        List<Object> values= st.get(col);
//
//        for (int i=0; i<values.size(); i++){
//            if (compare(values.get(i), compared) == 0){
//                selectedIndex.add(i);
//            }
//        }
//
//        return selectedIndex;
//    }

//    public List<Integer> whereEquals(Column col,Object compared, List<Integer> prevSelected){
//        List<Integer> selectedIndex= new ArrayList<>();
//        List<Object> values= st.get(col);
//
//        for (int i: prevSelected){
//            if (compare(values.get(i), compared) == 0){
//                selectedIndex.add(i);
//            }
//        }
//
//        return selectedIndex;
//    }

//    public List<Integer> whereLessThan(Column col, Object compared) {
//        List<Integer> selectedIndex = new ArrayList<>();
//        List<Object> values = st.get(col);
//
//        for (int i = 0; i < values.size(); i++) {
//            if (compare(values.get(i), compared) < 0) {
//                selectedIndex.add(i);
//            }
//        }
//
//        return selectedIndex;
//    }

//    public List<Integer> whereLessThan(Column col, Object compared, List<Integer> prevSelected) {
//        List<Integer> selectedIndex = new ArrayList<>();
//        List<Object> values = st.get(col);
//
//        for (int i : prevSelected) {
//            if (compare(values.get(i), compared) < 0) {
//                selectedIndex.add(i);
//            }
//        }
//
//        return selectedIndex;
//    }

//    public List<Integer> whereGreaterThan(Column col, Object compared) {
//        List<Integer> selectedIndex = new ArrayList<>();
//        List<Object> values = st.get(col);
//
//        for (int i = 0; i < values.size(); i++) {
//            if (compare(values.get(i), compared) > 0) {
//                selectedIndex.add(i);
//            }
//        }
//
//        return selectedIndex;
//    }

//    public List<Integer> whereGreaterThan(Column col, Object compared, List<Integer> prevSelected) {
//        List<Integer> selectedIndex = new ArrayList<>();
//        List<Object> values = st.get(col);
//
//        for (int i : prevSelected) {
//            if (compare(values.get(i), compared) > 0) {
//                selectedIndex.add(i);
//            }
//        }
//
//        return selectedIndex;
//    }

    // Utility method for comparison
    @SuppressWarnings("unchecked")
    private int compare(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        throw new IllegalArgumentException("Objects are not comparable");
    }
}