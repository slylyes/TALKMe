package talkme.table;

import java.util.Collections;
import java.util.List;

public class ColonnesException extends RuntimeException {
    public ColonnesException(List<String> colonnes) {
        super("Les colonnes suivantes n'existent pas dans la table : " + colonnes.toString());
    }
}
