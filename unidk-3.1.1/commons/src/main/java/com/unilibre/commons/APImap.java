package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import java.util.ArrayList;

public class APImap {

    private static ArrayList<String> APImapkeys = new ArrayList<>();
    private static ArrayList<String> APImapvals = new ArrayList<>();

    public static void instantiate() {
        APImap();
    }

    public static void APImap() {

        APImapkeys.clear(); // = new ArrayList<>();
        APImapvals.clear(); // = new ArrayList<>();
        int i = -1;

        i++;
        APImapkeys.add(i, "U2FILE");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "SQLTABLE");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "LIST");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "COLPFX");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "TEMPLATE");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "SELECT");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "NSELECT");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "PREUNI");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "PRESQL");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "POSTUNI");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "POSTSQL");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "RAWSEL");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "TRIGGER");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "TRIGQUE");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "PROCEED");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "FLOCN");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "LASTPART");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "FIRSTPART");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "");
        APImapvals.add(i, "");
        i++;
        APImapkeys.add(i, "");
        APImapvals.add(i, "");
    }

    public static void APIset(String key, String value) {
        String uKey = key.toUpperCase();
        int fnd = APImapkeys.indexOf(uKey);
        if (fnd < 0) {
            APImapkeys.add(uKey);
            APImapvals.add("");
            fnd = APImapkeys.indexOf(uKey);
        }
        APImapvals.set(fnd, value);
    }

    public static String APIget(String key) {
        String value = "";
        String uKey = key.toUpperCase();
        int fnd = APImapkeys.indexOf(uKey);
        if (fnd > -1) value = APImapvals.get(fnd);
        return value;
    }

}
