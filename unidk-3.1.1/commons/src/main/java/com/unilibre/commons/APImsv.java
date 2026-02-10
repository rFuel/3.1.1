package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import java.util.ArrayList;

public class APImsv {

    public static ArrayList<String> APIkeys;
    public static ArrayList<String> APIvalues;

    public static void instantiate() {
        new APImsv();
    }

    public APImsv() {

        APIkeys = new ArrayList<>();
        APIvalues = new ArrayList<>();
        int i = -1;

        i++;
        APIkeys.add(i, "TASK");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "BROKER");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "QNUMBER");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "INQUE");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "OUTQUE");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "MSCAT");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "PAYLOAD");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "TNAME");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "ESBFMT");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "MSCDIR");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "");
        APIvalues.add(i, "");
        i++;
        APIkeys.add(i, "");
        APIvalues.add(i, "");
    }

    public static void APIset(String key, String value) {
        String uKey = key.toUpperCase();
        int fnd = APIkeys.indexOf(uKey);
        if (fnd < 0) return;
        APIvalues.set(fnd, value);
    }

    public static String APIget(String key) {
        String value = "";
        String uKey = key.toUpperCase();
        int fnd = APIkeys.indexOf(uKey);
        if (fnd < 0) {
            uCommons.uSendMessage("ERROR: Cannot find " + key + " in the micro-service API");
            NamedCommon.ZERROR = true;
        } else {
            value = APIvalues.get(fnd);
        }
        return value;
    }

}
