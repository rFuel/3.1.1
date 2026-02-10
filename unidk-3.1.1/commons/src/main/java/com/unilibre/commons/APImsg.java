package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.cipher.uCipher;

import java.util.ArrayList;
import java.util.Properties;

public class APImsg {

    private static ArrayList<String> APIkeys = new ArrayList<>();
    private static ArrayList<String> APIvalues = new ArrayList<>();
    private static ArrayList<String> useVlist = new ArrayList<>();
    private static ArrayList<String> useVdata = new ArrayList<>();
    private static ArrayList<String> u2pVlist = new ArrayList<>();
    private static ArrayList<String> u2pVdata = new ArrayList<>();

    public static Properties MessageAsProperties() {
        Properties props = new Properties();
        int lx = APIkeys.size();
        for (int i=0 ; i < lx ; i++) {
            props.setProperty(APIkeys.get(i), APIvalues.get(i));
        }
        lx = useVlist.size();
        for (int i=0 ; i < lx ; i++) {
            props.setProperty(useVlist.get(i), useVdata.get(i));
        }
        lx = u2pVlist.size();
        for (int i=0 ; i < lx ; i++) {
            props.setProperty(u2pVlist.get(i), u2pVdata.get(i));
        }
        return props;
    }

    public static int GetMsgSize() { return APIkeys.size(); }

    public static void instantiate() {
        APImsg();
    }

    public static void APImsg() {
        APIkeys.clear();
        APIvalues.clear();
        useVlist.clear();
        useVdata.clear();
        u2pVlist.clear();
        u2pVdata.clear();
    }

    public static void APIset(String key, String value) {
        if (key.startsWith("#")) return;
        if (key.toUpperCase().startsWith("MULTIPLEACTIVERESULTSETS")) {
            String junk="";
        }
        boolean done = false;
        value = value.trim();
        if (key.startsWith("U2P")) {
            String chk = key.substring(3,4);
            if ("._-".contains(chk)) {
                UpdateU2PList(key, value, chk);
                done = true;
            }
        }
        if (key.startsWith("USE")) {
            String chk = key.substring(3,4);
            if ("._-".contains(chk)) {
                UpdateUseList(key, value, chk);
                done = true;
            }
        }
        if (!done) {
            String uKey = key.toUpperCase();
            int fnd = APIkeys.indexOf(uKey);
            if (fnd < 0) {
                APIkeys.add(uKey);
                APIvalues.add("");
                fnd = APIkeys.indexOf(uKey);
            }
            try {
                APIvalues.set(fnd, value);
            } catch (IndexOutOfBoundsException e) {
                String wtf="";
            }
        }
    }

    public static void APIdel(String key) {
        if (key.equals("")) return;
        key = key.trim();
        String uKey = key.toUpperCase();
        int fnd = APIkeys.indexOf(uKey);
        if (fnd >= 0) {
            APIkeys.remove(fnd);
            APIvalues.remove(fnd);
        }
    }

    private static void UpdateU2PList(String key, String value, String chk) {
        if (key.length() > 3) {
            key = key.substring(key.indexOf(chk)+1, key.length());
            if (u2pVlist.indexOf(key) < 0) {
                u2pVlist.add(key);
                u2pVdata.add(value);
            }
        }
    }

    private static void UpdateUseList(String key, String value, String chk) {
        if (key.length() > 3) {
            key = key.substring(4, key.length());
            if (useVlist.indexOf(key) < 0) {
                useVlist.add(key);
                useVdata.add(value);
            }
        }
    }

    public static String APIget(String key) {
        String value = "";
        String uKey = key.toUpperCase();
        int fnd = APIkeys.indexOf(uKey);
        if (NamedCommon.trace) uCommons.uSendMessage("Fnd="+fnd);
        if (fnd > -1) value = APIvalues.get(fnd);
        if (value.startsWith("ENC(")) value = uCipher.Decrypt(value);
        return value;
    }

    public static String APIgetKey(int fnd) {
        String value = APIkeys.get(fnd);
        return value;
    }

    public static String APIgetVal(int fnd) {
        String value = APIvalues.get(fnd);
        return value;
    }

    public static int GetVLISTSize() { return useVlist.size(); }

    public static String APIgetVLISTKey(int fnd) {
        String value = useVlist.get(fnd);
        return value;
    }

    public static String APIgetVLISTVal(int fnd) {
        String value = useVdata.get(fnd);
        return value;
    }

    public static int GetU2PSize() { return u2pVlist.size(); }

    public static String APIgetU2PKey(int fnd) {
        String value = u2pVlist.get(fnd);
        return value;
    }

    public static String APIgetU2PVal(int fnd) {
        String value = u2pVdata.get(fnd);
        return value;
    }

    public static String BuildMessage() {
        String message="", is="<is>", tm="<tm>\n";
        int eoi = APIkeys.size();
        for (int i=0; i < eoi; i++) { if (!APIvalues.get(i).equals("") ) message += APIkeys.get(i) + is + APIvalues.get(i) + tm; }
        return message;
    }

}
