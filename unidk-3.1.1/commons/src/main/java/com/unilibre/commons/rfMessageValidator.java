package com.unilibre.commons;

import java.util.ArrayList;

public class rfMessageValidator {

    public static ArrayList<String> Keys;
    public static ArrayList<String> Values;
    public static ArrayList<ArrayList<String>> inMessage;

    public static void Instantiate(String task) {
        Keys = new ArrayList<>();
        Values = new ArrayList<>();
        AddParameter("task", "");
        AddParameter("shost", "");
        AddParameter("map", "");
        AddParameter("format", "");
        AddParameter("mscat", "");
        AddParameter("task", "");
        AddParameter("payload", "");
    }

    private static void AddParameter(String param, String value) {
        String key = param.toUpperCase();
        int idx = Keys.indexOf(key);
        if (idx < 0) {
            Keys.add(key);
            Values.add(value);
        } else {
            Values.set(idx, value);
        }
    }

    public static String[] Validate(String kvSep, String paramSep, String message, String task) {
        Instantiate(task);
        ArrayList<String> temp = new ArrayList<String>();
        String tmp1 = message, tmp2 = "";
        String[] parts = new String[]{"200", "ok"};
        if (task.equals("910")) return parts;
        int idx;
        boolean done = false;
        while (!done) {
            idx = tmp1.indexOf(paramSep);
            if (idx >= 0) {
                tmp2 = tmp1.substring(0, idx);
                temp.add(tmp2);
                parts = tmp2.split(kvSep);
                if (Keys.indexOf(parts[0].toUpperCase()) >= 0) AddParameter(parts[0], parts[1]);
                tmp1 = tmp1.substring(tmp2.length() + paramSep.length(), tmp1.length());
            } else {
                uCommons.uSendMessage("Experienced and Error. "+tmp1+" has already been processed");
                System.exit(0);
            }
            if (tmp1.equals("")) done = true;
        }

        String errDesc = "", key = "", value = "";
        int nbrParams = Keys.size();

        for (int c = 0; c < nbrParams; c++) {
            key = Keys.get(c).toUpperCase();
            value = Values.get(c);
            if (value.equals("")) {
                switch (key) {
                    case "MSCAT":
                        if (task.equals("055")) errDesc += "No PAYLOAD value has been given. ";
                        break;

                    case "ITEM":
                        if (task.equals("055")) errDesc += "No ITEM value has been given. ";
                        break;

                    case "PAYLOAD":
                        if (task.equals("055")) errDesc += "No PAYLOAD value has been given. ";
                        break;

                    case "MAP":
                        if (task.equals("050")) errDesc += "No MAP value has been given. ";
                        break;

                    default:
                        errDesc += "No " + key + " value has been given. ";
                }
            }
        }
        String[] ans = new String[2];
        if (!errDesc.equals("")) {
            ans[0] = "500";
            ans[1] = errDesc;
        } else {
            ans[0] = "200";
            ans[1] = "ok";
        }
        return ans;
    }

}
