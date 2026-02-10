package com.unilibre.commons;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Iterator;

public class msgCommons {


    public static String CleanMessage(String instr) {
        // ----------- clean up the instr message ----------------------
        instr = instr.replaceAll("\\r?\\n", "");
        instr = instr.replace("<IS>", "<is>");
        instr = instr.replace("<Is>", "<is>");
        instr = instr.replace("<iS>", "<is>");
        instr = instr.replace("<TM>", "<tm>");
        instr = instr.replace("<Tm>", "<tm>");
        instr = instr.replace("<tM>", "<tm>");
        // -------------------------------------------------------------
        return  instr;
    }

    public static String jsonifyMessage(String instr) {

        instr = CleanMessage(instr);

        String answer="";
        JSONObject obj = new JSONObject();

        String[] mParts = instr.split("<tm>");
        int eop = mParts.length, eol=0;
        String line, mKey, mVal;
        for (int p=0; p < eop ; p++) {
            line = mParts[p];
            if (line.replace(" ", "").equals("")) continue;;
            String[] lParts = line.split("<is>");
            mKey = lParts[0];
            mVal = "";
            if (lParts.length > 0 && !line.endsWith("<is>")) mVal = lParts[1];
            obj.put(mKey.toUpperCase(), mVal);
        }

        JSONObject reqObj = new JSONObject();
        reqObj.put("request", obj);

        answer = reqObj.toString();
        obj = null;
        reqObj = null;
        return answer;
    }

    public static String stringifyMessage(String theMessage) {
        StringBuilder answer = new StringBuilder();
        JSONObject obj = new JSONObject(theMessage);
        if (obj == null) return "";
        if (obj.length() < 1) return "";
        String jHeader = "request";
        Iterator<String> jKeys = null;
        jKeys = obj.getJSONObject(jHeader).keys();
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = obj.getJSONObject(jHeader).get(zkey).toString();
            answer.append(zkey.toUpperCase() + "<is>" + zval + "<tm>");
        }
        return answer.toString();
    }

    private static String GetJobject(JSONObject obj, String key) {
        if (key.equals("")) return "";
        String answer = "";
        try {
            answer = obj.getString(key);
        } catch (JSONException je) {
            if (answer.equals("") && obj.toString().contains(key)) {
                try {
                    JSONObject tmpObj = obj.optJSONObject(key);
                    answer = tmpObj.toString();
                } catch (NullPointerException npe) {
                    uCommons.uSendMessage("Null pointer exception looking for \"" + key + "\"");
                }
            }
        }
        return answer;
    }

}
