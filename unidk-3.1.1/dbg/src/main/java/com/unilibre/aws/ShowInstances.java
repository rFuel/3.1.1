package com.unilibre.aws;

import com.unilibre.commons.uCommons;
import org.json.JSONArray;
import org.json.JSONObject;

public class ShowInstances {

    public static void main(String[] args) {

        JSONArray instArr;
        JSONObject instObj, stateObj, tagObj, tmpObj;
        String instID="", instName="", instState="", instIP="", line="";

        String cmd = "aws ec2 describe-instances";
        System.out.println(cmd);
        String answer = uCommons.nixExecCmd(cmd, 999999);

        JSONObject obj = new JSONObject(answer);                    // Reservations object
        JSONArray resArr = obj.getJSONArray("Reservations");
        JSONObject resObj;

        int eoo = resArr.length();

        for (int o=0; o < eoo; o++) {
            resObj = resArr.getJSONObject(o);
            int xx = resObj.keySet().size();
            instObj = resObj.getJSONArray("Instances").getJSONObject(0);

            instID = instObj.getString("InstanceId");
            stateObj = instObj.getJSONObject("State");
            instState = stateObj.getString("Name");
            if (instState.equals("running")) {
                instIP = instObj.getString("PublicIpAddress");
            } else {
                instIP = "---.---.---.---";
            }

            instArr = instObj.getJSONArray("Tags");
            instName = instArr.getJSONObject(0).getString("Value");
            // show the results --------------------------------------
            line = uCommons.RightHash(o + " >> ", 10) + " " +
                    uCommons.LeftHash(instName, 30) + " " +
                    uCommons.LeftHash(instID, 20) + " " +
                    uCommons.LeftHash(instState, 20) + " " +
                    uCommons.LeftHash(instIP, 12);
            System.out.println(line);
        }

    }
}
