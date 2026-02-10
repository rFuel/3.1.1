package com.unilibre.dataserver;

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.core.DataConverter;
import com.unilibre.core.MessageProtocol;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class muleserver {

    private static String BaseCamp="", dbfHost="rfuel22", is="<is>", tm="<tm>", getQ = "059_CDR_Responses";
    private static Properties runProps = null;
    private static int msgCounter = 0;
    private static ArrayList<String> reqKeys;
    private static ArrayList<String> reqVals;
    private static final String jHeader = "request";
    private static boolean firstTime = true;

    public static String entry(String request) {
        NamedCommon.Zmessage = "";
        NamedCommon.ZERROR = false;
        if (!isLicenced()) { System.out.println("licence failure"); System.exit(1); }
        if (firstTime) Setup();

        String answer="";
        APImsg.instantiate();

        if (!DecodeRequest(request)) {
            answer = "Message Decoder Failed";
        } else {
            request = StringifyRequest();
            uCommons.Message2Properties(request);
            NamedCommon.MessageID = NamedCommon.pid + "_" + msgCounter;
            msgCounter++;
            answer = MessageProtocol.handleProtocolMessage(GetDetail("task"), "1", request);
        }
        if (GetDetail("task").equals("055")) {
            uCommons.Sleep(0);
            String response = u2Commons.ReadAnItem("uRESPONSES", NamedCommon.zID, "1", "", "");
            String strJunk = "<status>";
            int intJunk = response.indexOf(strJunk);
            int status = 200;
            if (intJunk >= 0) {
                intJunk = intJunk + strJunk.length();
                strJunk = response.substring(intJunk, intJunk + 3);
                try {
                    status = Integer.valueOf(strJunk);
                } catch (NumberFormatException nfe) {
                    status = 500;
                }
                u2Commons.DeleteAnItem("uRESPONSES", NamedCommon.zID);
                answer = DataConverter.ResponseHandler(String.valueOf(status), "DONOTALTER", response, GetDetail("FORMAT").toUpperCase());
            } else {
                answer = DataConverter.ResponseHandler(String.valueOf(status), "DONOTALTER", answer, GetDetail("FORMAT").toUpperCase());
            }
        }

        System.out.println("Answer: " + answer);

        return answer;
    }

    private static String StringifyRequest() {
        StringBuilder answer = new StringBuilder();
        boolean hasReplyTo = false;
        String zkey, zval;
        for (int i=0 ; i < reqKeys.size(); i++) {
            zkey = reqKeys.get(i);
            zval = reqVals.get(i);
            answer.append(zkey.toUpperCase() + is + zval + tm);
            if (zkey.toLowerCase().equals("replyto")) hasReplyTo = true;
        }
        if (!hasReplyTo) {
            answer.append("replyto" + is + getQ);
        }
        return answer.toString();
    }

    private static void Setup() {
        NamedCommon.ShowDateTime = true;
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);

        runProps.clear();
        SetupReturnCodes();
        firstTime = false;
    }

    private static boolean DecodeRequest(String request) {
        reqKeys = new ArrayList<>();
        reqVals = new ArrayList<>();
        String tempVer = "";
        reqKeys.clear();
        reqVals.clear();
        try {
            JSONObject obj = null;
            obj = new JSONObject(request);
            Iterator<String> jKeys = null;
            jKeys = obj.getJSONObject(jHeader).keys();
            String zkey, zval;
            while (jKeys.hasNext()) {
                zkey = jKeys.next();
                zval = obj.getJSONObject(jHeader).get(zkey).toString();
                reqKeys.add(zkey.toLowerCase());
                reqVals.add(zval);
                int posx;
                switch (zkey) {
                    case "action":
                        // swap action for map
                        posx = reqKeys.indexOf(zkey.toLowerCase());
                        zkey = "map";
                        reqKeys.set(posx,zkey);
                        zkey = "task";
                        zval = "050";
                        reqKeys.add(zkey.toLowerCase());
                        reqVals.add(zval);
                        break;
                    case "customer":
                        // swap customer for item
                        posx = reqKeys.indexOf(zkey.toLowerCase());
                        zkey = "item";
                        reqKeys.set(posx,zkey);
                        break;
                    case "version":
                        // stash it away until all properties are obtained
                        tempVer = zval;
                        break;
                }
            }
            if (!tempVer.equals("")) {
                zkey = "map";
                int posx = reqKeys.indexOf(zkey.toLowerCase());
                zval = reqVals.get(posx);
                zval = tempVer + "/" + zval;
                reqVals.set(posx, zval);
            }
            obj = null;
        } catch (JSONException je) {
            if (request.startsWith("{")) { return false; }
            System.out.println("Not JSON, so assuming this is an rFuel message!");
            request = request.replaceAll("\\r?\\n", "");
            request = request.replace("<IS>", "<is>");
            request = request.replace("<Is>", "<is>");
            request = request.replace("<iS>", "<is>");
            request = request.replace("<TM>", "<tm>");
            request = request.replace("<Tm>", "<tm>");
            request = request.replace("<tM>", "<tm>");

            String[] mParts = request.split("<tm>");
            int eop = mParts.length;
            if (eop == 1) return false;
            String line, mKey, mVal;
            for (int p=0; p < eop ; p++) {
                line = mParts[p];
                if (line.replace(" ", "").equals("")) continue;;
                String[] lParts = line.split("<is>");
                mKey = lParts[0];
                mVal = "";
                if (lParts.length > 0 && !line.endsWith("<is>")) mVal = lParts[1];
                reqKeys.add(mKey.toLowerCase());
                reqVals.add(mVal);
            }

            int chk = reqKeys.indexOf("format");
            if (chk < 0) {
                reqKeys.add("format");
                reqVals.add(NamedCommon.esbfmt);
            } else {
                String schk = reqVals.get(chk);
                switch (schk.toUpperCase()) {
                    case "XML":
                        break;
                    case "JSON":
                        break;
                    default:
                        reqVals.set(chk, NamedCommon.esbfmt);
                }
            }
        }
        return true;
    }

    private static String GetDetail(String ppty) {
        int posx = reqKeys.indexOf(ppty.toLowerCase());
        if (posx < 0) return "";
        return reqVals.get(posx);
    }

    private static void SetupReturnCodes() {
        ArrayList<String> ReturnCodes = new ArrayList<>();
        for (int rc = 0; rc < 1100; rc++) { ReturnCodes.add(""); }
        int nbrvals;
        String hCodes = ReadDiskRecord(BaseCamp + "/conf/http-return-codes.csv");
        if (!hCodes.equals("")) {
            String[] tmp = hCodes.split("\\r?\\n");
            nbrvals = tmp.length;
            int idx;
            for (int rc = 0; rc < nbrvals; rc++) {
                String[] tmp1 = tmp[rc].split(",");
                try {
                    idx = Integer.valueOf(tmp1[0]);
                    ReturnCodes.set(idx, tmp1[1]);
                } catch (NumberFormatException nfe) {
                    // skip the line
                }
            }
        }
        NamedCommon.ReturnCodes = ReturnCodes;
    }

    private static boolean isLicenced() {
        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);

        if (NamedCommon.upl.equals("")) {
            NamedCommon.upl = System.getProperty("user.dir");
            NamedCommon.BaseCamp = NamedCommon.upl;
        }
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            uCommons.SetMemory("domain", dbfHost);
            NamedCommon.BaseCamp = "\\\\"+dbfHost+"\\all\\upl";
            NamedCommon.upl = NamedCommon.BaseCamp;
            NamedCommon.slash = "/";
        }
        BaseCamp = NamedCommon.BaseCamp;

//        NamedCommon.isDocker = System.getProperty("docker", "").toLowerCase().equals("true");
        NamedCommon.isDocker = true;

        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties rfProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) return false;
        uCommons.SetCommons(rfProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        rfProps = null;

        return License.IsValid();
    }

    private static String ReadDiskRecord(String infile) {

        String rec = "";
        BufferedReader BRin = null;
        FileReader fr = null;
        try {
            fr = new FileReader(infile);
            BRin = new BufferedReader(fr);
            String line = null;
            try {
                line = BRin.readLine();
            } catch (IOException e) {
                System.out.println("read FAIL on " + infile);
                System.out.println(e.getMessage());
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    System.out.println("read FAIL on " + infile);
                    System.out.println(e.getMessage());
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                System.out.println("File Close: FAIL on " + infile);
                System.out.println(e.getMessage());
            }
        } catch (IOException e) {
            System.out.println("-------------------------------------------------------------------");
            System.out.println("File Access FAIL :: " + infile);
            System.out.println(e.getMessage());
            System.out.println("-------------------------------------------------------------------");
        }
        return rec;
    }

}
