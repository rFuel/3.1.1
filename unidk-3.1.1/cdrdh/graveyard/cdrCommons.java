package com.unilibre.cdroem;


import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class cdrCommons {

    private static ArrayList<String> TasksArray = new ArrayList<>();
    private static ArrayList<String> qNameArray = new ArrayList<>();
    private static ArrayList<String> reqKeys = new ArrayList<>();
    private static ArrayList<String> reqVals = new ArrayList<>();
    private static boolean goDirect = false, secureHTTP = false;
    private static String jHeader = "request", URLhost, URLpath, bindAddess = "", protocol = "http", action;
    private static int URLport, counter=1;
    public static Properties bProps = null;

    public static void Initialise() throws IOException {
        bindAddess = "";
        protocol = "http";
        long lMem01=0;
        lMem01 = commons.Expiry;
        commons.Expiry = lMem01;        // cdr.properties sets the TTL for messages
        TasksArray.clear();
        qNameArray.clear();

        if (commons.ReturnCodes.isEmpty()) {
            commons.SetupReturnCodes();
            if (commons.ZERROR) return;
        }

        boolean URLnotset = false;
        if (URLpath == null ) URLnotset = true;
        if (URLpath == "" )   URLnotset = true;
        if (URLnotset) {
            // URL can be over-riden by script property
            String checkURL = System.getProperty("serveurl", "");
            if (!checkURL.equals("")) URLpath = checkURL;
        }

        if (secureHTTP) protocol = "https";
        bindAddess = protocol + "://" + URLhost + ":" + URLport;
        if (!URLpath.startsWith("/")) bindAddess += "/";
        bindAddess += URLpath;
        setURLhost(URLhost);
        setURLport(URLport);
        setURLpath(URLpath);
    }

    public static String LoadBP(nsCommonData comData, String message) {
        String answer = "";
//        JSONObject obj = new JSONObject(message);
//        SetRequestData(obj);
//        obj = null;
//        String key = GetRequestValue("ekey");
//        String file= GetRequestValue("file");
//        String iid = GetRequestValue("item");
//        String txt = GetRequestValue("prog");
//        commons.uSendMessage("   .) LoadBP("+file+", " + iid+")");
//        key = (key+"unilibreptyltdaustralia").substring(0,16);
//        String prog= "";
//        // ------------------------------------------------------
//        try {
//            String characterEncoding       = "UTF-8";
//            String cipherTransformation    = "AES/CBC/PKCS5PADDING";
//            String aesEncryptionAlgorithem = "AES";
//            Cipher dCipher = Cipher.getInstance(cipherTransformation);
//            byte[] dKey = key.getBytes(characterEncoding);
//            SecretKeySpec dSecretKey = new SecretKeySpec(dKey, aesEncryptionAlgorithem);
//            IvParameterSpec dIVparameterspec = new IvParameterSpec(dKey);
//            dCipher.init(Cipher.DECRYPT_MODE, dSecretKey, dIVparameterspec);
//            Base64.Decoder decoder = Base64.getDecoder();
//            byte[] dCipherText = decoder.decode(txt.getBytes(characterEncoding));
//            prog = new String(dCipher.doFinal(dCipherText), characterEncoding);
//        } catch (Exception E) {
//            System.err.println("decrypt Exception : "+E.getMessage());
//        }
//        if (prog.equals("")) return answer;
//        // ------------------------------------------------------
//        String[] callString = new String[4];
//        callString[0] = "";
//        callString[1] = file;
//        callString[2] = iid;
//        callString[3] = prog;
//        nsOBMethods methods = new nsOBMethods();
//        callString = methods.LoadProgram(comData,"SLBP", callString);
//        methods = null;
//        answer = callString[0];
//        if (answer.equals("{EOX}{ok}")) {
//            answer = commons.ResponseHandler("200", "OK", "", "JSON");
//        } else {
//            answer = commons.ResponseHandler("500", "Internal Server Error", "Compile failed", "JSON");
//        }
        return answer;
    }

    public static String HandleResponse(nsCommonData comData, String request) throws IOException {

        String reply="", message="";
        if (comData.ZERROR) return comData.Zmessage;

        if (request.length() > 0) {
            action = comData.nsMsgGet("action");
            comData.uSendMessage("   .) RequestHandler("+action+")");
        } else {
            comData.ZERROR = true;
            comData.Zmessage ="ERROR: The request body was empty";
            return commons.ResponseHandler("400", "Bad Request", commons.Zmessage, "JSON");
        }

        String CorrID  = comData.nsMsgGet("correlationid");

        if (!comData.ZERROR) {
            comData.uSendMessage("   .) CorrelationID(" + CorrID + ")");
            reply = HandleRequest(comData, "");
            if (comData.ZERROR) reply = comData.Zmessage;
            if (reply.contains("<<PASS>>")) reply = comData.DataList.get(0);
        } else {
            reply = commons.ResponseHandler("400", "Bad Request", comData.Zmessage, "JSON");
        }
        return reply;
    }

    public static String HandleRequest(nsCommonData comData, String prefix) throws IOException {
        comData.logHeader = prefix;
        if (!comData.logHeader.endsWith(" ")) comData.logHeader += " ";
        String reply = "";

        String xMap = comData.nsMsgGet("map");
        String fqn =  comData.BaseCamp + comData.slash + xMap;
        commons.ZERROR = false;
        commons.Zmessage = "";
        String mapDetails = commons.ReadDiskRecord(fqn);
        if (commons.ZERROR) {
            commons.ZERROR = false;
            commons.Zmessage = "";
            return "";
        }
        comData.nsMapLoader(mapDetails);
        if (comData.ZERROR) return "";

        if (comData.nsMapGet("class").toLowerCase().equals("cdrob")) {
            String type = comData.nsMapGet("domain").toLowerCase();

            nsOBMethods methods = new nsOBMethods();

            switch (type) {
                case "customerid":
                    reply = methods.GetCustomerID(comData);
                    break;
                case "customer":
                    reply = methods.GetCustomer(comData);
                    break;
                case "account":
                    reply = methods.GetAccounts(comData);
                    break;
                case "transactions":
                    reply = methods.GetTransactions(comData);
                    break;
                case "transactionsv2":
                    reply = methods.GetTransactionsV2(comData);
                    break;
                case "payees":
                    reply = methods.GetPayees(comData);
                    break;
                case "payments":
                    reply = methods.GetPayments(comData);
                    break;
                case "":
                    break;
            }
        } else {
            logger.logthis(comData.logHeader +   " Issue in Action map - it needs a class and domain definition");
        }

        if (!comData.ZERROR) logger.logthis(comData.logHeader +   " rFuel has completed instruction for " + xMap);
        return reply;
    }

    private static String BreakDown(String messageIn) {
        if (reqKeys == null) {
            DecodeRequest(messageIn);
            if (commons.ZERROR) return "";
        }

        String rfMessage="", inMap="",  fmt = "json", inPing="";
        String inAction="",  inBank="", inItem="", inAuth="", version="", inCorrel, inPage, inPgSz;
        try{
            inPing    = GetRequestValue("ping");
            inAction  = GetRequestValue("action");
            inBank    = GetRequestValue("bsb");
            inItem    = GetRequestValue("customer");
            inAuth    = GetRequestValue("credentials");
            version   = GetRequestValue("version");
            inCorrel  = GetRequestValue("correlationid");
            inPage    = GetRequestValue("page");
            inPgSz    = GetRequestValue("page-size");
        } catch (JSONException je) {
            commons.ZERROR = true;
            commons.Zmessage = je.getMessage();
            return commons.Zmessage;
        }

        if (inAction.equals("") && inPing.equals("")) {
            commons.ZERROR = true;
            commons.Zmessage = "ERROR: no [action] has been provided.";
            commons.uSendMessage(commons.Zmessage);
            return commons.Zmessage;
        }
        if (inAction.equals("GetCustomerId")) inItem = inItem.toUpperCase();

        if (inBank.equals("") && inPing.equals("")) {
            commons.ZERROR = true;
            commons.Zmessage = "ERROR: no [BSB] has been provided.";
            commons.uSendMessage(commons.Zmessage);
            return commons.Zmessage;
        }

        if (inItem.equals("") && inPing.equals("")) {
            commons.uSendMessage("WARNING: no [customer] ID has been provided.");
        }

        if (!inAuth.equals("")) {
            boolean valid = Verify(inAuth);
            if (!valid) {
                commons.ZERROR = true;
                commons.Zmessage = "Failure in credentials.";
                return commons.Zmessage;
            }
        }

        if (inPage.equals("")) inPage = "1";
        if (inPgSz.equals("")) inPgSz = "1000";

        inMap = "";
        if (!version.equals("")) inMap += version+"/";
        inMap += inAction;

        if ( inPing.equals("") ) {
            rfMessage = "task<is>050<tm>";
            rfMessage += "format<is>" + fmt + "<tm>";
            rfMessage += "shost<is>hosts/" + inBank + "<tm>";
            rfMessage += "map<is>" + inMap + "<tm>";
            rfMessage += "item<is>" + inItem + "<tm>";
            rfMessage += "correlationid<is>" + inCorrel + "<tm>";
            rfMessage += "page-size<is>" + inPgSz + "<tm>";
            rfMessage += "page<is>" + inPage + "<tm>";
            action = inMap;
            for (int i=0 ; i < reqKeys.size() ; i++) {
                rfMessage += reqKeys.get(i) + "<is>" + reqVals.get(i) + "<tm>";
            }
        } else {
            inCorrel = "heartbeat_" + counter;
            rfMessage = "task<is>999<tm>format<is>" + fmt + "<tm>correlationid<is>" + inCorrel + "<tm>";
            if (inPing.toLowerCase().equals("show")) {
                rfMessage += "debug<is>true<tm>";
            } else {
                commons.debugging = false;
            }
            action = "ping";
            counter++;
        }

        return rfMessage;
    }

    public static void DecodeRequest(String messageIn) {
        try {
            JSONObject obj = null;
            obj = new JSONObject(messageIn);
            SetRequestData(obj);
            obj = null;
        } catch (JSONException je) {
            commons.ZERROR = true;
            commons.Zmessage = je.getMessage();
        }
    }

    private static boolean Verify(String auth) {
        Base64.Decoder decoder = Base64.getDecoder();
        String dStr = "";
        try {
            dStr = new String(decoder.decode(auth));
            // dStr: -------------------------------
            // [0]  : 2020-05-05|11:14:63
            // [1]  : admin     (username)
            // [2]  : admin     (password)
        } catch (IllegalArgumentException e) {
            commons.uSendMessage("ERROR: Base64 - " + e.getMessage());
            commons.Zmessage = "";
            commons.ZERROR = true;
            return false;
        }
        String[] authParts = dStr.split("\\:");
        if (authParts.length != 3) { return false; }
        String exp = authParts[0];
        String usr = authParts[1];
        String pwd = authParts[2];

        Properties userProps = commons.LoadProperties(commons.BaseCamp + commons.slash + "conf/uxusers/" + usr);
        if (userProps.size() == 0) return false;

        String pword = userProps.getProperty("password");

        if (!pword.equals(pwd)) {
            commons.Zmessage = "Authorisation Failure - username / password invalid";
            commons.ZERROR = true;
            return false;
        } else {
            if (usr.toLowerCase().equals("debug")) return true;

            SimpleDateFormat fmtTime = new SimpleDateFormat("HH-mm-ss");
            SimpleDateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");

            String inDate = exp.split("\\|")[0];
            String inTime = exp.split("\\|")[1];
            String mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
            String mTime = new SimpleDateFormat("HH-mm-ss").format(Calendar.getInstance().getTime());

            try {
                Date d1 = fmtDate.parse(inDate);
                Date d2 = fmtDate.parse(mDate);
                if (d1.compareTo(d2) != 0) { return false; }
                Date t1 = fmtTime.parse(mTime);
                Date t2 = fmtTime.parse(inTime);
                long diff = t1.getTime() - t2.getTime();
                int sDiff = Integer.valueOf((int) (diff/1000));
                if (sDiff > 30) {
                    commons.uSendMessage("ALERT: correct credentials but expired by " + (sDiff - 30) + " seconds.");
                    return false;
                }
                fmtTime = null;
                fmtDate = null;
                mDate   = null;
                mTime   = null;
                d1 = null;
                d2 = null;
                t1 = null;
                t2 = null;
                diff = 0;
            } catch (ParseException e) {
                commons.uSendMessage("ALERT: " + e.getMessage());
                return false;
            }
            return true;
        }
    }

    private static void SetRequestData(JSONObject obj) {
        reqKeys.clear();
        reqVals.clear();
        Iterator<String> jKeys = null;
        jKeys = obj.getJSONObject(jHeader).keys();
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = obj.getJSONObject(jHeader).get(zkey).toString();
            reqKeys.add(zkey.toLowerCase());
            reqVals.add(zval);
        }
    }

    public static String GetRequestValue(String key) throws JSONException {
        String ans = "";
        int fnd = reqKeys.indexOf(key.toLowerCase());
        if ( fnd >= 0 ) {
            ans = reqVals.get(fnd);
            reqKeys.remove(fnd);
            reqVals.remove(fnd);
        }
        return ans;
    }

    public static void setURLhost(String val) {
        if (!val.equals(URLhost)) {
            if (URLhost != null) commons.uSendMessage("Changing URLhost from " + URLhost + "  to   " + val);
            URLhost = val;
        }
    }

    public static void setURLport(int val) { URLport = val; }

    public static void setURLpath(String val) { URLpath = val; }

    public static void setSecure(boolean val) {secureHTTP = val; }

    public static void setMode(boolean val) { goDirect = val; }

    public static String getAddress() { return bindAddess; }

    public static String getPath() { return URLpath; }

    public static int getPort() { return URLport; }

    public static String getHost() { return URLhost; }

}
