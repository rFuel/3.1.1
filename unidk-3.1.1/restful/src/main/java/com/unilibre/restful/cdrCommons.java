package com.unilibre.restful;

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.MessageInOut;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.msgCommons;
import com.unilibre.commons.uCommons;
import com.unilibre.core.MessageProtocol;
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
    private static boolean uncleHarry = false;
    private static boolean goDirect   = false;
    private static boolean secureHTTP = false;
    private static String jHeader = "request";
    private static String URLhost;
    private static int URLport;
    private static int counter=1;
    private static String URLpath;
    private static String bindAddess = "";
    private static String protocol = "http";
    private static String vTopic = "";
    private static String iam = "";
    private static String action;
    private static Properties bProps = null;
    public static String queue      = "";
    public static String replyto    = "059_CDR_Responses";
    private static MessageInOut mio;

    public static void Initialise() throws IOException {
        bindAddess = "";
        protocol = "http";
        NamedCommon.task = "050";

        String opsys = System.getProperty("os.name");
        methods.SetMemory("os.name", opsys);

        String sMem01="";
        int iMem01=0;
        long lMem01=0;

        methods.GetDomain();
        URLhost = NamedCommon.hostname;

        NamedCommon.isValid= true;
        NamedCommon.isWebs = true;
        NamedCommon.isWhse = false;
        NamedCommon.isNRT  = false;
        NamedCommon.isRest = true;

        if (NamedCommon.ReturnCodes.isEmpty()) {
            methods.SetupReturnCodes();
            NamedCommon.ReturnCodes = NamedCommon.ReturnCodes;
        }

        NamedCommon.escChars[0][0] = "\"";
        NamedCommon.escChars[0][1] = "&quot";
        NamedCommon.escChars[1][0] = "'";
        NamedCommon.escChars[1][1] = "&apos";
        NamedCommon.escChars[2][0] = ">";
        NamedCommon.escChars[2][1] = "&gt";
        NamedCommon.escChars[3][0] = "<";
        NamedCommon.escChars[3][1] = "&lt";
        NamedCommon.escChars[4][0] = "&";
        NamedCommon.escChars[4][1] = "&amp";
        NamedCommon.escChars[5][0] = "~";
        NamedCommon.escChars[5][1] = "comma";
        NamedCommon.escChars[6][0] = "!";
        NamedCommon.escChars[6][1] = "exclaimed";
        NamedCommon.escChars[7][0] = "@";
        NamedCommon.escChars[7][1] = "at";
        NamedCommon.escChars[8][0] = "\\";
        NamedCommon.escChars[8][1] = "slash";
        NamedCommon.escChars[9][0] = "^";
        NamedCommon.escChars[9][1] = "caret";

        lMem01 = NamedCommon.Expiry;
        String fname = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash + "rFuel.properties";
        Properties props = uCommons.LoadProperties(fname);
        uCommons.SetCommons(props);
        NamedCommon.Expiry = lMem01;        // cdr.properties sets the TTL for messages sent by Http2Server
        lMem01=0;

        bProps = null;
        TasksArray.clear();
        qNameArray.clear();
        if (!NamedCommon.Broker.equals("")) {
            fname = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash + NamedCommon.Broker;
            bProps = new Properties(uCommons.LoadProperties(fname));
            uCommons.BkrCommons(bProps);
//            NamedCommon.messageBrokerUrl = bProps.getProperty("url", "");
            vTopic = bProps.getProperty("topic", "");
            if (!vTopic.equals("")) queue = "VirtualTopic." + vTopic;
            TasksArray = new ArrayList<>(Arrays.asList(bProps.getProperty("tasks").split("\\,")));
            qNameArray = new ArrayList<>(Arrays.asList(bProps.getProperty("qname").split("\\,")));
        }

        boolean URLnotset = false;
        if (URLpath == null ) URLnotset = true;
        if (URLpath == "" )   URLnotset = true;
        if (URLnotset) {
            //
            // default URL is in rFuel.properties
            //
            String checkURL = props.getProperty("serveurl", "");
            if (!checkURL.equals("")) URLpath = checkURL;
            //
            // URL can be over-riden by script property
            //
            checkURL = System.getProperty("serveurl", "");
            if (!checkURL.equals("")) URLpath = checkURL;

        }

        setURLhost(URLhost);
        setURLport(URLport);
        setURLpath(URLpath);
        NamedCommon.isRest = true;

        mio = new MessageInOut();

    }

    public static void SetIAM(String invar) { iam = invar; }

    public static String HandleResponse(String request) {

        NamedCommon.ZERROR = false;
        NamedCommon.ZERROR = false;
        mio.SetZerror(false, "");
        NamedCommon.Zmessage = "";

        if (vTopic.equals("") && TasksArray.size() > 0) {
            queue = qNameArray.get(TasksArray.indexOf("050")) + "_001";
        }

        NamedCommon.Zmessage = "";
        NamedCommon.ZERROR = false;
        NamedCommon.CorrelationID = "";
        
        String reply="", message="";

        if (request.length() > 0) {
            action = "";
            message = BreakDown(request);
            uCommons.uSendMessage("   .) HandleResponse("+action+")");
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage ="ERROR: The request body was empty";
            uCommons.uSendMessage("   .) HandleResponse("+NamedCommon.Zmessage+")");
        }

        if (NamedCommon.CorrelationID.equals("")) {
            NamedCommon.CorrelationID = (Thread.currentThread().getName() + UUID.randomUUID().toString());
            while (NamedCommon.CorrelationID.contains(" ")) {NamedCommon.CorrelationID = NamedCommon.CorrelationID.replace("\\ ", "-"); }
        }

        if (!NamedCommon.ZERROR) {
            NamedCommon.task = "050";

            if (!message.contains(replyto)) message += "replyto<is>" + replyto + "<tm>";
            if (!message.toLowerCase().contains("correlationid")) message += "correlationid<is>" + NamedCommon.CorrelationID + "<tm>";
            if (!message.toLowerCase().contains("ttl<is>")) message += "ttl<is>" + NamedCommon.Expiry + "<tm>";

            NamedCommon.MessageID = NamedCommon.CorrelationID;

            if (goDirect) {
                NamedCommon.ZERROR = false;
                NamedCommon.ZERROR = false;
                NamedCommon.Zmessage = "";
                NamedCommon.MessageID += "_" + counter;
                counter++;
                uCommons.uSendMessage("   .) Message:  task = "+action);
                uCommons.uSendMessage("   .) Calling MessageProtocol("+NamedCommon.CorrelationID+")");
                uCommons.MessageToAPI(message);
                reply = MessageProtocol.handleProtocolMessage("050", "1", message);
            } else {
                uCommons.uSendMessage("   .) Message:  task = "+action);
                uCommons.uSendMessage("   .) ReplyTo:  " + replyto);
                uCommons.uSendMessage("   .) Correll:  " + NamedCommon.CorrelationID);
                uCommons.uSendMessage("   .) Sending message to   : " + queue);
                uCommons.uSendMessage("   .) Waiting for reply on : " + replyto);
                uCommons.uSendMessage("   .) Calling MessageInOut.SendAndReceive(" + NamedCommon.CorrelationID + ")");
                if (!message.startsWith("{")) message = msgCommons.jsonifyMessage(message);
                mio.SetFilter(NamedCommon.CorrelationID);
                reply = mio.SendAndReceive(NamedCommon.messageBrokerUrl, queue, replyto, NamedCommon.CorrelationID , message, "CDR-" + NamedCommon.pid);
                uCommons.uSendMessage("   .) MessageInOut.reply.length("+reply.length()+")");
            }

            iam = "";

            if (mio.isZERROR()) {
                uCommons.uSendMessage("   .) MessageInOut.ZERROR("+mio.Zmessage+")");
                reply = methods.ResponseHandler("400", "Bad Request", reply, "JSON");
                mio.SetZerror(false, "");
            }

            NamedCommon.ZERROR   = mio.ZERROR;
            NamedCommon.Zmessage = mio.Zmessage;
            
            if (NamedCommon.ZERROR) reply = methods.ResponseHandler("400", "Bad Request", NamedCommon.Zmessage, "JSON");
        } else {
            reply = methods.ResponseHandler("400", "Bad Request", NamedCommon.Zmessage, "JSON");
        }

        return reply;
    }

    private static String jsonifyMessage(String instr) throws JSONException {

        // ----------- clean up the instr message ----------------------
        instr = instr.replaceAll("\\r?\\n", " ");
        instr = instr.replace("<IS>", "<is>");
        instr = instr.replace("<Is>", "<is>");
        instr = instr.replace("<iS>", "<is>");
        instr = instr.replace("<TM>", "<tm>");
        instr = instr.replace("<Tm>", "<tm>");
        instr = instr.replace("<tM>", "<tm>");
        // -------------------------------------------------------------

        String answer="";
        JSONObject obj = new JSONObject();

        String[] mParts = instr.split("<tm>");
        int eop = mParts.length, eol=0;
        String line, mKey, mVal;
        for (int p=0; p < eop ; p++) {
            line = mParts[p];
            if (line.replace("\\ ", "").equals("")) continue;
            String[] lParts = line.split("<is>");
            mKey = lParts[0].replace("\\ ", "");
            mVal = "";
            if (lParts.length > 1) mVal = lParts[1];
            obj.put(mKey.toUpperCase(), mVal);
        }

        answer = obj.toString();
        obj = null;
        return answer;
    }

    private static String BreakDown(String messageIn) {
        JSONObject obj = null;
        try {
            obj = new JSONObject(messageIn);
        } catch (JSONException je) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = je.getMessage();
            return NamedCommon.Zmessage;
        }

        String rfMessage="", inMap="";
        String fmt = "json", inPing="";
        String inAction="",  inBank="", inItem="", inAuth="", version="", inCorrel, inPage, inPgSz;
        SetRequestData(obj);
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
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = je.getMessage();
            return NamedCommon.Zmessage;
        }

        if (inCorrel.equals("")) {
            inCorrel = UUID.randomUUID().toString();
            uCommons.uSendMessage("No correlatoin id provided !!");
            uCommons.uSendMessage("Using UUID : " + inCorrel);
        }
        inCorrel +=  "_" + iam;

        NamedCommon.CorrelationID = inCorrel;

        if (inAction.equals("") && inPing.equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "ERROR: no [action] has been provided.";
            uCommons.uSendMessage(NamedCommon.Zmessage);
            return NamedCommon.Zmessage;
        }
        if (inAction.equals("GetCustomerId")) inItem = inItem.toUpperCase();

        if (inBank.equals("") && inPing.equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "ERROR: no [BSB] has been provided.";
            uCommons.uSendMessage(NamedCommon.Zmessage);
            return NamedCommon.Zmessage;
        }

        if (inItem.equals("") && inPing.equals("")) {
            uCommons.uSendMessage("WARNING: no [customer] ID has been provided.");
        }

        if (!inAuth.equals("")) {
            boolean valid = Verify(inAuth);
            if (!valid) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "Failure in credentials.";
                return NamedCommon.Zmessage;
            }
        }

        if (inPage.equals("")) inPage = "1";
        if (inPgSz.equals("")) inPgSz = "1000";

        inMap = "OB/";
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
                NamedCommon.debugging = false;
            }
            action = "ping";
            counter++;
        }

        return rfMessage;
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
            uCommons.uSendMessage("ERROR: Base64 - " + e.getMessage());
            NamedCommon.Zmessage = "";
            NamedCommon.ZERROR = true;
            return false;
        }
        String[] authParts = dStr.split("\\:");
        if (authParts.length != 3) { return false; }
        String exp = authParts[0];
        String usr = authParts[1];
        String pwd = authParts[2];

        Properties userProps = uCommons.LoadProperties(NamedCommon.BaseCamp + NamedCommon.slash + "conf/uxusers/" + usr);
        if (userProps.size() == 0) return false;

        String pword = userProps.getProperty("password");

        if (pword.startsWith("ENC(")) {
            pword = pword.substring(4, pword.length());
            pword = pword.substring(0, pword.length() - 1);
            pword = uCipher.Decrypt(pword);
        }

        if (!pword.equals(pwd)) {
            NamedCommon.Zmessage = "Authorisation Failure - username / password invalid";
            NamedCommon.ZERROR = true;
            return false;
        } else {
            if (usr.toLowerCase().equals("debug")) return true;

            SimpleDateFormat fmtTime = new SimpleDateFormat("HH-mm-ss");
            SimpleDateFormat fmtDate = new SimpleDateFormat("yyyy-MM-dd");

            String inDate = methods.FieldOf(exp, "\\|", 1);
            String inTime = methods.FieldOf(exp, "\\|", 2);
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
                    uCommons.uSendMessage("ALERT: correct credentials but has expired by " + (sDiff - 30) + " seconds.");
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
                uCommons.uSendMessage("ALERT: " + e.getMessage());
                return false;
            }
            return true;
        }
    }

    public static void SetResponseData(JSONObject obj) {
        reqKeys.clear();
        reqVals.clear();
        Iterator<String> jKeys = null;
        jKeys = obj.getJSONObject("body").keys();
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = obj.getJSONObject("body").get(zkey).toString();
            reqKeys.add(zkey.toLowerCase());
            reqVals.add(zval);
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
            URLhost = val;
        }
    }

    public static void setURLport(int val) { URLport = val; }

    public static void setURLpath(String val) { URLpath = val; }

    public static void setSecure(boolean val) {secureHTTP = val; }

    public static void setMode(boolean val) {
        goDirect = val;
    }

    public static boolean getMode() { return goDirect; }

    public static String getAddress() {
        if (secureHTTP) protocol = "https";
        bindAddess = protocol + "://" + URLhost + ":" + URLport;
        if (!URLpath.startsWith("/")) bindAddess += "/";
        bindAddess += URLpath;
        return bindAddess;
    }

    public static String getPath() { return URLpath; }

    public static int getPort() { return URLport; }

    public static String getHost() { return URLhost; }

}
