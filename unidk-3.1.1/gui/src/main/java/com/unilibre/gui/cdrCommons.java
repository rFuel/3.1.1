package com.unilibre.gui;




import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.*;

public class cdrCommons {

    public ArrayList<String> ThisRunKey = new ArrayList<>();
    public ArrayList<String> ThisRunVal = new ArrayList<>();
    public String[][] escChars = new String[10][2];
    public String BaseCamp = "", hostname = "", Zmessage = "";
    public boolean ZERROR = false;

    private ArrayList<String> TasksArray = new ArrayList<>();
    private ArrayList<String> qNameArray = new ArrayList<>();
    private ArrayList<String> reqKeys = new ArrayList<>();
    private ArrayList<String> reqVals = new ArrayList<>();
    private ArrayList<String> users = new ArrayList<>();
    private boolean register = false, deregister = false;
    private boolean goDirect   = false;
    private boolean secureHTTP = false;
    private boolean isValid= true, isWebs = true, isWhse = false, isNRT  = false, isRest = true;
    private String jHeader = "request";
    private String URLhost;
    private int URLport, Expiry;
    private String URLpath;
    private int counter=1;
    private String bindAddess = "";
    private String protocol = "http";
    private String vTopic = "";
    private String task = "";
    private String action;
    private Properties bProps = null;
    public String queue      = "";
    public String replyto    = "059_CDR_Responses";

    public void Initialise() throws IOException {
        this.bindAddess = "";
        this.protocol = "http";
        this.task = "050";
        this.isValid= true;
        this.isWebs = true;
        this.isWhse = false;
        this.isNRT  = false;
        this.isRest = true;

        this.escChars[0][0] = "\"";
        this.escChars[0][1] = "&quot";
        this.escChars[1][0] = "'";
        this.escChars[1][1] = "&apos";
        this.escChars[2][0] = ">";
        this.escChars[2][1] = "&gt";
        this.escChars[3][0] = "<";
        this.escChars[3][1] = "&lt";
        this.escChars[4][0] = "&";
        this.escChars[4][1] = "&amp";
        this.escChars[5][0] = "~";
        this.escChars[5][1] = "comma";
        this.escChars[6][0] = "!";
        this.escChars[6][1] = "exclaimed";
        this.escChars[7][0] = "@";
        this.escChars[7][1] = "at";
        this.escChars[8][0] = "\\";
        this.escChars[8][1] = "slash";
        this.escChars[9][0] = "^";
        this.escChars[9][1] = "caret";

        this.bProps = null;
        this.TasksArray.clear();
        this.qNameArray.clear();

        if (this.secureHTTP) this.protocol = "https";
        this.bindAddess = this.protocol + "://" + this.URLhost + ":" + this.URLport;
        if (!this.URLpath.startsWith("/")) this.bindAddess += "/";
        this.bindAddess += this.URLpath;
        setURLhost(this.URLhost);
        setURLport(this.URLport);
        setURLpath(this.URLpath);
        
        this.Expiry = 60000;
    }

    public void SetMemory(String key, String val) {
        if (!key.equals("")) {
            int fnd = this.ThisRunKey.indexOf(key);
            if (fnd < 0) {
                this.ThisRunKey.add(key);
                this.ThisRunVal.add(val);
            } else {
                this.ThisRunVal.set(fnd, val);
            }
        }
    }

    public void SetHost(String host) { this.hostname = host; }

    public String GetHost() { return this.hostname; }
    
    public void SetURL(String host, int port, String path) {
        this.URLhost = host;
        this.URLport = port;
        this.URLpath = path;
        this.bindAddess = this.protocol + "//" + this.URLhost + ":" + this.URLport + this.URLpath;
    }

    public String HandleResponse(String request) {
        this.ZERROR = false;
        this.Zmessage = "";
        String reply = "";
        return reply;
    }

    private String jsonifyMessage(String instr) throws JSONException {

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

    public void SetBasecamp(String base) { this.BaseCamp = base; }

    public String GetBasecamp() { return this.BaseCamp; }

    public void setURLhost(String val) { if (!val.equals(URLhost)) { URLhost = val; } }

    public void setURLport(int val) { this.URLport = val; }

    public void setURLpath(String val) { this.URLpath = val; }

    public void setSecure(boolean val) { this.secureHTTP = val; }

    public void SetRegistrations(boolean reg, boolean dereg) {
        register = reg; deregister = dereg;
    }

    public boolean getMode() { return this.goDirect; }

    public String getAddress() { return this.bindAddess; }

    public String getPath() { return this.URLpath; }

    public int getPort() { return this.URLport; }

    public String getHost() { return this.URLhost; }

    public String GetZmessage() { return this.Zmessage; }

    public void SetError(boolean err, String msg) {
        this.ZERROR = err;
        this.Zmessage = msg;
    }
    
    public boolean isZERROR() { return this.ZERROR; }

    public void SetReqArrays() {
        this.reqKeys.clear();
        this.reqVals.clear();
    }

    public String BuildReqString(String rfMessage) {
        for (int i=0 ; i < this.reqKeys.size() ; i++) {
            rfMessage += this.reqKeys.get(i) + "<is>" + this.reqVals.get(i) + "<tm>";
        }
        return rfMessage;
    }

    public void AddReqDetails(String zkey, String zval) {
        this.reqKeys.add(zkey.toLowerCase());
        this.reqVals.add(zval);
    }

    public int ReqKeyLoc(String key) {
        return this.reqKeys.indexOf(key.toLowerCase());
    }

    public String GetReqVal(int i) {
        return this.reqVals.get(i);
    }

    public String FindReqKey(String key) {
        String ans = "";
        int fnd = ReqKeyLoc(key);
        if ( fnd >= 0 ) {
            ans = this.reqVals.get(fnd);
            this.reqKeys.remove(fnd);
            this.reqVals.remove(fnd);
        }
        return ans;
    }
}
