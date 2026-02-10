package com.unilibre.cdroem;

import com.northgateis.reality.rsc.RSCConnection;
import com.northgateis.reality.rsc.RSCException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;

public class nsCommonData {
    public ArrayList<String> jField = new ArrayList<>();
    public ArrayList<String> jValue = new ArrayList<>();
    public boolean ZERROR, DBconnected;
    public String Zmessage = "", usage = System.getProperty("usage", "cdr").toLowerCase();
    public ArrayList<String> impTmpls = new ArrayList<>();
    public ArrayList<String> impTlines = new ArrayList<>();
    public ArrayList<String> impLines = new ArrayList<>();
    public int pg, pgSz, prevPage, nextPage, lastPage, totRecs, dsdControl, nbrDays = 366;
    public String page, pgSize, lowdate, iid, blank, logHeader, BaseCamp;
    public String FMark, Tmark, IMark, KMark, VMark, SMark, slash;
    public String host, dbase, dbuser, dbacct, VTopic;
    public String cwd = System.getProperty("user.dir");
    public ArrayList<String> MSGkeys = new ArrayList<>();
    public ArrayList<String> MSGvals = new ArrayList<>();
    public ArrayList<String> MAPkeys = new ArrayList<>();
    public ArrayList<String> MAPvals = new ArrayList<>();
    public ArrayList<String> SubsList = new ArrayList<>();
    public ArrayList<String> DataList = new ArrayList<>();
    public ArrayList<String> Templates = new ArrayList<>();
    public ArrayList<String> TmplList = new ArrayList<>();
    public RSCConnection rcon;


    public nsCommonData() {
        jField.clear();
        jValue.clear();
        impTmpls.clear();
        impTlines.clear();
        impLines.clear();
        MSGkeys.clear();
        MSGvals.clear();
        MAPkeys.clear();
        MAPvals.clear();
        SubsList.clear();
        DataList.clear();
        Templates.clear();
        TmplList.clear();

        FMark = "<fm>";
        Tmark = "<tm>";
        IMark = "<im>";
        KMark = "<km>";
        VMark = "<vm>";
        SMark = "<sm>";
        BaseCamp = cwd + "/cdr";
        Zmessage = "";
        page = "";
        pgSize = "";
        lowdate = "";
        iid = "";
        logHeader = "";
        blank = "[<END>]";
        slash = "/";
        host = "";
        dbase = "";
        dbuser = "";
        dbacct = "";
        VTopic = "";

        ZERROR = false;
        DBconnected = false;

        pg = 0;
        pgSz = 0;
        prevPage = 0;
        nextPage = 0;
        lastPage = 0;
        totRecs = 0;
        dsdControl = 0;
        nbrDays = 366;
        rcon = null;
    }

    public void uSendMessage(String inMsg) {
        if (inMsg == null) return;
        logger.logthis(inMsg);
    }

    public void nsReqLoader(String request) {
        this.MSGkeys.clear();
        this.MSGvals.clear();
        try {
            JSONObject obj = null;
            obj = new JSONObject(request);
            SetRequestData(obj);
            obj = null;
        } catch (JSONException je) {
            this.ZERROR = true;
            this.Zmessage = je.getMessage();
        }
        if (this.MSGkeys.indexOf("MAP") < 0) {
            String action = nsMsgGet("action");
            this.MSGkeys.add("MAP");
            this.MSGvals.add(action);
        }
    }

    private void SetRequestData(JSONObject obj) {
        Iterator<String> jKeys = null;
        jKeys = obj.getJSONObject(cdrReceiver.jHeader).keys();
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = obj.getJSONObject(cdrReceiver.jHeader).get(zkey).toString();
            this.MSGkeys.add(zkey.toUpperCase());
            this.MSGvals.add(zval);
        }
    }

    public void nsMsgLoader(String message) {
        this.MSGkeys.clear();
        this.MSGvals.clear();
        message = message.replaceAll("\\r?\\n", " ");
        while (message.toLowerCase().contains("<is>")) {
            message = message.replace("<is>", "=");
            message = message.replace("<IS>", "=");
            message = message.replace("<Is>", "=");
            message = message.replace("<iS>", "=");
        }
        message = message.replace("<TM>", Tmark);
        message = message.replace("<Tm>", Tmark);
        message = message.replace("<tM>", Tmark);
        String[] values = message.split(Tmark);
        int nbrValues = values.length;
        int posx;
        String key = "", val = "";
        for (int i = 0; i < nbrValues; i += 1) {
            posx = values[i].indexOf("=");
            if (posx > 0) {
                key = values[i].substring(0, posx).replaceAll("\\ ", "");
                val = values[i].substring(posx + 1, values[i].length());
                this.MSGkeys.add(key.toUpperCase());
                this.MSGvals.add(val);
            }
        }
        values = null;
        message = null;
    }

    public String nsMsgGet(String key) {
        String value = "";
        String uKey = key.toUpperCase();
        int fnd = this.MSGkeys.indexOf(uKey);
        if (fnd > -1) value = this.MSGvals.get(fnd);
        return value;
    }

    public String nsMapGet(String key) {
        String value = "";
        String uKey = key.toUpperCase();
        int fnd = this.MAPkeys.indexOf(uKey);
        if (fnd > -1) value = this.MAPvals.get(fnd);
        return value;
    }

    public void nsMapLoader(String maplines) {
        maplines = maplines.trim().replaceAll("\\ +", " ");
        String[] values = maplines.split("\\r?\\n");
        int nbrValues = values.length, px;
        this.MAPkeys.clear();
        this.MAPvals.clear();
        if (nbrValues > 1) {
            String key, val, line, chr;
            for (int i = 0; i < nbrValues; i += 1) {
                values[i] = values[i].replace(" = ", "=");
                line = values[i];
                if (line.equals("")) continue;
                chr = line.substring(0, 1);
                if ("*#".indexOf(chr) > -1) continue;
                if (line.indexOf("=") > 0) {
                    px = line.indexOf("=");
                    if (px > -1) {
                        key = line.substring(0, px);
                        val = line.substring(px + 1, line.length());
                        this.MAPkeys.add(key.toUpperCase());
                        this.MAPvals.add(val);
                    }
                }
            }
        }
        values = null;
    }

    public void ConnectProxy() {
        String conMsg = "nsCommonData.ConnectProxy() ";
        conMsg += "host: [" + this.host + "]  database: [" + this.dbase + "]";
        conMsg += "  account [" + this.dbacct + "]";
        logger.logthis(conMsg);

        this.rcon = new RSCConnection(this.host, this.dbase, this.dbuser, this.dbacct);
        try {
            this.rcon.connect();
            this.DBconnected = true;
//            logger.logthis(this.logHeader + "DB connection: Success");
        } catch (RSCException e) {
            this.ZERROR = true;
            logger.logthis(this.logHeader + "ERROR: DB connection failure : " + e.toString());
        }
    }

    public void DisconnectProxy() {
        String conMsg = "nsCommonData.DisconnectProxy() ";
        this.rcon.disconnect();
        this.rcon = null;
        this.DBconnected = false;
        logger.logthis(conMsg);
    }
}
