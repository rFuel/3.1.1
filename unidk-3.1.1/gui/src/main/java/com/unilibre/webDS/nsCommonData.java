package com.unilibre.webDS;

import com.northgateis.reality.rsc.RSCConnection;
import com.northgateis.reality.rsc.RSCException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class nsCommonData {
    public boolean DBconnected;
    public String usage=System.getProperty("usage", "cdr");
    public String logHeader;
    public String host, dbase, dbuser, dbacct;
    public Map<String, String> msgMap = new HashMap<>();
    public Map<String, String> mapMap = new HashMap<>();
    public ArrayList<String> SubsList = new ArrayList<>();
    public ArrayList<String> DataList = new ArrayList<>();
    public ArrayList<String> Templates = new ArrayList<>();
    public ArrayList<String> TmplList = new ArrayList<>();
    public RSCConnection rcon;


    public nsCommonData() throws IOException {
        host=""; dbase=""; dbuser=""; dbacct="";

        DBconnected=false;

        String defaultCFG = "biza.properties";

        String cfg = System.getProperty("config", defaultCFG);
        try {
            InputStream is = new FileInputStream(cfg);
            Properties props = new Properties();
            props.load(is);
            host     = props.getProperty("pxIP", "ERROR");
            dbase    = props.getProperty("pxDB", "ERROR");
            dbuser   = props.getProperty("pxUP", "ERROR");
            dbacct   = props.getProperty("pxAC", "ERROR");
        } catch (FileNotFoundException e) {
            System.out.println("File Not Found : " + cfg);
            throw e;
        } catch (IOException e) {
            System.out.println("Error loading properties " + e.getMessage());
            throw e;
        }

        rcon = null;
    }

    public void nsReqLoader(String request) {
        try {
            JSONObject obj = null;
            obj = new JSONObject(request);
            SetRequestData(obj);
        } catch (JSONException je) {
            throw je;
        }
        if (!this.msgMap.containsKey("MAP")) {
            String action = nsMsgGet("action");
            this.msgMap.put("MAP", action);
        }
    }

    private void SetRequestData(JSONObject obj) {
        Iterator<String> jKeys;
        jKeys = obj.getJSONObject(cdrReceiver.jHeader).keys();
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = obj.getJSONObject(cdrReceiver.jHeader).get(zkey).toString();
            this.msgMap.put(zkey.toUpperCase(),zval);
        }
    }

    public String nsMsgGet(String key) {
        String uKey = key.toUpperCase();
        return msgMap.getOrDefault(uKey, "");
    }

    public String nsMapGet(String key) {
        String uKey = key.toUpperCase();
        return mapMap.getOrDefault(uKey, "");
    }

    public void nsMapLoader(String maplines) {
        maplines = maplines.trim().replaceAll(" +", " ");
        String[] values = maplines.split("\\r?\\n");
        int nbrValues = values.length, px;
        this.mapMap.clear();
        if (nbrValues > 1) {
            String key, val, line, chr;
            for (int i = 0; i < nbrValues; i += 1) {
                values[i] = values[i].replace(" = ", "=");
                line = values[i];
                if (line.equals("")) continue;
                chr = line.substring(0, 1);
                if ("*#".contains(chr)) continue;
                if (line.indexOf("=") > 0) {
                    px = line.indexOf("=");
                    if (px > -1) {
                        key = line.substring(0, px);
                        val = line.substring(px + 1);
                        this.mapMap.put(key.toUpperCase(), val);
                    }
                }
            }
        }
        values = null;
    }

    public void ConnectProxy() throws RSCException {
        String conMsg = "SourceDB.Connect_Reality() ";
        conMsg += "host: [" + this.host + "]  database: [" + this.dbase + "]";
        conMsg += "  account [" + scrubPassword(this.dbacct) + "]";
        logger.logthis(this.logHeader +  conMsg);

        if (this.rcon != null) {
            try {
                this.rcon.verifyConnection();
                logger.logthis(this.logHeader + "DB Connection still connected. Reusing old connection.");
            } catch (RSCException e) {
                logger.logthis(this.logHeader + "DB Connection has stopped working. Discarding old connection. Error was: " + e.getMessage());
                this.rcon = null;
            }
        }
        if (this.rcon == null) {
            this.rcon = new RSCConnection(this.host, this.dbase, this.dbuser, this.dbacct);
            try {
                this.rcon.connect();
                this.DBconnected = true;
                logger.logthis(this.logHeader + "DB connection: Success");
            } catch (RSCException e) {
                logger.logthis(this.logHeader + "ERROR: DB connection failure : " + e.getMessage());
                throw e;
            }
        }
    }

    /**
     * The account can have a password in it. Like this: BANKACCT,Passw0rd so strip it out
     * @param dbacct: The account being logged into on the proxy db. May just be a name or name,pass
     * @return A loggable account name
     */

    private static String scrubPassword(String dbacct) {
        if (dbacct != null) {
            String[] parts = dbacct.split("\\,", 2);
            if (parts.length == 2) {
                return parts[0] + ",<password masked>";
            } else {
                return parts[0];
            }
        } else {
            return null;
        }
    }

    public void DisconnectProxy() {
        if (this.rcon != null) {
            this.rcon.disconnect();
            this.rcon = null;
        }
        this.DBconnected = false;
    }
}
