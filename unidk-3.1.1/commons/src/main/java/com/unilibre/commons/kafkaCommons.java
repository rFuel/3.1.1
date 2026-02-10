package com.unilibre.commons;

import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniSession;
import com.unilibre.cipher.uCipher;
import org.json.JSONObject;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Properties;

public class kafkaCommons {

    public static String delim= NamedCommon.IMark;
    public static boolean KERROR = false, stopSW = false, uConnected = false, firstSW = true, ENCR=false;
    public static String Zmessage;
    public static String pid = ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0];
    public static String BaseCamp = "/upl";
    public static String slash = "";
    public static String rawDB="";
    private static String passport, issuer, logid, timedate, account, file, itemId, record;
    public static String brokers="", topic="", dbhost="", dbpath="", dbuser="", dbacct, mTemplate="";
    public static String dbpwd="", dbSecure="", minPool="", maxPool="";
    private static ArrayList<String> reqKeys = new ArrayList<>();
    private static ArrayList<String> reqVals = new ArrayList<>();
    public static int pause=0, heartbeat=0;
    private static UniJava uJava = new UniJava();
    private static UniSession uSession = null;
    private static UniFile U2File = null;

    public static boolean isLicenced() {
        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);

        if (NamedCommon.upl.equals("")) {
            NamedCommon.upl = System.getProperty("user.dir");
            NamedCommon.BaseCamp = NamedCommon.upl;
        }
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            NamedCommon.upl = NamedCommon.BaseCamp;
            NamedCommon.slash = "/";
        }

        NamedCommon.isDocker = true;

        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties rfProps;
        rfProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) return false;
        uCommons.SetCommons(rfProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        rfProps = null;

        return License.IsValid();
    }

    public static boolean SetPayload(String line) {

        passport="";issuer="";logid="";timedate="";account="";file="";itemId="";record="";
        String eDate="", eTime="";

        if (line.startsWith("{")) {
            // ------------------------------------------------------------------
            // From rFuel +    ALL Kafka records are written in json format
            // ------------------------------------------------------------------
            reqKeys.clear();
            reqVals.clear();
            JSONObject obj = new JSONObject(line);
            Iterator<String> keys = obj.keys();
            String key, val;
            while(keys.hasNext()) {
                key = keys.next();
                val = obj.getString(key);
                reqKeys.add(key.toUpperCase());
                reqVals.add(val);
            }
            // these properties are NEVER encrypted -----------
            eTime    = GetJSONvalue(obj,"time");
            eDate    = GetJSONvalue(obj,"date");
            if (eDate.equals("") || eTime.equals("")) {
                String dts = GetJSONvalue(obj,"dts");
                eDate = dts.substring(0,7);
                eTime = dts.substring(8,99);
            }
            // ------------------------------------------------
            passport = GetJSONvalue(obj, "passport");               // MUST get this first for decryption
            issuer   = GetJSONvalue(obj,"issuer");
            logid    = GetJSONvalue(obj,"sourceinstance");
            account  = GetJSONvalue(obj,"sourceaccount");
            file     = GetJSONvalue(obj,"file");
            itemId   = GetJSONvalue(obj,"item");
            record   = GetJSONvalue(obj,"record");
            // timedate : ccyymmddhhMMss  : 20200630073015 : 07:30:15am 30-06-2020
            // Bug: it was the other way around - time:date and failed LoadDte filtering
            timedate = eDate.replaceAll("\\-", "");
            timedate+= eTime.replaceAll("\\:", "");
            obj      = null;
        } else {
            // ------------------------------------------------------------------
            // Originally, rFuel just wrote stringified data into Kafka topics.
            //      the data was delimited by "<im>".
            // ------------------------------------------------------------------
            String[] lparts = line.split(NamedCommon.IMark);
            if (lparts.length < 5) return false;

            timedate = lparts[0].replaceAll("\\.","");
            account  = lparts[1];
            file     = lparts[2];
            itemId   = lparts[3];
            record   = lparts[4];
            lparts   = null;
        }
        return true;
    }

    public static void SetENCR(boolean inval) { ENCR = inval;}

    private static String GetJSONvalue(JSONObject obj, String property) {
        String ans = "";
        if (reqKeys.indexOf(property.toUpperCase()) >= 0) {
            ans = reqVals.get(reqKeys.indexOf(property.toUpperCase()));
        } else {
            return "";
        }

        if (!passport.equals("") && property.equals("record") && ENCR) {
            ans = uCipher.v2UnScramble(uCipher.keyBoard25, ans, uCommons.FieldOf(passport, "~", 2));
        }

        return ans;
    }

    public static String GetPassport() { return passport;}

    public static String GetIssuer() { return issuer;}

    public static String GetLogid() { return logid;}

    public static String GetTimeDate() {return timedate;}

    public static String GetAccount() {return account;}

    public static String GetFile() {return file;}

    public static String GetItem() {return itemId;}

    public static String GetRecord() {return record;}

    public static Properties LoadProperties(String fname) {
        if (!fname.contains(NamedCommon.slash)) {
            fname = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash + fname;
        }

        Properties props = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(fname);
        } catch (FileNotFoundException e) {
            KERROR = true;
            Zmessage = "Cannot find " + fname;
            uSendMessage(Zmessage);
            uSendMessage(e.getMessage());
            return null;
        }
        if (is != null) {
            try {
                props.load(is);
            } catch (IOException e) {
                KERROR = true;
                Zmessage = "Cannot load " + fname;
                uSendMessage(Zmessage);
                uSendMessage(e.getMessage());
            } catch (IllegalArgumentException iae) {
                KERROR = true;
                Zmessage = iae.getMessage();
                uSendMessage(Zmessage);
                return null;
            }
            try {
                is.close();
                is = null;
            } catch (IOException e) {
                KERROR = true;
                Zmessage = "Cannot close '" + fname + "'  " + e.getMessage();
                uSendMessage(Zmessage);
                uSendMessage(e.getMessage());
                return null;
            }
        } else {
            KERROR = true;
            Zmessage = "Please load '" + fname + "'";
            uSendMessage(Zmessage);
            return null;
        }
        return props;
    }

    public static String ReadDiskRecord(String infile) {
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
                uSendMessage("read FAIL on " + infile);
                uSendMessage(e.getMessage());
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    uSendMessage("read FAIL on " + infile);
                    uSendMessage(e.getMessage());
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                uSendMessage("File Close FAIL on " + infile);
                uSendMessage(e.getMessage());
            }
        } catch (IOException e) {
            rec = "";
            // Read and processed by another uStream process
        }
        return rec;
    }

    public static void uSendMessage(String msg) {
        int ThisMS;
        int nDeep = Thread.currentThread().getStackTrace().length;
        String caller = Thread.currentThread().getStackTrace()[nDeep-1].getClassName();
        //
        String chk, mTime, mDate, MSec;
        chk = msg.trim();
        if (chk.length() > 0) {
            mTime = "";
            mDate = "";
            MSec  = "";
            if (NamedCommon.ShowDateTime) {
                mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
                if (NamedCommon.ZERROR) {
                    mDate = "**** ERROR";
                } else {
                    if (NamedCommon.showPID) {
                        mDate = uCommons.LeftHash(NamedCommon.pid, 10);
                    } else {
                        ThisMS = Calendar.getInstance().get(Calendar.MILLISECOND);
                        MSec = "." + (ThisMS + "000").substring(0, 3);
                        mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
                    }
                }
            }
            if (!NamedCommon.jLogs) {
                System.out.println(mDate + " " + mTime + MSec + " " + msg);
            } else {
                JSONObject jMsg = new JSONObject();
                jMsg.append("Date",mDate);
                jMsg.append("Time", mTime+"."+MSec);
                jMsg.append("Caller",caller);
                jMsg.append("Event",msg);
                msg = jMsg.toString();
                jMsg = null;
                System.out.println(msg);
            }
            mTime = "";
            mDate = "";
            MSec = "";
            msg="";
        }

    }

    public static void Sleep (int mSecs) {
        try {
            Thread.sleep(mSecs);
        } catch (InterruptedException e) {
            uSendMessage("Thread Sleep error");
            uSendMessage(e.getMessage());
        }

    }

    public static String RightHash (String value, int len) {
        while (value.length() < len) {
            value = "0000000"+value;
        }
        int lx = value.length()-len;
        return value.substring(lx,value.length());
    }

    public static String LeftHash (String value, int len) {
        while (value.length() < len) {
            value += "       ";
        }
        return value.substring(0,len);
    }

}
