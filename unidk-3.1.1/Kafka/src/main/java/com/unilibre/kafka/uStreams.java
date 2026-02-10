package com.unilibre.kafka;

/*     **********************************************       */
/*     SELECT uDELTA.LOG and send to MQ Broker vTopic       */
/*     This can bypass the use of kafka                     */
/*     **********************************************       */

import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;
import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.artemisMQ;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.kafkaCommons;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.unilibre.commons.uCommons.uSendMessage;

public class uStreams {

    public static Runtime Garbo = Runtime.getRuntime();
    private static boolean stopSW = false;
    private static boolean uConnected = false;
    private static boolean debug = false;
    private static boolean firstSW = true;
    private static String underline = "---------------------------------------------------------------";
    private static ArrayList<String> badRecs = new ArrayList();
    private static long lastAction = 0;
    private static long rightNow = 0;
    private static double laps = 0;
    private static long mStart = 0;
    private static long mFinish = 0;
    private static double mTime = 0;
    private static int heartbeat = 45, nbrQues = 1;
    private static double div = 1000000000.00;
    private static Properties props;
    private static Properties runProps;
    private static String MQtype, bkrUrl, bkr_user, bkr_pword, toQue, nQues;
    private static String dbhost;
    private static String dbpath;
    private static String dbuser;
    private static String dbpwd;
    private static String dbSecure;
    private static String minPool;
    private static String maxPool;
    private static String logFile = "uDELTA.LOG";
    private static String mTemplate="";
    private static UniJava uJava = new UniJava();
    private static UniSession uSession = null;
    private static UniFile U2File = null;
    private static UniCommand ucmd;
    private static UniFile BPUPL;

    private static void Process(String cfile) {

        UniString rID = null, rRec = null;
//        UniDynArray dRec = null;
        String datAct, fileName, itemID, sRec;
        String cmd = "SELECT "+logFile, sID;

        ucmd.setCommand(cmd);

        String message;
        UniString uStop = new UniString("STOP");
        uSendMessage("Looking for deltas ....");
        CheckStop(BPUPL, uStop);
        String qName;
        int qNbr=0;

        while (!stopSW) {
            try {
                ucmd.exec();
                boolean eof = false;
                while (!eof) {
                    try {
                        rID = uSession.selectList(0).next();
                        sID = String.valueOf(rID);
                        if (uSession.selectList(0).isLastRecordRead() || sID.isEmpty()) {
                            rID = null;
                            sID = "";
                            eof = true;
                            continue;
                        } else {
                            if (badRecs.indexOf(sID) > -1) continue;
                            try {
                                U2File.setRecordID(rID);
                                U2File.lockRecord(rID, 1);
                                rRec = U2File.read();
                                U2File.unlockRecord(rID);
                                // -----------------------------------------------------------------------------
                                // What happens if the record has been deleted by another uStream process ?
                                // The .read() will fail and rRec will be null
                                // -----------------------------------------------------------------------------
                                sRec = rRec.toString();
                                if (rRec == null) sRec = "";
                                if (sRec.equals("")) continue;

                                // .1 Build rFuel 022 Message
                                List<String> tmpList = new ArrayList<>(Arrays.asList(sID.split("<im>")));
                                if (tmpList.size() < 3) { tmpList = null; continue; }
                                datAct      = tmpList.get(0);
                                fileName    = tmpList.get(1);
                                itemID      = tmpList.get(2);
                                tmpList     = null;

                                if (!sRec.startsWith(sID)) sRec = sID + "<im>" + sRec;

                                message     = mTemplate.replace("$map$", fileName);
                                message     = message.replace("$dacct$", datAct);
                                message     = message.replace("$db", "$DB");
                                message     = message.replace("$sc", "$SC");
                                message     = message.replace("$record$", sRec);

                                // .2 Send to rFuel
                                mStart = System.nanoTime();
                                qNbr++;
                                if (qNbr > nbrQues) qNbr=1;
                                qName = toQue+"_"+(RightHash("000"+qNbr, 3));
                                try {
                                    switch (MQtype) {
                                        case "artemis":
                                            artemisMQ.produce(bkrUrl, bkr_user, bkr_pword, "", qName, message);
                                            break;
                                        case "activeMQ":
                                            com.unilibre.MQConnector.commons.inputQueue = NamedCommon.inputQueue;
                                            activeMQ.SetDeliveryMode(2);
                                            activeMQ.produce(bkrUrl, bkr_user, bkr_pword, "", qName, message);
                                            com.unilibre.MQConnector.commons.inputQueue = "";
                                            break;
                                    }
                                } catch (JMSException e) {
                                    U2File.unlockRecord(rID);
                                    uSendMessage(underline);
                                    uSendMessage("uStreams.Process :ERROR: "+e.getMessage());
                                    uSendMessage("It is likely that the MQ broker url is incorrect in "+cfile);
                                    uSendMessage(underline);
                                    System.exit(0);
                                }
                                U2File.deleteRecord(rID);
                                U2File.unlockRecord(rID);
                                mFinish = System.nanoTime();
                                mTime = (mFinish - mStart) / div;
                                uSendMessage(LeftHash(String.valueOf(mTime), 5) + " second to process [" + sID + "] sent to "+qName);
                                lastAction = System.nanoTime();
                            } catch (UniFileException e) {
                                continue;
                            }
                        }
                    } catch (UniSelectListException sle) {
                        uSendMessage("Select list error "+sle.getMessage());
                        stopSW = true;
                        continue;
                    } catch (UniSessionException use) {
                        if (use.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                        uSendMessage("Session error "+use.getMessage());
                        stopSW = true;
                        continue;
                    }
                }
                try {
                    uSession.selectList(0).clearList();
                } catch (UniSelectListException e) {
                    // ignore this error ??
                    uSendMessage("**********************************************************");
                    uSendMessage(e.getMessage());
                    uSendMessage("**********************************************************");
                } catch (UniSessionException e) {
                    uSendMessage("**********************************************************");
                    uSendMessage(e.getMessage());
                    uSendMessage("**********************************************************");
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                }
                try {
                    rightNow = System.nanoTime();
                    laps = (rightNow - lastAction) / div;
                    if (laps >= heartbeat) {
                        uSendMessage("<<heartbeat>> uStreams()");
                        DisconnectUV();
                        SetValues(cfile);
                        if (!uConnected) stopSW = true;
                        ucmd = null;
                        ucmd = uSession.command();
                        ucmd.setCommand(cmd);
                        lastAction = rightNow;
                        Garbo.gc();
                    }
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    uSendMessage("Thread.sleep() ERROR: " + e.getMessage());
                    System.exit(0);
                } catch (UniSessionException e) {
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    uSendMessage("<<heartbeat>> Reconnect failed to create a UniCommand "+e.getMessage());
                    System.exit(0);
                }
            } catch (UniCommandException e) {
                uSendMessage("Cannot execute " + cmd);
                stopSW = true;
                continue;
            }
            CheckStop(BPUPL, uStop);
        }

        uSendMessage(" ");
        uSendMessage("STOP switch is ON - process stopping now.");
        uSendMessage("===================================================================");
        for (int x=0; x< 5; x++) uSendMessage(" ");
        System.exit(0);
    }

    private static String RightHash (String value, int len) {
        while (value.length() < len) {
            value = "0000000"+value;
        }
        int lx = value.length()-len;
        return value.substring(lx,value.length());
    }

    private static String LeftHash (String value, int len) {
        while (value.length() < len) {
            value += "       ";
        }
        return value.substring(0,len);
    }

    private static void CheckStop(UniFile bpupl, UniString uStop) {
        String stopRec;
        try {
            BPUPL.setRecordID(uStop);
            stopRec = BPUPL.read().toString();
        } catch (UniFileException e) {
            stopRec = "";
        }
        if (stopRec.toLowerCase().contains("stop")) stopSW = true;
    }

    private static void SetValues(String cfile) {
        props = kafkaCommons.LoadProperties(cfile);
        if (kafkaCommons.KERROR) System.exit(0);

        dbhost = props.getProperty("u2host", "NotInConfigFile");
        dbpath = props.getProperty("u2path", "NotInConfigFile");
        dbuser = props.getProperty("u2user", "NotInConfigFile");
        dbpwd = props.getProperty("u2pass", "NotInConfigFile");
        dbSecure = props.getProperty("secure", "NotInConfigFile");
        minPool = props.getProperty("minpoolsize", "NotInConfigFile");
        maxPool = props.getProperty("maxpoolsize", "NotInConfigFile");
        String bkr = props.getProperty("bkr", "streams.bkr");
        mTemplate = props.getProperty("message", "");

        if (mTemplate.equals("")) {
            uSendMessage("rFuel Message template is missing or invalid.");
            System.exit(0);
        }

        if ((dbhost + dbpath + dbuser + dbpwd + dbSecure + minPool + maxPool).contains("NotInConfigFile")) {
            uSendMessage("UV database connection parameters are invalid.");
            System.exit(0);
        }

        try {
            heartbeat = Integer.valueOf(props.getProperty("heartbeat", "100"));
        } catch (NumberFormatException nfe) {
            uSendMessage("HeartBeat value must be an integer > 100.");
            System.exit(0);
        }

        stopSW = (props.getProperty("stop", "").toLowerCase().equals("true"));
        if (stopSW) return;

        bkr = NamedCommon.DevCentre + "\\conf\\streams.bkr";

        props = kafkaCommons.LoadProperties(bkr);
        if (kafkaCommons.KERROR) System.exit(0);
        // currently, all are CLEAR TEXT - ENC() later
        bkrUrl      = props.getProperty("url", "localhost:61616");
        toQue       = props.getProperty("qname", "022_Streams");
        bkr_user    = props.getProperty("bkruser", "");
        bkr_pword   = props.getProperty("bkrpword", "");
        MQtype      = props.getProperty("type", "artemis");
        nQues     = props.getProperty("responders", "1");
        props.clear();

        if (toQue.equals("")) {
            uSendMessage("Broker: "+bkr+" is not set up correctly (qname).");
            System.exit(0);
        }

        try {
            nbrQues = Integer.valueOf(nQues);
        } catch (NumberFormatException e) {
            uSendMessage("Broker: "+bkr+" is not set up correctly (responders). Defaulting to 1 responder");
            nbrQues = 1;
        }

        if (!uConnected) ConnectUV();
    }

    private static void ConnectUV() {

        int minSize = 1;
        int maxSize = 1;
        try {
            minSize = Integer.valueOf(minPool);
            maxSize = Integer.valueOf(maxPool);
        } catch (NumberFormatException nfe) {
            kafkaCommons.KERROR = true;
            kafkaCommons.Zmessage = "UV connection pool sizes MUST be integers - please fix.";
            uSendMessage(kafkaCommons.Zmessage);
            return;
        }

        int contype = 0;
        if (dbSecure.toLowerCase().equals("true")) contype = UniObjectsTokens.SECURE_SESSION;

        try {
            uJava.setUOPooling(true);
            uSession = uJava.openSession(contype);
            uJava.setMinPoolSize(minSize);
            uJava.setMaxPoolSize(maxSize);
            uSession = uJava.openSession();
            uSession.setHostName(dbhost);
            uSession.setUserName(dbuser);
            uSession.setPassword(dbpwd);
            uSession.setAccountPath(dbpath);
            uSession.setConnectionString("uvcs");
            uSession.connect();
            if (firstSW) {
                uSendMessage("   Using UniVerse host  : [" + dbhost + "]");
                uSendMessage("   Using UniVerse path  : [" + dbpath + "]");
            }
        } catch (UniSessionException e) {
            kafkaCommons.KERROR = true;
            uSendMessage("Cannot establish UV session: " + dbhost + " " + dbpath);
            uSendMessage(e.getMessage());
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            return;
        }
        try {
            U2File = uSession.openFile(logFile);
            if (firstSW) uSendMessage("   Using UniVerse file  : ["+logFile+"]");
        } catch (UniSessionException e) {
            kafkaCommons.KERROR = true;
            uSendMessage("Cannot open "+logFile+": " + e.getMessage());
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            return;
        }
        try {
            BPUPL = uSession.openFile("BP.UPL");
        } catch (UniSessionException e) {
            kafkaCommons.KERROR = true;
            uSendMessage("Cannot open BP.UPL: " + e.getMessage());
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            return;
        }
        try {
            ucmd = uSession.command();
        } catch (UniSessionException e) {
            uSendMessage("Cannot set uCmd " + e.getMessage());
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            System.exit(0);
        }
        if (firstSW) firstSW = false;

        uConnected = true;
    }

    private static void DisconnectUV() {
        try {
            if (U2File != null) {
                U2File.close();
                U2File = null;
            }
            if (uSession != null) {
                uSession.disconnect();
                uSession = null;
            }
        } catch (UniFileException ufe) {
            U2File = null;
        } catch (UniSessionException e) {
            uSession = null;
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
        }
        uConnected = false;
    }

    public static void main(String[] args) {
        // ------- [DEV / PROD housekeeping -------------------------------------------
        if (NamedCommon.upl.equals("")) NamedCommon.upl = System.getProperty("user.dir");
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        if (NamedCommon.upl.contains("/home/andy")) {
            String rfHost = "rfuel22" + NamedCommon.slash + "all";
            NamedCommon.BaseCamp = NamedCommon.slash + NamedCommon.slash + rfHost + NamedCommon.slash + "upl";
            NamedCommon.gmods = NamedCommon.BaseCamp + NamedCommon.slash + "lib" + NamedCommon.slash;
        }

        for (int x=0; x< 5; x++) uSendMessage("*");
        uSendMessage("uStreams() -------------------------------------");
        debug = (System.getProperty("debug", "false").toLowerCase().equals("true"));
        String cFile = System.getProperty("conf", "uKafka.properties");
        if (cFile.equals("")) System.exit(0);
        SetValues(cFile);
        uSendMessage("===================================================================");
        uSendMessage(" ");
        if (uConnected) Process(cFile);
    }

}
