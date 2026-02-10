package com.unilibre.kafka.redundant;
/* * Copyright UniLibre on 2015. ALL RIGHTS RESERVED  **/

// uDataLoader takes packets out of a Kafka stream and loads them into UniVerse
//      * it acts an an HADR service
//      * it can be used to migrate from one DB to UniVerse
//      * it is UniVerse focused as the target DB.

import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.kafkaCommons;
import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class uDataLoader {

    private static Properties props;
    private static String brokers;
    private static String topic;
    private static UniJava uJava = new UniJava();
    private static UniSession uSession = null;
    private static UniFile U2File = null;
    private static String dbhost;
    private static String dbpath;
    private static String dbacct;
    private static String dbuser;
    private static String dbpwd;
    private static String minPool;
    private static String maxPool;
    private static String dbSecure;
    private static String IMark = "<im>";
    private static String FMark = "<fm>";
    private static String VMark = "<vm>";
    private static String SMark = "<sm>";
    private static String dot = ".";
    public static int pause;
    public static int heartbeat;
    public static boolean uConnected = false;
    private static boolean stopSW = false;
    private static boolean firstSW = true;

    public static void main(String[] args) {
        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            NamedCommon.slash = "\\";
        }

        String cFile = System.getProperty("conf", "uKafka.properties");
        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage("uDataLoader(): Will pull database deltas FROM Kafka streams");
        uCommons.uSendMessage("        Using: " + cFile);
        uCommons.uSendMessage("===============================================================");

        int lCnt=0, fCnt=0;
        kConsumer.updates = new ArrayList<String>();
        SetValues(cFile);
        if (kafkaCommons.KERROR) {
            stopSW = true;
        } else {
            uCommons.uSendMessage("   Using Kafka broker(s): [" + brokers + "]");
            uCommons.uSendMessage("   Using Kafka topic    : [" + topic + "]");
            uCommons.uSendMessage("===============================================================");
            uCommons.uSendMessage(" ");
        }

        while (!stopSW) {
            try {
                kConsumer.kConsume();
                if (kConsumer.didNothing) {
                    lCnt++;
                    if (lCnt > heartbeat) {
                        kafkaCommons.Sleep(pause);
                        DisconnectUV();
                        SetValues(cFile);
                        uCommons.uSendMessage("uDataLoader   <<Heartbeat>>");
                        lCnt = 0;
                    }
                } else {
                    lCnt = 0;
                    ProcessUpdates(kConsumer.updates);
                    kConsumer.updates.clear();
                }
            } catch (IllegalStateException ise) {
                fCnt++;
                uCommons.uSendMessage(fCnt+"  Cannot connect to Kafka broker(s). "+ise.getMessage());
                kafkaCommons.Sleep(5000);
                if (fCnt >= 10) {
                    uCommons.uSendMessage("Seems Kafka brokers are unavailable. Stopping now.");
                    stopSW = true;
                }
            }
        }
        kConsumer.Close();
        if (uConnected) DisconnectUV();
        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage("Master stop switch turned ON. Stopping now.");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage(" ");
    }

    private static void ProcessUpdates(ArrayList<String> updates) {
        int nbrUpds = updates.size(), uNext = 0;
        String line, uExtn = "";

        String passport, issuer, logid, dts, timedate, account, file, itemId, record, base;

        UniString uID, existingRec;
        boolean writeOK = false;
        for (int u = 0; u < nbrUpds; u++) {
            line = updates.get(u);
            line = line.replaceAll("\\<\\|\\>", IMark);
            kafkaCommons.SetPayload(line);
            passport = kafkaCommons.GetPassport();
            issuer   = kafkaCommons.GetIssuer();
            timedate = kafkaCommons.GetTimeDate();
            logid    = kafkaCommons.GetLogid();
            account  = kafkaCommons.GetAccount();
            file     = kafkaCommons.GetFile();
            itemId   = kafkaCommons.GetItem();
            record   = kafkaCommons.GetRecord();
//            if (line.startsWith("{")) {
//                JSONObject obj = new JSONObject(line);
//                passport = obj.getString("Passport");
//                issuer   = obj.getString("Issuer");
//                timedate = obj.getString("LogID");
//                logid    = obj.getString("DTS");
//                account  = obj.getString("Account");
//                file     = obj.getString("File");
//                itemId   = obj.getString("Item");
//                record   = obj.getString("Record");
//                obj      = null;
//                if (record.length() - record.replace("~","").length() == 2) {
//                    record = uCipher.Decrypt(record);
//                }
//            } else {
//                String[] lparts = new String[10];
//                for (int l = 0; l < 10; l++) { lparts[l] = ""; }
//                lparts   = uStrings.gSplit2Array(line, IMark);
//                timedate = lparts[0].replaceAll("\\.","");
//                account  = lparts[1];
//                file     = lparts[2];
//                itemId   = lparts[3];
//                record   = lparts[4];
//                lparts   = null;
//            }
            record   = account + kafkaCommons.delim + file + kafkaCommons.delim + itemId + kafkaCommons.delim + record;
            uCommons.uSendMessage(timedate + kafkaCommons.delim + account + kafkaCommons.delim + file + kafkaCommons.delim + itemId);

            uExtn = "";
            base = timedate + dot + account + dot + file + dot + itemId;
            while (!writeOK) {
                try {
                    U2File.setRecordID(base + uExtn);
                    existingRec = U2File.read();
                    uNext++;
                    uExtn = "." + String.valueOf(uNext);
                } catch (UniFileException ufe) {
                    U2File.setRecord(record);
                    try {
                        U2File.write();
                        writeOK = true;
                        uNext = 0;
                    } catch (UniFileException ufe2) {
                        uCommons.uSendMessage("ERROR: Cannot write "+timedate+uExtn+" to "+U2File.getFileName());
                        kafkaCommons.KERROR = true;
                    }
                }
            }
        }
    }

    public static void SetValues(String cfile) {
        props = kafkaCommons.LoadProperties(cfile);
        if (kafkaCommons.KERROR) System.exit(0);

        brokers = props.getProperty("brokers", "");
        topic   = props.getProperty("topic", "UniLibre");
        dbhost  = props.getProperty("u2host", "NotInConfigFile");
        dbpath  = props.getProperty("u2path", "NotInConfigFile");
        dbuser  = props.getProperty("u2user", "NotInConfigFile");
        dbpwd   = props.getProperty("u2pass", "NotInConfigFile");
        dbSecure  = props.getProperty("secure", "NotInConfigFile");
        minPool  = props.getProperty("minpoolsize", "NotInConfigFile");
        maxPool  = props.getProperty("maxpoolsize", "NotInConfigFile");

        if (brokers.equals("")) {
            uCommons.uSendMessage("No Kafka broker(s) found.");
            System.exit(0);
        }
        if (topic.equals("")) {
            uCommons.uSendMessage("No Kafka topic found.");
            System.exit(0);
        }
        if ((dbhost+dbpath+dbuser+dbpwd+dbSecure+minPool+maxPool).contains("NotInConfigFile")) {
            uCommons.uSendMessage("UV database connection parameters are invalid.");
            System.exit(0);
        }

        try {
            pause = Integer.valueOf(props.getProperty("pause", "250"));
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("Pause value must be an integer > 100.");
            System.exit(0);
        }

        try {
            heartbeat = Integer.valueOf(props.getProperty("heartbeat", "100"));
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("HeartBeat value must be an integer > 100.");
            System.exit(0);
        }

        stopSW = (props.getProperty("stop", "").toLowerCase().equals("true"));

        props.clear();

        kConsumer.SetBroker(brokers);
        kConsumer.SetTopic(topic);
        kConsumer.SetClientID("Client:Loader:"+kafkaCommons.pid);
        kConsumer.SetGroup("Group:for: Client:Loader:"+kafkaCommons.pid);
        kConsumer.pause = pause;
        kConsumer.MAX_NO_MESSAGE_FOUND_COUNT=heartbeat;

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
            uCommons.uSendMessage(kafkaCommons.Zmessage);
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
                uCommons.uSendMessage("   Using UniVerse host  : [" + dbhost + "]");
                uCommons.uSendMessage("   Using UniVerse path  : [" + dbpath + "]");
            }
        } catch (UniSessionException e) {
            kafkaCommons.KERROR = true;
            uCommons.uSendMessage("Cannot establish UV session: "+ dbhost + " " + dbpath);
            uCommons.uSendMessage(e.getMessage());
            return;
        }
        try {
            U2File = uSession.openFile("INBOUND.DELTAS");
            if (firstSW) uCommons.uSendMessage("   Using UniVerse file  : [INBOUND.DELTAS]");
        } catch (UniSessionException e) {
            kafkaCommons.KERROR = true;
            uCommons.uSendMessage("Cannot open INBOUND.DELTAS: "+e.getMessage());
            return;
        }
        firstSW = false;
        uConnected = true;
    }

    public static void DisconnectUV() {
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
        }
        uConnected = false;
    }

}
