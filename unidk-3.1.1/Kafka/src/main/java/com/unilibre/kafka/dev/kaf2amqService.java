package com.unilibre.kafka.dev;

/* ***************************************************************************************
    In development - will eventually replace kStream.
   *************************************************************************************** */

import com.unilibre.MQConnector.activeMQ;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.kafka.kafka2amq;
import com.unilibre.kafka.kafkaServices;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.*;

public class kaf2amqService {

    // General variables
    private static String main, cfile;
    private static ArrayList<String> updates, reqKeys, reqVals;
    private static int numThreads;
    private static Thread[] threads = null;
    // AMQ variables
    private static String mqUrl, mqUsr, mqPwd, mqTopic, combo;
    // Kafka variables
    public static final Integer MAX_POLL_RECORDS = 10;
    private static String keySerdes, valSerdes, acks, retries, kGroup, brokers, topic, mTemplate, runtype;
    private static String ClientID;
    private static Consumer consumer;

    private static String[] tcols= NamedCommon.rawCols.split("\\,");
    private static boolean firstSW = true, silent = false, encrypt = false, stopSW, mqvTopic = false;
    private static boolean isConnectedMQ=false;
    private static int expiry, pause, heartbeat, qLoad=0, minQn, maxQn, lastQ, numSent;

    // =============================================================================================

    private static void Process() {
        stopSW = false;
        GetConsumer();
        ArrayList<String> messages;
        while (!stopSW) {
            messages = new ArrayList<>(GetMessages());
            ThreadHandler(messages);
        }
        CloseConsumer();
    }

    private static ArrayList GetMessages() {
        updates.clear();
        ConsumerRecords<String, String> records = consumer.poll(MAX_POLL_RECORDS);
        String key, value;
        for (ConsumerRecord<String, String> record : records) {
            key   = record.key().toString();
            value = record.value().toString();
            updates.add(value);
        }
        key=null;
        value=null;
        if (updates.size() > 0) {
            consumer.commitSync();
            consumer.commitAsync();
        }
        return updates;
    }

    private static void ThreadHandler(ArrayList<String> messages) {
        double laps, div = 1000000000.00;
        long start, finish;
        int eom = messages.size();
        threads = new Thread[numThreads];
        kafka2amq[] k2at = new kafka2amq[numThreads];
        start = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread((Runnable) k2at[i]);
            threads[i] = t;
            threads[i].start();
            t = null;
        }
        SendMessage("... wait for threads. ------------------------------------");
        boolean areAlive=true;
        while (areAlive) {
            areAlive = false;
            for (int x = 0; x < numThreads; x++) {
                if (threads[x].isAlive()) areAlive = true;
            }
        }
        finish = System.currentTimeMillis();
        laps = (finish - start) / div;
        SendMessage("All threads finished in " + laps + " seconds ");
        SendMessage("Events per seconds " + (Double) (eom / laps));
        SendMessage(" ----------------------------------------------------------------------------");


        String line, eTime, eDate, passport, issuer, logid, account, file, itemId, record, payload, message, datetime;
        boolean proceed = false;
        reqKeys = new ArrayList<>();
        reqVals = new ArrayList<>();

        for (int m=0 ; m < eom ; m++) {
            reqKeys.clear();
            reqVals.clear();
            line = messages.get(m);
            JSONObject obj;
            try {
                obj = new JSONObject(line);
            } catch (JSONException je) {
                SendMessage("ERROR: " + je.getMessage());
                continue;
            }
            Iterator<String> keys = obj.keys();
            String key, val;
            while(keys.hasNext()) {
                key = keys.next();
                val = obj.getString(key);
                reqKeys.add(key.toUpperCase());
                reqVals.add(val);
            }
            // these properties are NEVER encrypted -----------
            passport = "";
            eTime    = GetJSONvalue(obj,"time", passport);
            eDate    = GetJSONvalue(obj,"date", passport);
            if (eDate.equals("") || eTime.equals("")) {
                String dts = GetJSONvalue(obj,"dts", passport);
                eDate = dts.substring(0,7);
                eTime = dts.substring(8,99);
            }
            // ------------------------------------------------
            passport = GetJSONvalue(obj, "passport", passport);
            issuer   = GetJSONvalue(obj,"issuer", passport);
            datetime = GetJSONvalue(obj,"datetime", passport);
            logid    = GetJSONvalue(obj,"sourceinstance", passport);
            account  = GetJSONvalue(obj,"sourceaccount", passport);
            file     = GetJSONvalue(obj,"file", passport);
            itemId   = GetJSONvalue(obj,"item", passport);
            record   = GetJSONvalue(obj,"record", passport);
            obj      = null;

            payload  = account + kafkaCommons.delim + file + kafkaCommons.delim + itemId + kafkaCommons.delim + record;
            NamedCommon.MessageID = logid + kafkaCommons.delim + account + kafkaCommons.delim + file + kafkaCommons.delim + itemId;
            NamedCommon.CorrelationID = "Load--" + logid + "--" + account + "--" + file + "--" + itemId;

            message = mTemplate;
            message = Resolve(message, "$map$", file);
            message = Resolve(message, "$passport$", passport);
            message = Resolve(message, "$issuer$", issuer);
            message = Resolve(message, "$item$", itemId);
            message = Resolve(message, "$dacct$", account);
            message = Resolve(message, "$loadts$", datetime);
            message = Resolve(message, "$logid$", logid);
            message = Resolve(message, "$record$", payload);

            proceed = SendToMQ(message);
            while (!proceed) {
                uCommons.Sleep(5);
                proceed = SendToMQ(message);
            }

        }
    }

    private static boolean SendToMQ(String message) {
        boolean mqPassed = false;
        String eMsg="";
//        while (!mqPassed) {
//            if (NamedCommon.artemis) {
//                eMsg = "ArtemisMQ Producer ERROR: ";
//                try {
//                    mqPassed = artemisMQ.produce(mqUrl, mqUsr, mqPwd, mqCli, mqTopic, message);
//                } catch (JMSException e) {
//                    mqPassed = false;
//                    eMsg += e.getMessage();
//                }
//            } else {
//                eMsg = "ActiveMQ Producer ERROR: ";
//                // -------------------------------------------------------------------------------
//                // what if I want to send the message to N queues all at once?
//                // must set proceed<is>false or rFuel will forward to next queue
//                //     : maintain String[]mqTopics --- dam good idea !
//                //     : maintain String[]mqUrls  ---- NO, keep everything on one BrokerURL
//                // -------------------------------------------------------------------------------
//                if (!mqvTopic) {
//                    theQ = mqTopic + GetNextQue();
//                } else {
//                    theQ = mqTopic;
//                }
//                if (!isConnectedMQ) {
//                    CheckMQconnection();
//                    isConnectedMQ = true;
//                }
//                mqPassed = activeMQ.produce(mqUrl, mqUsr, mqPwd, mqCli, theQ, message);
//            }
//            if (!mqPassed) {
//                uCommons.uSendMessage(eMsg);
//                uCommons.uSendMessage("Trying to reclaim a connection:.");
//                CheckMQconnection();
//            }
//        }
        return mqPassed;
    }

    private static String GetJSONvalue(JSONObject obj, String property, String passport) {
        String ans = "";
        if (reqKeys.indexOf(property.toUpperCase()) >= 0) {
            ans = reqVals.get(reqKeys.indexOf(property.toUpperCase()));
        } else {
            return "";
        }

        if (!passport.equals("") && property.equals("record")) {
            ans = uCipher.v2UnScramble(uCipher.keyBoard25, ans, uCommons.FieldOf(passport, "~", 2));
        }

        return ans;
    }

    private static String Resolve(String inmsg, String repl, String invar) {
        while (inmsg.contains(repl)) {
            inmsg = inmsg.replace(repl, invar);
        }
        return inmsg;
    }

    private static void CheckKafka() {
        kafkaServices kService = GetConsumer();
        Consumer testCons = kService.createConsumer(combo, main);
        if (testCons == null) {
            SendMessage("Kafka error - stopping now");
            System.exit(1);
        }
        kService.CloseConsumer(testCons);
        testCons = null;
        kService = null;
    }

    private static kafkaServices GetConsumer() {
        kafkaServices ks = new kafkaServices();
        ks.SetAcks(acks);
        ks.SetBatching(false);
        ks.SetBroker(brokers);
        ks.SetGroup(kGroup);
        ks.SetClientID(ClientID);
        ks.SetSASL(false, "", "", "");
        ks.SetSerdes(keySerdes, valSerdes);
        ks.SetTopic(topic);
        ks.SetVerbose(false);
        ks.Configure("consumer");
        consumer = ks.createConsumer(combo, main);
        return ks;
    }

    private static void CloseConsumer() {
        consumer.commitSync();
        consumer.close();
        consumer = null;
    }

    private static void CheckMQconnection() {
        if (!silent) SendMessage("Check MQconnection() ******************************************");

        activeMQ.SetExpiry(expiry);
        activeMQ.SetDocker(true);
        activeMQ.SetHost("");
        activeMQ.CloseProducer();
        int failedTries=0;
        while (true) {
            if (activeMQ.rfProducer(mqUrl, mqUsr, mqPwd, "kStream", NamedCommon.testQ) != null) {
                if (!silent) uCommons.uSendMessage("Passed ********************************************************");
                isConnectedMQ = true;
                break;
            } else {
                failedTries++;
                SendMessage("... fail " + failedTries + " of 60");
                SendMessage("Waiting for an MQ connection. Sleep 5 seconds.");
                SendMessage("...");
                SendMessage("...");
                uCommons.Sleep(5);
                if (failedTries > 60) {
                    SendMessage("Giving up.");
                    System.exit(1);
                }
            }
        }
    }

    public static String GetValue(Properties inprops, String key, String def) {
        String value = inprops.getProperty(key, def);
        if (value.equals(null) || value.equals("")) {
            value = def;
        } else {
            if (value.startsWith("ENC(")) {
                String tmp = value.substring(4, (value.length() - 1));
                value = uCipher.Decrypt(tmp);
            }
        }
        return value;
    }

    private static void GetSerDes(Properties props) {
        // serdes and serialiser do the "same"
        // they act as the default for both key and value
        String serdes   = props.getProperty("serdes", "");
        String serial   = props.getProperty("serialiser", "");
        //
        String kserial  = props.getProperty("kserialiser", "");
        String vserial  = props.getProperty("vserialiser", "");
        //
        // set as default
        //
        keySerdes = "STRING";
        valSerdes = keySerdes;
        //
        if (serdes.toUpperCase().equals("BYTE")) {
            keySerdes = serdes.toUpperCase();
            valSerdes = keySerdes;
        }
        if (serial.toUpperCase().equals("BYTE")) {
            keySerdes = serial.toUpperCase();
            valSerdes = keySerdes;
        }
        if (kserial.toUpperCase().equals("BYTE")) {
            keySerdes = kserial.toUpperCase();
        }
        if (vserial.toUpperCase().equals("BYTE")) {
            valSerdes = vserial.toUpperCase();
        }
        combo = keySerdes + "-" + valSerdes;
    }

    private static void SendMessage(String msg) {
        System.out.println(new Date() + " " + main + msg);
    }

    public static void main(String[] args) {
        main = " MAIN   ";
        SendMessage("Starting kStream() --------------------------------------------------");
        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
        }
        Properties rfProps;
        rfProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) {
            System.out.println("... stopping after error.");
            return;
        }
        uCommons.SetCommons(rfProps);
        rfProps = null;
        String sProcs = uCommons.ReadDiskRecord("/proc/1/cgroup", true);
        if (!NamedCommon.isDocker) NamedCommon.isDocker = (sProcs.contains("/docker/"));
        if (!NamedCommon.isDocker) NamedCommon.isDocker = (sProcs.contains("/kubepods"));
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";

        if (!License.IsValid()) {
            System.out.println("... licence violation.");
            System.exit(1);
        }

        cfile = System.getProperty("conf", "ERROR.properties");
        if (firstSW && !silent) {
            System.out.println(" ");
            uCommons.uSendMessage("-----------------------------------------------------------------------");
            uCommons.uSendMessage("Setup for " + cfile);
        }

        Properties props = uCommons.LoadProperties(cfile);
        if (NamedCommon.ZERROR) {
            System.exit(0);
        }
        NamedCommon.databaseType = GetValue(props, "dbtype", "");
        NamedCommon.protocol = GetValue(props, "protocol", "");
        switch (NamedCommon.protocol) {
            case "u2cs":
                NamedCommon.dbhost = GetValue(props, "u2host", "");
                if (NamedCommon.dbhost.equals("")) {
                    uCommons.uSendMessage("FATAL: No Source data directory specified.");
                    System.exit(0);
                }
                NamedCommon.dbPort = GetValue(props, "u2port", NamedCommon.dbPort);
                NamedCommon.dbpath = GetValue(props, "u2path", "");
                NamedCommon.dbuser = GetValue(props, "u2user", "");
                NamedCommon.passwd = GetValue(props, "u2pass", "");
                NamedCommon.datAct = GetValue(props, "u2acct", "");
                NamedCommon.minPoolSize = GetValue(props, "minpoolsize", "");
                NamedCommon.maxPoolSize = GetValue(props, "maxpoolsize", "");
                NamedCommon.CPL = GetValue(props, "cpl", "").toLowerCase().equals("true");
                NamedCommon.uSecure = GetValue(props, "secure", "").toLowerCase().equals("true");
                break;
            case "real":
                NamedCommon.realhost = GetValue(props, "realhost", NamedCommon.realhost);
                NamedCommon.realuser = GetValue(props, "realuser", NamedCommon.realuser);
                NamedCommon.realdb = GetValue(props, "realdb", NamedCommon.realdb);
                NamedCommon.realac = GetValue(props, "realac", NamedCommon.realac);
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
                return;
            default:
                if (firstSW) uCommons.uSendMessage("INFO: No source database definition in " + cfile);
        }

        encrypt = GetValue(props, "encrypt", "").toLowerCase().equals("true");

        GetSerDes(props);
        String mqBkr;
        runtype = GetValue(props, "runtype", "uplift");
        brokers = GetValue(props, "brokers", "");
        acks    = GetValue(props, "acks", "all");
        retries = GetValue(props, "retries", "");
        topic = GetValue(props, runtype, "uplift");
        mTemplate = GetValue(props, "message", "");
        mqBkr = GetValue(props, "bkr", "");
        kGroup = GetValue(props, "group", kGroup);
        ClientID = "k2amq-" + UUID.randomUUID();

        if (!mTemplate.toLowerCase().contains("encrypt")) mTemplate = "encrypt<is>" + encrypt + "<tm>" + mTemplate;

        boolean proceed = true;
        if (brokers.equals("")) {
            SendMessage("No Kafka broker(s) found.");
            proceed = false;
        }
        if (topic.equals("")) {
            SendMessage("No Kafka topic found.");
            proceed = false;
        }

        try {
            String sChk = props.getProperty("retries", "");
            int iChk = Integer.valueOf(sChk);
            retries = sChk;
        } catch (NumberFormatException nfe) {
            retries = Integer.toString(Integer.MAX_VALUE);
        }

        try {
            expiry = Integer.valueOf(GetValue(props, "expiry", "-1"));
        } catch (NumberFormatException nfe) {
            SendMessage("Expiry value must be an integer > 1000.");
            proceed = false;
        }

        try {
            pause = Integer.valueOf(GetValue(props, "pause", "250"));
        } catch (NumberFormatException nfe) {
            SendMessage("Pause value must be an integer > 100.");
            proceed = false;
        }

        try {
            heartbeat = Integer.valueOf(GetValue(props, "heartbeat", "100"));
        } catch (NumberFormatException nfe) {
            SendMessage("HeartBeat value must be an integer > 100.");
            proceed = false;
        }

        int maxPoll;
        try {
            maxPoll = Integer.valueOf(GetValue(props, "maxpoll", "10"));
        } catch (NumberFormatException nfe) {
            maxPoll = 5;
        }

        try {
            numThreads = Integer.valueOf(props.getProperty("threads", ""));
        } catch (NumberFormatException nfe) {
            SendMessage("Number of threads value must be an integer >= 1");
            numThreads = 1;
        }


        props.clear();
        if (!proceed || stopSW) System.exit(1);

        if (mqBkr.equals("")) {
            NamedCommon.que = "1";
            mqvTopic = false;
        } else {
            props = uCommons.LoadProperties(mqBkr);
            if (NamedCommon.ZERROR) System.exit(0);

            ArrayList<String> bkrTasks = new ArrayList<>(Arrays.asList(GetValue(props, "tasks", "").split("\\,")));
            ArrayList<String> bkrQname = new ArrayList<>(Arrays.asList(GetValue(props, "qname", "").split("\\,")));
            ArrayList<String> bkrQload = new ArrayList<>(Arrays.asList(GetValue(props, "loads", "").split("\\,")));
            ArrayList<String> bkrQvols = new ArrayList<>(Arrays.asList(GetValue(props, "responders", "").split("\\,")));

            minQn = 1;
            maxQn = 1;
            qLoad = 0;
            try {
                maxQn = Integer.valueOf(bkrQvols.get(0));
            } catch (NumberFormatException e) {
                maxQn = 0;
            }
            try {
                if (bkrQload.get(0).contains(":")) {
                    qLoad = Integer.valueOf(uCommons.FieldOf(bkrQload.get(0), ":", 2));
                    if (qLoad > 0) maxQn = qLoad;
                }
            } catch (NumberFormatException e) {
                qLoad = 0;
            }
            lastQ = 0;
            numSent = 0;
            String mqTmp, mqType;
            mqUrl = GetValue(props, "url", "");
            mqUsr = GetValue(props, "bkruser", "");
            mqPwd = GetValue(props, "bkrpword", "");
            mqType = GetValue(props, "type", "");
            mqTmp = GetValue(props, "topic", "");
            if (mqTmp.equals("")) {
                mqTmp = GetValue(props, "qname", "").split("\\,")[0];
                if (mqTmp.equals("")) {
                    SendMessage("Property file ["+mqBkr+"] has no Virtual Topic OR MQ Queue defined !!!");
                    System.exit(0);
                }
                mqTmp += "_";
                mqvTopic = false;
                Hop.setShow(mqvTopic);
            } else {
                mqTmp = "VirtualTopic." + mqTmp;
            }
            mqTopic = mqTmp;
            props.clear();

            if (mqUrl.startsWith("ENC(")) mqUrl = uCipher.Decrypt(mqUrl);
            if (mqUsr.startsWith("ENC(")) mqUsr = uCipher.Decrypt(mqUsr);
            if (mqPwd.startsWith("ENC(")) mqPwd = uCipher.Decrypt(mqPwd);
            if (mqType.equals("artemis")) NamedCommon.artemis = true;
        }

        if (NamedCommon.AMS.toLowerCase().equals("split")) {
            String[] newTcols = new String[tcols.length+2];
            String pfx = "";
            int lastPos=0;
            for (int xx = 0; xx < tcols.length; xx++) {
                if (tcols[xx].contains("NestPosition")) {
                    pfx = tcols[xx].replace("NestPosition", "");
                    newTcols[lastPos] = pfx+"CT"; lastPos++;
                    newTcols[lastPos] = pfx+"MV"; lastPos++;
                    newTcols[lastPos] = pfx+"SV"; lastPos++;
                } else {
                    newTcols[lastPos] = tcols[xx]; lastPos++;
                }
            }
            tcols = null;
            tcols = new String[newTcols.length];
            for (int xx=0; xx<newTcols.length; xx++) {
                if (newTcols[xx] != null) {
                    tcols[xx] = newTcols[xx];
                } else {
                    tcols[xx] = "";
                }
            }
            newTcols = null;
        }

        CheckMQconnection();
        CheckKafka();
        Process();
    }
}
