package com.unilibre.kafka;
/* * Copyright UniLibre on 2015. ALL RIGHTS RESERVED  **/


/*     **********************************************       */
/*     Pulls from Kafka and sends to MQ Broker vTopic       */
/*     **********************************************       */

// ------------------------------------[ Notes ] ----------------------------------------------
// ****************************************************
// ***   Can only have 1 consumer per partition !!  ***
// ****************************************************
// It will only consume when;
//  1.  it has read_committed messages in the topic
//  2.  these messages have a LAG greater than the current-offset
//  3.  these messages are less than the log-end-offset (others are uncommitted)
//
// dstopic {topic}
// TOPIC    PARTITION  CURRENT-OFFSET   LOG-END-OFFSET  LAG     CONSUMER-ID         HOST            CLIENT-ID
// myTopic  0          0                250             250     ConsTest-0-[blah]   192.168.48.1    ConsTest-0
//
// Must kill ALL consumers to the group BEFORE krewind can work !!!
//
// "krewind" {topic} with no warnings will set the current-offset to 0
//
// NB: kafka admin commands are NOT instantaneous - you have to wait a minute or two
// --------------------------------------------------------------------------------------------


import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.artemisMQ;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import org.apache.commons.lang.RandomStringUtils;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;


public class kStream {
    private static final DecimalFormat decfmt = new DecimalFormat("#,###.##");

    private static Properties props;
    private static String[] tCols = NamedCommon.rawCols.split("\\,");

    private static String brokers,  topic,   mTemplate="", theQ, thisHost, cfile, runtype, kGroup = "uStreams";
    private static String mqBkr="", mqUsr="", mqPwd="", mqCli="", mqType="", mqUrl="", mqTopic="", main, runID;

    private static ArrayList<String> bkrQload;
    private static ArrayList<String> bkrQvols;

    private static int pause, mqCnt=0, MAX=10000, heartbeat, tCnt=0, pCnt=0, sFnd=0, qLoad=0;
    private static int minQn, maxQn, lastQ, thisQ, numSent, expiry, numThreads, maxPoll=10;
    private static int lCnt=0, zCnt=0, prevTcnt = 0;

    private static boolean stopSW = false, firstSW = true, threading = false, legal = false;
    private static boolean silent = false, sasl = false, mqvTopic = true;
    public static boolean ENCR = false, isConnectedMQ=false;

    static FutureTask[] futures;
    static Thread[] threads = null;

    private static long startMS, finishMS;
    private static double  div = 1000000000.00;
    private static float laps;

    //
    // =========================================================================================================
    //

    public static void main(String[] args) {
        main = "MAIN  ";
        String uuid = UUID.randomUUID().toString();
        runID = RandomStringUtils.random(4, uuid);
        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);
        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
        }

        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties rfProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) return;
        uCommons.SetCommons(rfProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        rfProps = null;

        // The lines below may also be done in rFuel.properties docker=true
        String sProcs = uCommons.ReadDiskRecord("/proc/1/cgroup", true);
        if (!NamedCommon.isDocker) NamedCommon.isDocker = (sProcs.contains("/docker/"));
        if (!NamedCommon.isDocker) NamedCommon.isDocker = (sProcs.contains("/kubepods"));
        if (NamedCommon.isDocker) {
            thisHost = UUID.randomUUID().toString();
            // can't use container hostname - multi-instances will overlap !!
        } else {
            thisHost = "";
        }
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage="";
        MakeCLI();
        SendMessage("Starting kStream(" + mqCli + ") --------------------------------------------------");

        legal = License.IsValid();
        if (!legal) {
            SendMessage("... licence violation.");
            System.exit(1);
        }

        cfile = System.getProperty("conf", "uKafka.properties");
        runtype = System.getProperty("runtype", "uplift");

        SetValues();
        SendMessage(" ");
        if (!isConnectedMQ) {
            SendMessage(" ");
            SendMessage("AMQ has not been connected !");
            System.exit(0);
        }

        ConsumeLog();

        kConsumer.Close();
        activeMQ.CloseProducer();
        SendMessage(" ");
        SendMessage("===============================================================");
        SendMessage("Master stop switch turned ON. Stopping now.");
        SendMessage("===============================================================");
        SendMessage(" ");
        System.exit(1);
    }

    private static void SendMessage(String msg) {
        System.out.println(new Date() + " " + main + " " + msg);
    }
    
    private static void ConsumeLog() {
        kConsumer.updates = new ArrayList<String>();
        ArrayList<String> kItems = new ArrayList<String>();

        if (threading) {
            futures = new FutureTask[numThreads];
            threads = new Thread[numThreads];
        }

        if (kafkaCommons.KERROR) stopSW = true;
        while (!stopSW) {
            kItems.clear();
            kItems = kConsumer.kConsume();
            if (kItems.size() == 0) {
                NothingToDo();
            } else {
                prevTcnt = 0;
                lCnt = 0;
                zCnt = 0;
                ProcessUpdates(kItems);
            }
        }
    }

    private static void NothingToDo() {
        if (firstSW) {
            SendMessage("waiting on delta events");
            firstSW = false;
            silent = true;
        }
        stopSW = CheckStopSW();
        if(!stopSW) kafkaCommons.Sleep(pause);
        String line;
        lCnt++;
        if (lCnt > heartbeat) {
            zCnt++;
            silent = true;
            line = "000"+String.valueOf(zCnt);
            line = line.substring(line.length()-3, line.length());
            SendMessage("kStream(" + mqCli + ")  ("+line+")   <<Heartbeat>>  "+  tCnt + " messages sent to MQ");
            if (prevTcnt == 0) prevTcnt = tCnt;
            if (zCnt > 99) {
                silent = false;
                legal = License.IsValid();
                if (!legal) {
                    if (NamedCommon.BaseCamp.equals(NamedCommon.DevCentre)) {
                        legal = true;
                    } else {
                        SendMessage("Licence Expiriration. Stopping now.");
                        System.exit(0);
                    }
                }
                SendMessage("   .) kStream Commit & Close");
                SendMessage("   .) Reconnect with Kafka");
                kConsumer.Commit();
                kConsumer.Close();
                kConsumer.kConnect();
                SendMessage("   .) Reconnect with AMQ");
                isConnectedMQ = false;
                CheckMQconnection();
                zCnt=0;
                // will stop after 100 non-action loops.
                // should be restarted by container management (docker-compose)
                // ... Hmmm - not always it seems !!
                // System.exit(non-zero) will invoke the restart strategy
                if (prevTcnt == tCnt) stopSW = true;
            }
            lCnt = 0;
        }
    }

    private static void ProcessUpdates(ArrayList<String> updates) {
        int eoi = updates.size();
        if (eoi == 0) return;
        if (threading) {
            CallThreads(updates);
        } else {
            for (int u=0; u < eoi; u++) { ThreadDo(updates.get(u)); }
        }
    }

    private static void CallThreads(ArrayList<String> updates) {
        startMS = System.nanoTime();
        SendMessage("---------------------------------------------------------------------------------");
        SendMessage("*");
        int upto=0, totSize=updates.size();
        int parts = (totSize / numThreads) + 1;
        SendMessage("(" + totSize+ ") Messages to process");
        ArrayList<String> sendArray;
        Callable callme;

        for (int i=0 ; i < numThreads ; i++) {
            sendArray = new ArrayList<>();
            for (int m=0 ; m <= parts ; m++) {
                if (upto < totSize) {
                    sendArray.add(updates.get(upto));
                    upto++;
                }
            }
            kafka2amq k2a = new kafka2amq(i);
            callme = SetThread(k2a, sendArray);
            futures[i] = new FutureTask(callme);
            Thread t = new Thread(futures[i]);
            t.start();
        }

        SendMessage("Waiting on threads ----------------------------");
        ArrayList<Integer> finishedT = new ArrayList<>();
        ArrayList<String> returnList= new ArrayList<>();
        int hangCnt=0;
        Object obj;
        finishedT.add(9999);
        boolean areAlive=true;
        boolean okay2commit=true;
        while (areAlive) {
            Sleep(1);
            for (int x = 0; x < numThreads; x++) {
                if (finishedT.indexOf(x) > 0) continue;
                if (futures[x].isDone()) {
                    SendMessage("thread # " + x + " has finished. ************");
                    try {
                        obj = futures[x].get();
                        if (obj instanceof ArrayList) returnList.addAll((Collection<? extends String>) obj);
                        obj = null;
                    } catch (InterruptedException e) {
                        SendMessage("***********[ error retrieving failures (01) ] *************");
                        SendMessage(e.getMessage());
                        SendMessage("***********[       unrecoverable            ] *************");
                        okay2commit=false;
                        stopSW = true;
                    } catch (ExecutionException e) {
                        SendMessage("***********[ error retrieving failures (02) ] *************");
                        SendMessage(e.getMessage());
                        SendMessage("***********[       unrecoverable            ] *************");
                        okay2commit=false;
                        stopSW = true;
                    }
                    finishedT.add(x);
                    areAlive = false;
                }
            }
            if (finishedT.size() > numThreads) areAlive = false;
            hangCnt++;
            if (hangCnt > 100) break;
        }

        finishMS = System.nanoTime();
        laps = (float) ((finishMS - startMS) / div);
        SendMessage("All threads completed in " + decfmt.format(laps) + " seconds");
        laps = (float) (totSize / laps);
        SendMessage("Throughput of " + decfmt.format(laps) + " events per second");
        if (hangCnt > 100) stopSW = true;
        finishedT.clear();
        if (okay2commit) kConsumer.Commit();
        SendMessage(" ");
    }

    private static kafka2amq SetThread(kafka2amq k2a, ArrayList<String> send2thread) {
        MakeCLI();
        //
        String que = "001", topic = mqTopic;
        if (mqTopic.endsWith("_")) topic += que;
        k2a.SetMQurl(mqUrl);
        k2a.SetMQtopic(topic);
        k2a.SetMQque(topic);
        k2a.SetMqusr(mqUsr);
        k2a.SetMqpwd(mqPwd);
        k2a.SetMQcli(mqCli);
        k2a.SetTemplate(mTemplate);
        k2a.SetLogEvents(send2thread);
        return k2a;
    }

    public static void ThreadDo (String inMessage) {
        String line, datetime, account, file, itemId, record, payload, message, passport, issuer, logid;
        String answer="";
        line = inMessage;

        boolean proceed = kafkaCommons.SetPayload(line);
        if (!proceed) {
            SendMessage("We received a \"corrupt\" record from kafka - not in JSON format.");
            SendMessage(line);
            SendMessage("This record was ignored. --------------------------------------");
            return;   //  this is NOT a JSON obect we can work with.
        }

        passport = kafkaCommons.GetPassport();
        issuer   = kafkaCommons.GetIssuer();
        datetime = kafkaCommons.GetTimeDate();
        logid    = kafkaCommons.GetLogid();
        account  = kafkaCommons.GetAccount();
        file     = kafkaCommons.GetFile();
        itemId   = kafkaCommons.GetItem();
        record   = kafkaCommons.GetRecord();

        payload  = account + kafkaCommons.delim + file + kafkaCommons.delim + itemId + kafkaCommons.delim + record;
        NamedCommon.MessageID = logid + kafkaCommons.delim + account + kafkaCommons.delim + file + kafkaCommons.delim + itemId;
        NamedCommon.CorrelationID = "Load--" + logid + "--" + account + "--" + file + "--" + itemId;

        message     = mTemplate;
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

        tCnt++;
        pCnt++;
        if (pCnt >= 500) {
            SendMessage(tCnt + " messages have been sent to amq in TOTAL.");
            pCnt = 0;
        }
    }

    private static void Sleep(int s) {
        if (s ==0) {
            s = 500;
        } else {
            s = s * 1000;
        }
        try {
            Thread.sleep(s);
        } catch (InterruptedException e) {
            // do nothing.
        }
    }

    private static String Resolve(String inmsg, String repl, String invar) {
        while (inmsg.contains(repl)) {
            inmsg = inmsg.replace(repl, invar);
        }
        return inmsg;
    }

    private static void MakeCLI() {
        mqCnt++;
        if (mqCnt >= MAX) mqCnt=0;
        if (!NamedCommon.isDocker) {
            mqCli = NamedCommon.pid + "-kStream-" + + mqCnt;
        } else {
            mqCli = "uK2MQ-" + runID + "-" + mqCnt;
        }
    }

    private static boolean SendToMQ(String message) {
        boolean mqPassed = false;
        String eMsg="";
        while (!mqPassed) {
            if (NamedCommon.artemis) {
                eMsg = "ArtemisMQ Producer ERROR: ";
                try {
                    mqPassed = artemisMQ.produce(mqUrl, mqUsr, mqPwd, mqCli, mqTopic, message);
                } catch (JMSException e) {
                    mqPassed = false;
                    eMsg += e.getMessage();
                }
            } else {
                eMsg = "ActiveMQ Producer ERROR: ";
                // -------------------------------------------------------------------------------
                // what if I want to send the message to N queues all at once?
                // must set proceed<is>false or rFuel will forward to next queue
                //     : maintain String[]mqTopics --- dam good idea !
                //     : maintain String[]mqUrls  ---- NO, keep everything on one BrokerURL
                // -------------------------------------------------------------------------------
                if (!mqvTopic) {
                    theQ = mqTopic + GetNextQue();
                } else {
                    theQ = mqTopic;
                }
                if (!isConnectedMQ) {
                    CheckMQconnection();
                    isConnectedMQ = true;
                }
                mqPassed = activeMQ.produce(mqUrl, mqUsr, mqPwd, mqCli, theQ, message);
            }
            if (!mqPassed) {
                SendMessage(eMsg);
                SendMessage("Trying to reclaim a connection:.");
                CheckMQconnection();
            }
        }
        return mqPassed;
    }

    private static String GetNextQue() {
        if (qLoad > 1) {
            if (lastQ >= 1) {
                if (numSent >= qLoad && qLoad > 1) {
                    thisQ = 1;
                    numSent = 0;
                } else {
                    numSent++;
                    thisQ = lastQ + 1;
                    if (thisQ > maxQn) thisQ = 2;    // do not go back to 1
                }
            } else {
                numSent = 0;
                thisQ = lastQ + 1;
            }
        } else {
            thisQ = lastQ + 1;
            if (thisQ > maxQn) thisQ = 1;
        }
        if (thisQ > maxQn) thisQ = 1;
        if (thisQ < minQn) thisQ = 1;
        lastQ = thisQ;
        String qNbr = uCommons.RightHash("000" + thisQ, 3);
        return qNbr;
    }

    public static void SetValues() {
        if (firstSW && !silent) {
            SendMessage(" ");
            SendMessage("-----------------------------------------------------------------------");
            SendMessage("Setup for " + cfile);
        }

        props = kafkaCommons.LoadProperties(cfile);
        if (kafkaCommons.KERROR) { System.exit(0); }
        NamedCommon.databaseType = GetValue(props, "dbtype", "");
        NamedCommon.protocol = GetValue(props, "protocol", "");
        switch (NamedCommon.protocol) {
            case "u2cs":
                NamedCommon.dbhost = GetValue(props, "u2host", "");
                if (NamedCommon.dbhost.equals("")) {
                    SendMessage("FATAL: No Source data directory specified.");
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
                if (firstSW) SendMessage("INFO: No source database definition in " + cfile);
        }

        ENCR   = GetValue(props, "encrypt", "").toLowerCase().equals("true");

        brokers   = GetValue(props, "brokers", "");
        topic     = GetValue(props, runtype, "uplift");
        mTemplate = GetValue(props, "message", "");
        mqBkr     = GetValue(props, "bkr", "");
        // the Kafka GROUP is referenced in kafka commands such as rewind and describe - it MUST be consistent
        kGroup    = GetValue(props, "group", kGroup);               // E.g. uStream

        if (ENCR) {
            if (!mTemplate.toLowerCase().contains("encrypt")) mTemplate = "encrypt<is>true<tm>" + mTemplate;
        }
        kafkaCommons.SetENCR(ENCR);

        if (brokers.equals("")) {
            SendMessage("No Kafka broker(s) found.");
            System.exit(0);
        }
        if (topic.equals("")) {
            SendMessage("No Kafka topic found.");
            System.exit(0);
        }

        try {
            String nthrd = props.getProperty("threads", "");
            numThreads = Integer.valueOf(nthrd);
            threading  = true;
        } catch (NumberFormatException nfe) {
            SendMessage("Threads  value must be an integer.");
            System.exit(0);
        }

        try {
            expiry    = Integer.valueOf(GetValue(props, "expiry", "-1"));
        } catch (NumberFormatException nfe) {
            SendMessage("Expiry value must be an integer > 1000.");
            System.exit(0);
        }

        try {
            pause = Integer.valueOf(GetValue(props, "pause", "250"));
        } catch (NumberFormatException nfe) {
            SendMessage("Pause value must be an integer > 100.");
            System.exit(0);
        }

        try {
            heartbeat = Integer.valueOf(GetValue(props, "heartbeat", "100"));
        } catch (NumberFormatException nfe) {
            SendMessage("HeartBeat value must be an integer > 100.");
            System.exit(0);
        }

        try {
            maxPoll = Integer.valueOf(GetValue(props, "maxpoll", "10"));
        } catch (NumberFormatException nfe) {
            maxPoll = 5;
        }

        stopSW = (GetValue(props, "stop", "").toLowerCase().equals("true"));

        props.clear();

        if (mqBkr.equals("")) {
            NamedCommon.que = "1";
            mqvTopic = false;
        } else {
            props = uCommons.LoadProperties(mqBkr);
            if (NamedCommon.ZERROR) System.exit(0);
            if (!threading) {

                bkrQload = new ArrayList<>(Arrays.asList(GetValue(props, "loads", "").split("\\,")));
                bkrQvols = new ArrayList<>(Arrays.asList(GetValue(props, "responders", "").split("\\,")));

                // HARD-CODE sFnd to 0 as an rFuel entry point.
                // In the bkr file, it can be 025 or 022 depending on kafka before RDS ...
                sFnd = 0;
                minQn = 1;
                try {
                    maxQn = Integer.valueOf(bkrQvols.get(sFnd));
                } catch (NumberFormatException e) {
                    maxQn = 0;
                }

                try {
                    if (bkrQload.get(sFnd).contains(":")) {
                        qLoad = Integer.valueOf(uCommons.FieldOf(bkrQload.get(sFnd), ":", 2));
                        if (qLoad > 0) maxQn = qLoad;
                    } else {
                        qLoad = 0;
                    }
                } catch (NumberFormatException e) {
                    qLoad = 0;
                }
                lastQ = 0;
                numSent = 0;
            }

            String mqTmp;
            mqUrl = GetValue(props, "url", "");
            mqUsr = GetValue(props, "bkruser", "");
            mqPwd = GetValue(props, "bkrpword", "");
            mqType = GetValue(props, "type", "");
            mqTmp = GetValue(props, "topic", "");
            if (mqTmp.equals("")) {
                mqTmp = GetValue(props, "qname", "").split("\\,")[sFnd];
                if (mqTmp.equals("")) {
                    SendMessage("Property file ["+mqBkr+"] has no Virtual Topic OR MQ Queue defined !!!");
                    System.exit(0);
                }
                mqTmp += "_";           // e.g. Kafka_
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

        kafkaCommons.rawDB = NamedCommon.rawDB;
        NamedCommon.SqlDatabase = kafkaCommons.rawDB;
        NamedCommon.isNRT = true;
        NamedCommon.isRest= false;
        NamedCommon.isWhse= true;

        if (NamedCommon.AMS.toLowerCase().equals("split") && !threading) {

            String[] newTcols = new String[tCols.length+2];
            String pfx = "";
            int lastPos=0;
            for (int xx = 0; xx < tCols.length; xx++) {
                if (tCols[xx].contains("NestPosition")) {
                    pfx = tCols[xx].replace("NestPosition", "");
                    newTcols[lastPos] = pfx+"CT"; lastPos++;
                    newTcols[lastPos] = pfx+"MV"; lastPos++;
                    newTcols[lastPos] = pfx+"SV"; lastPos++;
                } else {
                    newTcols[lastPos] = tCols[xx]; lastPos++;
                }
            }
            tCols = null;
            tCols = new String[newTcols.length];
            for (int xx=0; xx<newTcols.length; xx++) {
                if (newTcols[xx] != null) {
                    tCols[xx] = newTcols[xx];
                } else {
                    tCols[xx] = "";
                }
            }
            newTcols = null;
        }

        kConsumer.SetBroker(brokers);
        kConsumer.SetTopic(topic);
        kConsumer.SetGroup(kGroup);
        kConsumer.SetClientID(kGroup+":"+runID);

        // these are not private variables
        kConsumer.tCols = tCols;
        kConsumer.pause = pause;
        kConsumer.MAX_NO_MESSAGE_FOUND_COUNT=heartbeat;
        kConsumer.MAX_POLL_RECORDS = maxPoll;
        NamedCommon.rFuelLogs = true;

        if (sasl) {
            // still need to develop / assign sasl details in a config file
            String saslUser="", saslPass="",saslKey="";
            kConsumer.SetSASL(true, saslUser, saslPass, saslKey);
        }

        // AMQ & Kafka Brokers MUST be started and ready for service
        if (firstSW) {
            SendMessage(" ");
            SendMessage("Starting with these parameters: -------------------------------------- ");
            SendMessage("Config:*******");
            SendMessage("   .) Using: " + cfile);
            SendMessage("   .) SQL  : " + NamedCommon.jdbcCon);
            SendMessage("   .) Thds : " + numThreads);
            SendMessage("Kafka:********");
            SendMessage(" broker(s): " + brokers);
            SendMessage(" SRC topic: " + topic);
            SendMessage("     group: " + kGroup);
            SendMessage(" TGT topic: " + "{file}-" + NamedCommon.topicExtn);
            SendMessage("  max-poll: " + maxPoll);
            SendMessage("      sasl: " + sasl);
            SendMessage(" ");
            SendMessage("AMQ:**********");
            SendMessage("    broker: " + mqBkr);
            SendMessage("      type: " + mqType);
            SendMessage("       url: " + mqUrl);
            SendMessage("   V-Topic: " + mqvTopic);
            SendMessage("  MQ-topic: " + mqTopic + "nnn");
            SendMessage("      user: " + mqUsr);
            SendMessage("-----------------------------------------------------------------------");
            SendMessage(" ");
            SendMessage("Setup finished.");
            SendMessage(" ");
        }
        if (!silent) {
            SendMessage("Check Kafka connection() ****************************************");
        }
        kConsumer.SetSilent(silent);
        kConsumer.Close();
        kConsumer.kConnect();
        if (kConsumer.GetConsumer() != null) {
            if (!silent) SendMessage("Passed **********************************************************");
        } else {
            kafkaCommons.KERROR = true;
            return;
        }
        CheckMQconnection();
    }

    private static void CheckMQconnection() {
        if (!silent) SendMessage("Check MQconnection() ******************************************");

        activeMQ.SetExpiry(expiry); // 2mins
        activeMQ.SetDocker(true);
        activeMQ.SetHost(thisHost);
        activeMQ.CloseProducer();
        // this is a JMS message consumer - not an activemq message consumer !
        MessageProducer prod = null;
        int failedTries=0;

        while (prod == null) {
            com.unilibre.MQConnector.commons.ZERROR = false;
            prod = activeMQ.rfProducer(mqUrl, mqUsr, mqPwd, "kStream", NamedCommon.testQ);
            if (!com.unilibre.MQConnector.commons.ZERROR) {
                if (!silent) SendMessage("Passed ********************************************************");
                isConnectedMQ = true;
                prod = null;
                break;
            } else {
                SendMessage("...");
                SendMessage("...");
                SendMessage("...");
                SendMessage("Waiting for an MQ connection. Sleep 5 seconds.");
                uCommons.Sleep(5);
                failedTries++;
                if (failedTries > 60) {
                    SendMessage("Giving up.");
                    System.exit(0);
                }
            }
        }
    }

    public static String GetValue(Properties runProps, String key, String def) {
        String value = runProps.getProperty(key, def);
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

    private static boolean CheckStopSW() {
        props = uCommons.LoadProperties(cfile);
        return (props.getProperty("stop", "").toLowerCase().equals("true"));
    }
    
}
