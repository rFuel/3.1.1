package com.unilibre.core;

/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.artemisMQ;
import com.unilibre.MQConnector.commons;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.kafka.kProducer;

import javax.jms.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class responder implements MessageListener {

    public static boolean StopFlag = false;
    private static AtomicBoolean IamWaiting = new AtomicBoolean(false);
    private static AtomicBoolean restartActive = new AtomicBoolean(false);;
    private static boolean restart = false;
    private static boolean okay =false;
    private static boolean safe2restart=false;
    private static String TheMessage = "";
    private static boolean isPing = false;

    private static String ReplyTo = "";
    private static String conf = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash;

    private static int ackMode;
    private static int maxNoAction = 50;
    private static int noActionCtr  = 0;
    private static int heavyUseCtr  = 0;
    private static int nextTest = 1;
    private static int nextMsg = 1;
    public static String messageBrokerUrl;
    private static String inputQueue;
    public static String uTask;
    private static String listeningOn;
    private static String qNbr;
    private static String CorrelID;
    private static String MessageID;
    private static String uExec = "COUNT VOC LIKE upl_...";
    private static String rExec = "COUNT MD = \"upl_]\"";
    private static String heartbeat = "<<heartbeat>> restart";
    private static String mqcountdown = " MQ in $$ seconds";
    private static String dbcountdown = " DB's in ## seconds";
    private static String msgIn = "Message  <><><><><><><><><><><><><><><><><><><><><><><><><><><>";
    private static String rMsg = "********************  Responder Starting ********************";
    private static String ok200 = "<status>200</status><message>OK</message>";
    private static String defaultQ = "";
    private static MessageConsumer consumer = null;
    private static long lastMessage, dbActive, mqActive;
    private static long rightNow, lastHB;
    private static double laps, mqlaps, dblaps;
    private static double div = 1000000000.00;
    private static long startM = 0, finishM = 0, lastM = 0;
    private static int rHash = String.valueOf(NamedCommon.mqWait).length() + 1;
    private static int dbCheck = 60;
    private static TextMessage inMsg;

    static {
        messageBrokerUrl = "tcp://localhost:61616";
        inputQueue = "queue_not_initialised";
        ackMode = Session.AUTO_ACKNOWLEDGE;
        IamWaiting.set(false);
    }

    public static void SetDBactive(long now) {
        dbActive = now;
    }

    public static void respond(String task, String que) {
        GarbageCollector.CleanUp();
        NamedCommon.Reset();
        StopFlag = false;
        IamWaiting.set(true);
        okay =false;
        safe2restart=false;
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage="";
        NamedCommon.hostname = NamedCommon.hostname.replaceAll("\\r?\\n", "");

        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        uTask = task;
        qNbr = que;

        if (NamedCommon.isWhse || NamedCommon.isNRT) defaultQ = "019_Finish";
        if (NamedCommon.isRest) defaultQ = "019_DefaultReplies";

        dbActive = NamedCommon.dbActive;
        // ------------- 30-11-24 -----------------
//        NamedCommon.Reset();
//        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        // ----------------------------------------

        if (NamedCommon.isDocker) {
            String cnfHost = NamedCommon.hostname;
            NamedCommon.hostname = uCommons.ReadDiskRecord("/etc/hostname", true);
            if (NamedCommon.hostname.equals("")) NamedCommon.hostname = cnfHost;
            cnfHost=null;
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage="";
        }

        if (task.equals("025")) PrepareKafka();
        boolean stopNow = coreCommons.StopNow();

        while (!stopNow && !safe2restart) {
            lastMessage=System.nanoTime();
            responder resp = new responder();
            if (NamedCommon.ZERROR) return;
            mqActive = System.nanoTime();
            WaitForMessage();
            resp = null;
            if (NamedCommon.ZERROR) { stopNow=true; break; }
            NamedCommon.Reset();
            uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt); // MUST clear AES vars first.
            runProps.clear();
            runProps = uCommons.LoadProperties("rFuel.properties");
            uCommons.SetCommons(runProps);
            uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
            runProps.clear();
            IamWaiting.set(true);
            if (mqCommons.cmqFactory  != null) mqCommons.CloseConnection();
            if (pmqCommons.pmqFactory != null) pmqCommons.CloseConnection();
            stopNow = coreCommons.StopNow();
        }

        if (safe2restart) {
            uCommons.uSendMessage("<<heartbeat>> resetting responder while inactive.");
            GarbageCollector.CleanUp();
            return;     // returns to StartUp
        }

        StopFlag = true;
        System.out.println("");
        uCommons.uSendMessage("<<heartbeat>> Waiting for the pid to die.");
        GarbageCollector.CleanUp();
        uCommons.Sleep(0);
        System.exit(1);
    }

    public responder() {

        if (!restart) {
            System.out.println(" ");
            uCommons.uSendMessage(NamedCommon.block);
            uCommons.uSendMessage(rMsg);
            uCommons.uSendMessage(NamedCommon.block);
            System.out.println(" ");
        } else {
            uCommons.uSendMessage("Responder has restarted.");
        }

        Properties props;
        // --------------------------------------------------------------
        // isNRT does not require a SourceDB - the data comes from kafka
        // For non-NRT tasks, validate that the SourceDB is contactable
        // --------------------------------------------------------------

        // ## Gitlab 921

        if (!NamedCommon.isNRT) {
            uCommons.uSendMessage("ConnectSourceDB() ----------------------------------------------------------");
            SourceDB.ConnectSourceDB();
            while (!NamedCommon.sConnected) {
                NamedCommon.masterStop = coreCommons.StopNow();
                if (NamedCommon.masterStop) uCommons.StopProcessNow();
                SourceDB.ReconnectService();
            }
            NamedCommon.runSilent = false;
            NamedCommon.U2File = u2Commons.uOpen("BP.UPL");
            if (NamedCommon.U2File.isOpen()) {
                String stoprec = String.valueOf(u2Commons.uRead(NamedCommon.U2File, "STOP"));
                if (stoprec.contains("stop")) {
                    uCommons.uSendMessage(NamedCommon.block);
                    uCommons.uSendMessage("STOP switch set ON");
                    uCommons.uSendMessage(NamedCommon.block);
                    NamedCommon.ZERROR = true;
                    SourceDB.DisconnectSourceDB();
                    uCommons.uSendMessage("DisconnectSourceDB() -------------------------------------------------------");
                    NamedCommon.Zmessage = "Pausing 60 seconds for you to turn OFF the STOP switch";
                    return;
                }
            } else {
                uCommons.uSendMessage(NamedCommon.block);
                uCommons.uSendMessage("Cannot find BP.UPL !! Check rFuel.propertes");
                uCommons.uSendMessage(NamedCommon.block);
                uCommons.uSendMessage("DisconnectSourceDB() -------------------------------------------------------");
                NamedCommon.Zmessage = "Pausing 60 seconds for you to check rFuel.properties";
                SourceDB.DisconnectSourceDB();
                return;
            }
            props = null;
            SourceDB.DisconnectSourceDB();
            uCommons.uSendMessage("DisconnectSourceDB() -------------------------------------------------------");
            SqlCommands.DisconnectSQL();
        }
        if (pmqCommons.pmqFactory != null) pmqCommons.CloseConnection();
        if (mqCommons.cmqFactory  != null) mqCommons.CloseConnection();
        Hop.Restart();

        if (NamedCommon.hostname.equals("")) {
            String cmd = License.InitialiseRange();
            boolean junk = License.GetDomain(cmd);
            NamedCommon.hostname = License.domain;
            if (NamedCommon.hostname.equals("")) NamedCommon.hostname = "rFuel";
        }
        if (NamedCommon.Broker.equals("")) GetBroker();
        props = uCommons.LoadProperties(NamedCommon.Broker);
        uCommons.BkrCommons(props);
        boolean bkrExists = (!props.getProperty("tasks", "").equals(""));
        while (!bkrExists) {
            uCommons.uSendMessage("Broker [" + NamedCommon.Broker + "] does not exist! Fix this in conf/this.server ... looping until fixed.");
            uCommons.Sleep(30);
            GetBroker();
            props = uCommons.LoadProperties(NamedCommon.Broker);
            uCommons.BkrCommons(props);
            bkrExists = (!props.getProperty("tasks", "").equals(""));
        }
//        NamedCommon.messageBrokerUrl = props.getProperty("url");
        ArrayList<String> TasksArray = new ArrayList<>(Arrays.asList(props.getProperty("tasks").split("\\,")));
        ArrayList<String> qNameArray = new ArrayList<>(Arrays.asList(props.getProperty("qname").split("\\,")));
        int tPos = TasksArray.indexOf(uTask);
        inputQueue = qNameArray.get(tPos) + "_";
        int lxq = qNbr.length();
        for (int ii = lxq; ii < 3; ii++) { inputQueue += "0"; }
        inputQueue += qNbr;
        NamedCommon.inputQueue = inputQueue;
        ReplyRequired();
        NamedCommon.task = uTask;
        listeningOn = "PID: "+NamedCommon.pid+" listening on queue (" + NamedCommon.inputQueue + ") of " + NamedCommon.Broker + " ";
        listeningOn+= NamedCommon.block;
        listeningOn = uCommons.LeftHash(listeningOn, 75);

        boolean keepTying=true;
        int failedTries=0;
        while (keepTying) {
            setupMessageQueueConsumer();
            if (NamedCommon.ZERROR) {
                NamedCommon.ZERROR = false;
                uCommons.uSendMessage("...");
                uCommons.uSendMessage("...");
                uCommons.uSendMessage("...");
                uCommons.uSendMessage("Waiting for an MQ connection. Sleep 5 seconds.");
                uCommons.Sleep(5);
                failedTries++;
                if (failedTries > 60) {
                    uCommons.uSendMessage("Giving up.");
                    System.exit(0);
                }
            } else {
                keepTying=false;
                failedTries=0;
            }
        }

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("-------------------- create connection ----------------------");
            uCommons.uSendMessage("     Host : " + NamedCommon.hostname);
            uCommons.uSendMessage("   Broker : " + NamedCommon.messageBrokerUrl);
            uCommons.uSendMessage("    Queue : " + NamedCommon.inputQueue);
            uCommons.uSendMessage("     User : " + NamedCommon.bkr_user);
            uCommons.uSendMessage(" Password : " + "************"); // NamedCommon.bkr_pword);
            uCommons.uSendMessage(" ClientID : " + NamedCommon.hostname + ":" + NamedCommon.inputQueue);
            uCommons.uSendMessage("-------------------------------------------------------------");
        }
    }

    private void ReplyRequired() {
        NamedCommon.replyReqd = true;
        switch (uTask) {
            case "015":
                NamedCommon.replyReqd = false;
                break;
            case "022":
                NamedCommon.replyReqd = false;
                break;
            case "025":
                NamedCommon.replyReqd = false;
                break;
            case "910":
                NamedCommon.replyReqd = false;
                break;
            case "920":
                NamedCommon.replyReqd = false;
                break;
            case "999":
                NamedCommon.replyReqd = false;
                break;
        }
    }

    public static void GetBroker() {
        if (NamedCommon.Broker.equals("")) {
            Properties sProps = uCommons.LoadProperties("this.server");
            if (sProps.size() > 0) {
                String[] brokers = sProps.getProperty("brokers", "").split(",");
                for (int b = 0; b < brokers.length; b++) {
                    if (brokers[b].length() > 0) {
                        uCommons.uSendMessage("Checking : " + brokers[b] + " for task " + uTask);
                        Properties bProps = uCommons.LoadProperties(brokers[b]);
                        String bTasks = bProps.getProperty("tasks");
                        if (bTasks.contains(uTask)) {
                            NamedCommon.Broker = brokers[b];
                            uCommons.BkrCommons(bProps);
//                            NamedCommon.messageBrokerUrl = bProps.getProperty("url");
                            uCommons.uSendMessage("  On URL : " + NamedCommon.messageBrokerUrl);
                            break;
                        }
                    }
                }
                if (NamedCommon.Broker.equals("")) {
                    uCommons.uSendMessage("<<FAIL>> inresponder.respond() - cannot find a broker for task " + uTask);
                    StopFlag = true;
                    return;
                }
            } else {
                uCommons.uSendMessage("<<FAIL>> in responder.respond() - cannot find 'this.server'");
                StopFlag = true;
                return;
            }
        }
    }

    public void setupMessageQueueConsumer() {
        String junk;
        uCommons.uSendMessage("Create MQ connection to queue: " + NamedCommon.inputQueue + "  on broker: " + NamedCommon.messageBrokerUrl);
        if (!NamedCommon.artemis) {
            activeMQ.SetDocker(NamedCommon.isDocker);
            activeMQ.SetHost(NamedCommon.hostname);
            consumer = activeMQ.rfConsumer(NamedCommon.messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, "Broker-consumer", NamedCommon.inputQueue);
            if (commons.ZERROR) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = commons.ERRmsg;
            } else {
                NamedCommon.ZERROR = false;
                NamedCommon.Zmessage="";
            }
        } else {
            String url = NamedCommon.messageBrokerUrl;
            String usr = NamedCommon.bkr_user;
            String pwd = NamedCommon.bkr_pword;
            String name;
            name = "Consumer:" + NamedCommon.task + ":" + NamedCommon.hostname + ":" + NamedCommon.pid;
            String que = NamedCommon.inputQueue;
            try {
                consumer = artemisMQ.getConsumer(url, usr, pwd, name, que);
            } catch (JMSException e) {
                NamedCommon.ZERROR = true;
                uCommons.uSendMessage("--------------------------------------------------------------------");
                uCommons.uSendMessage("Issue with: "+url+" "+usr+" *** "+name+" "+que);
                NamedCommon.Zmessage = "com.unilibre.core.responder.setupMessageQueueConsumer ERROR:";
                uCommons.uSendMessage(NamedCommon.Zmessage);
                uCommons.uSendMessage(e.getMessage());
                uCommons.uSendMessage("--------------------------------------------------------------------");
                return;
            }
        }
        if (NamedCommon.ZERROR) {
            junk = NamedCommon.Zmessage;
            NamedCommon.Zmessage="";
            uCommons.uSendMessage("------------------------------------------------------");
            uCommons.uSendMessage("Error on connection with MQ service:");
            uCommons.uSendMessage(junk);
            uCommons.uSendMessage("------------------------------------------------------");
            junk="";
        } else {
            lastMessage = System.nanoTime();
            try {
                consumer.setMessageListener(this);
                uCommons.uSendMessage(listeningOn);
                System.out.println("");
            } catch (JMSException e) {
                uCommons.uSendMessage("*");
                uCommons.uSendMessage("*******************************************");
                uCommons.uSendMessage("*");
                uCommons.uSendMessage("* Cannot create a consume / connection to <<" + NamedCommon.inputQueue + ">>");
                uCommons.uSendMessage("* Reason: " + e.getMessage());
                uCommons.uSendMessage("* Stopping now.");
                uCommons.uSendMessage("*");
                uCommons.uSendMessage("*******************************************");
                System.exit(0);
            }
        }
    }

    private static void WaitForMessage() {
        String msg = "", timeLeft = "", tLine="", tExec="";
        if (NamedCommon.protocol.equals("real")) {
            tExec = rExec;
        } else {
            tExec = uExec;
        }
        int mqIdle=0, dbIdle=0, dbChk=0, countDown=0;
        int idleCnt=0, mqTimeout=0;
        long lastCheck, lastGC = System.nanoTime();
        boolean loopCtl = true;
        restart = false;

        boolean stopNow = coreCommons.StopNow();
        if (stopNow) uCommons.StopProcessNow();

        while (loopCtl && !stopNow) {
            if (!IamWaiting.get() && NamedCommon.isWhse) uCommons.Sleep(5);
            // RESET counters
            mqIdle=0; dbIdle=0; dbChk=0; countDown=0; idleCnt=0; mqTimeout=0;
            lastHB = System.nanoTime();
            lastCheck = System.nanoTime();
            while (IamWaiting.get()) {
                idleCnt++;
                if (idleCnt > 20) {
                    uCommons.CleanupSettings(NamedCommon.BaseCamp, "uojlog");
                    idleCnt = 0;
                }
                try {
                    Thread.sleep(150);
                } catch (InterruptedException e) {
                    uCommons.uSendMessage("Thread.sleep issue in WaitForMessage()");
                }

                rightNow = System.nanoTime();

                laps = (rightNow - lastHB) / div;
                countDown = ((int) (laps + 0.5));

                if (NamedCommon.isNRT) {
                    // NEVER touch the SourceDB on NRT
                    dbIdle = NamedCommon.dbWait - 2;
                    dbChk  = dbCheck - 2;
                    mqTimeout = NamedCommon.mqWait - 2;
                    mqIdle    = mqTimeout;
                } else {
                    laps = (rightNow - lastMessage) / div;
                    mqTimeout = ((int) (laps + 0.5));
                    mqIdle = mqTimeout;

                    dblaps = (rightNow - dbActive) / div;
                    dbIdle = ((int) (dblaps + 0.5));

                    dbChk = (int) (((rightNow - lastCheck) / div) + 0.5);
                }

                if (!IamWaiting.get()) break;

                // ----------------------------------------
                // check if the DB still active & available
                // ----------------------------------------
                if (dbChk >= dbCheck) {
                    if (NamedCommon.PoolDebug) {
                        tLine = "mq="+mqTimeout + " hb=" + countDown + " db=" + dbIdle;
                        uCommons.uSendMessage(tLine);
                    }

                    if (NamedCommon.sConnected) {
                        if (!u2Commons.TestAlive()) {
                            restartActive.set(true);
                            SourceDB.ReconnectService();
                            restartActive.set(false);
                        }
                    }
                    if (NamedCommon.uCon != null) {
                        try {
                            if (!NamedCommon.uCon.isValid(2)) SqlCommands.ReconnectService();
                        } catch (SQLException e) {uCommons.uSendMessage(e.getMessage());}
                    }
                    lastCheck = System.nanoTime();
                }

                if (!IamWaiting.get()) break;

                // ----------------------------------------
                // DB Restart Services
                // ----------------------------------------
                if (dbIdle >= NamedCommon.dbWait) {
                    if (NamedCommon.tConnected && IamWaiting.get()) {
                        restartActive.set(true);
                        System.out.println(" ");
                        uCommons.uSendMessage("<<heartbeat>> Reconnect TargetDB - idle timeout exceeded.");
                        NamedCommon.runSilent = true;
                        SqlCommands.DisconnectSQL();
                        SqlCommands.ConnectSQL();
                        GarbageCollector.CleanUp();
                        NamedCommon.runSilent = false;
                        lastHB = System.nanoTime();
                        restartActive.set(false);
                        uCommons.uSendMessage("<<heartbeat>> service is listening again.");
                    }
                    if (NamedCommon.sConnected && IamWaiting.get()) {
                        restartActive.set(true);
                        NamedCommon.runSilent = true;
                        System.out.println(" ");
                        uCommons.uSendMessage("<<heartbeat>> Reconnect SourceDB - idle timeout exceeded.");
                        if (SourceDB.ReConnect()) {
                            GarbageCollector.CleanUp();
                            lastHB = System.nanoTime();
                            dbActive = System.nanoTime();
                        } else {
                            SourceDB.ReconnectService();
                        }
                        lastHB = System.nanoTime();
                        restartActive.set(false);
                        NamedCommon.runSilent = false;
                        uCommons.uSendMessage("<<heartbeat>> service is listening again.");
                    }
                    NamedCommon.runSilent = false;
                    dbActive = System.nanoTime();
                    lastCheck = dbActive;
                    restart = true;
                }

                if (!IamWaiting.get()) break;

                // ----------------------------------------
                // MQ Restart Service
                // ----------------------------------------
                if (mqTimeout >= NamedCommon.mqWait) {
                    System.out.println(" ");
                    restartActive.set(true);
                    uCommons.uSendMessage("<<heartbeat>> Reconnect MQ - idle timeout exceeded.");
                    if (mqCommons.cmqFactory  != null) mqCommons.CloseConnection();
                    if (pmqCommons.pmqFactory != null) pmqCommons.CloseConnection();
                    Hop.Restart();
                    uCommons.uSendMessage("Restarting this responder now.");
                    loopCtl    = false;   // restart the responder
                    IamWaiting.set(false);   // restart the responder
                    restart = true;
                    safe2restart = true;
                    lastHB = System.nanoTime();
                    restartActive.set(false);
                    uCommons.uSendMessage("<<heartbeat>> service is listening again.");
                }

                if (!IamWaiting.get()) break;

                // ----------------------------------------
                // Kafka Flush Service
                // ----------------------------------------
                if (uTask.equals("025")) kProducer.kMontior();

                // ----------------------------------------
                // Restart Countdown Service
                // ----------------------------------------
                if (countDown >= (NamedCommon.mqHeartBeat) && loopCtl) {
                    if (!NamedCommon.isNRT) {
                        msg = heartbeat;
                        timeLeft = uCommons.RightHash(String.valueOf(NamedCommon.mqWait - mqIdle), rHash);
                        msg += mqcountdown.replace("$$", timeLeft);
                        if (NamedCommon.sConnected || NamedCommon.tConnected) {
                            timeLeft = uCommons.RightHash(String.valueOf(NamedCommon.dbWait - dbIdle), rHash);
                            msg += dbcountdown.replace("##", timeLeft);
                        } else {
                            msg += " DB's are disconnected.";
                        }
                        uCommons.uSendMessage(msg);
                    } else {
                        uCommons.uSendMessage("<<heartbeat>> ["+NamedCommon.inputQueue+"] process is waiting.");
                        noActionCtr++;
                        if (noActionCtr >= maxNoAction) {
                            stopNow = true;
                            IamWaiting.set(false);
                            restartActive.set(false);
                            safe2restart = true;
                            break;
                        }
                    }
                    lastHB = System.nanoTime();
                }

                GarbageCollector.CleanUp();
                stopNow = coreCommons.StopNow();
                if (stopNow) uCommons.StopProcessNow();

                if (countDown > 3) heavyUseCtr = 0;

                if (NamedCommon.sConnected) {
                    if (!u2Commons.TestAlive()) {
                        if (IamWaiting.get()) SourceDB.ReconnectService();
                    }
                }
                if (NamedCommon.uCon != null) {
                    try {
                        if (!NamedCommon.uCon.isValid(2)) {
                            if (IamWaiting.get()) SqlCommands.ReconnectService();
                        }
                    } catch (SQLException e) {uCommons.uSendMessage(e.getMessage());}
                }

            }
            rightNow = System.nanoTime();
            laps = (rightNow - lastGC) / div;
            if (laps > 120) {
                restartActive.set(true);
                GarbageCollector.CleanUp();  // make SURE we don't flood OS with garbage collects
                lastGC = rightNow;
                restartActive.set(false);
            }
        }

    }

    private static void PrepareKafka() {
        if (!kProducer.isKafkaReady()) {
            String privID = NamedCommon.pid;
            if (NamedCommon.isDocker) privID = NamedCommon.hostname;
            Properties kProps = uCommons.LoadProperties(conf + NamedCommon.kafkaBase);
            kProducer.SetBroker(kProps.getProperty("brokers"));
            kProducer.SetTopic(kProps.getProperty("topic"));
            kProducer.SetSASL(false, "", "", "");
            kProducer.SetGroup(kProps.getProperty("group"));
            kProducer.SetClientID(kProps.getProperty("clientid")+"-"+privID);
            int linger=0, batchsize=0;
            try {
                linger = Integer.valueOf(kProps.getProperty("linger"));
            } catch (NumberFormatException nfe) {
                linger = 0;
            }
            try {
                batchsize = Integer.valueOf(kProps.getProperty("batch"));
            } catch (NumberFormatException nfe) {
                batchsize = 0;
            }
            kProducer.SetLinger(linger);
            kProducer.SetTransID(0);
            kProducer.SetBatchSize(batchsize);
            uCommons.uSendMessage("Connecting to Kafka:");
            uCommons.uSendMessage("             config: " + NamedCommon.kafkaBase);
            uCommons.uSendMessage("            Brokers: " + kProps.getProperty("brokers"));
            uCommons.uSendMessage("              Topic: " + kProps.getProperty("topic"));
            uCommons.uSendMessage("              Group: " + kProps.getProperty("group"));
            uCommons.uSendMessage("             Client: " + kProps.getProperty("clientid") + "-" + privID);

            // --------------------------------------------------------------------------
            // Could do this in a self-healing way :-
            // okay = false; while(!okay) { kProducer.kConnect(); ans=kProducer.isKafkaReady(); if (!okay) sleep 5; }
            // BUT - it could sit there endlessly and messages will build up then crash AMQ
            // --------------------------------------------------------------------------
            kProducer.kConnect();
            if (!kProducer.isKafkaReady()) {
                uCommons.uSendMessage("Cannot establish a connection with Kafka - stopping.");
                System.exit(1);
            }
            NamedCommon.kafkaAction = kProps.getProperty("action");   // how to handle the inbound data then update kafka
            kProps = null;
        }

    }

    public void onMessage(Message message) {
        IamWaiting.set(false);
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";
        if (restartActive.get()) {
            int restartCtr = 0;
            while (restartActive.get()) {
                restartCtr++;
                uCommons.Sleep(1);
                if (restartCtr > 10) {
                    uCommons.uSendMessage("... rFuel responder is waiting on a restart service ...");
                    restartCtr = 0;
                }
            }
        }
        ResetMemory();
        System.out.println(" ");
        uCommons.uSendMessage(msgIn);
        lastMessage = System.nanoTime();
        mqActive   = lastMessage;
        dbActive   = lastMessage;

        if (message instanceof TextMessage) {okay=true;} else {okay=false; }
        if (!okay) {
            uCommons.uSendMessage("Faulty message received. Probaly noise, ignoring it.");
            System.out.println(" ");
            return;
        }

        inMsg = (TextMessage) message;
        if (inMsg == null) return;
        try {
            TheMessage = inMsg.getText();
            if (!NamedCommon.artemis) {
                uCommons.uSendMessage("Message ID: " + message.getJMSMessageID());
            }
        } catch (JMSException e) {
            uCommons.uSendMessage("Invalid message (datatype) received.");
            e.getMessage();
            return;
        }

        int mChk = TheMessage.length();
        if (mChk < 1) return;
        if (TheMessage.contains(ok200)) return;

        TheMessage = TheMessage.replaceAll("\\r?\\n", "");
        APImsg.instantiate();
        uCommons.MessageToAPI(TheMessage);

        if (APImsg.APIget("ping").toLowerCase().equals("true")) isPing=true;

        if (NamedCommon.ZERROR) {
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage = "";
            if (APImsg.APIget("task").startsWith("05")) {
                String err = "422", fmt = APImsg.APIget("FORMAT").toUpperCase();
                ReplyTo = APImsg.APIget("replyto");
                CorrelID = APImsg.APIget("correlationid");
                String descr = NamedCommon.ReturnCodes.get(Integer.valueOf(err));
                if (descr.equals("")) descr = "ReturnCode [" + err + "] not found.";
                String errMsg = "{\"body\":{" +
                        "\"response\":\"Bad Paramter\"," +
                        "\"message\":\"" + descr + "\"," +
                        "\"status\":" + err +
                        "}}";
                SendResponse(errMsg, ReplyTo);
                return;
            } else {
                FinaliseMessage(message);
                return;
            }
        }

        if (NamedCommon.isWhse) {
            if (NamedCommon.RunType.equals("REFRESH")) {
                NamedCommon.aStep = NamedCommon.rStep;
            } else {
                NamedCommon.aStep = NamedCommon.iStep;
            }
        }

        NamedCommon.isWebs = false;
        if (!APImsg.APIget("vque").equals("")) NamedCommon.isWebs = true;

        try {
            CorrelID = null;
            ReplyTo = null;
            MessageID = null;

            MessageID = message.getJMSMessageID();
            if (MessageID == null) MessageID = NamedCommon.pid;
            if (MessageID.equals("")) MessageID = NamedCommon.pid;
            if (MessageID.equals(NamedCommon.pid)) {
                MessageID += "_" + nextMsg;
                nextMsg++;
            }

            CorrelID = String.valueOf(message.getJMSCorrelationID());
            if (CorrelID == null)        CorrelID = APImsg.APIget("correlationid");
            if (CorrelID.equals("null")) CorrelID = APImsg.APIget("correlationid");
            if (CorrelID.equals(""))     CorrelID = APImsg.APIget("correlationid");

            ReplyTo = String.valueOf(message.getJMSReplyTo());
            if (ReplyTo == null)         ReplyTo = APImsg.APIget("replyto");
            if (ReplyTo.equals("null"))  ReplyTo = APImsg.APIget("replyto");
            if (ReplyTo.equals(""))      ReplyTo = APImsg.APIget("replyto");
            if (ReplyTo.startsWith("queue://")) ReplyTo = ReplyTo.substring(8, ReplyTo.length());

        } catch (JMSException e) {
            uCommons.uSendMessage("***");
            uCommons.uSendMessage("*** JMS Error: " + e.getMessage());
            uCommons.uSendMessage("***");
            ReplyTo = APImsg.APIget("replyto");
            CorrelID = APImsg.APIget("correlationid");
        }

        if (ReplyTo.startsWith("temp")) {
            int pos = ReplyTo.indexOf("/") + 2;
            ReplyTo = ReplyTo.substring(pos, ReplyTo.length());
        }

        if (CorrelID.toLowerCase().startsWith("@test")) {
            CorrelID += "_" + nextTest;
            nextTest++;
        }

        if ((ReplyTo == null || ReplyTo.equals("") || ReplyTo.equals("null")) && NamedCommon.replyReqd) {
            uCommons.uSendMessage("[0921] Auto-Defaulted Queue to " + defaultQ);
            APImsg.APIset("replyto", defaultQ);
            ReplyTo = defaultQ;
        }

        String note = "";
        if (ReplyTo.startsWith("ID:")) note = " (\"ID:\" is a temp-queue://)";

        if (CorrelID.contains("%%")) {
            NamedCommon.msgCt++;
            CorrelID = CorrelID.replaceAll("\\%\\%", String.valueOf(NamedCommon.msgCt));
        }
        APImsg.APIset("replyto", ReplyTo);
        APImsg.APIset("correlationid", CorrelID);
        NamedCommon.reply2Q = ReplyTo;
        NamedCommon.CorrelationID = CorrelID;

        uCommons.uSendMessage("CorrelartionID: " + NamedCommon.CorrelationID);
        uCommons.uSendMessage("       ReplyTo: " + NamedCommon.reply2Q + note);
        uCommons.uSendMessage("     Data Acct: " + NamedCommon.datAct);

        if (NamedCommon.isWhse && !NamedCommon.sConnected) SourceDB.ConnectSourceDB();

        if (!NamedCommon.isNRT) {
            if (!APImsg.APIget("task").startsWith("9")) {
                uCommons.eMessage = "";
                if (APImsg.APIget("correlationid").equals("")) uCommons.eMessage += "No CorrelationID in the message!  ";
                if (APImsg.APIget("replyto").equals("")) uCommons.eMessage += "No ReplyTo in the message!  ";
                if (APImsg.APIget("task").equals("")) uCommons.eMessage += "No task in the message!  ";
                if (NamedCommon.dbhost.equals("") && NamedCommon.protocol.equals("u2cs")) uCommons.eMessage += "No sourceDB host in the message!";
                if (!uCommons.eMessage.equals("")) {
                    if (!isPing) uCommons.uSendMessage("[0921] " + uCommons.eMessage);
                }
            }
        }

        String[] decoder = new String[20];
        for (int dc = 0; dc < 20; dc++) { decoder[dc] = ""; }
        String tname = CorrelID;
        String mscat = "";
        String esbFMT = "";
        String payload = "";
        String mscDir = "";
        decoder[0] = uTask;
        decoder[1] = NamedCommon.Broker;
        decoder[2] = qNbr;
        decoder[3] = NamedCommon.inputQueue;
        decoder[4] = mscat;
        decoder[5] = payload;
        decoder[6] = tname;
        decoder[7] = esbFMT;
        decoder[8] = mscDir;

        if (NamedCommon.isWhse) {
            MessageID = "uBulk:" + NamedCommon.pid+":"+nextTest;
            nextTest++;
            NamedCommon.MessageID = MessageID;
            APImsg.APIset("MESSAGE", NamedCommon.MessageID);
        }

        if (isPing) {
            SendResponse("{\"message\":\"alive\"}", NamedCommon.reply2Q);
        } else {
            if (uTask.startsWith("05") && NamedCommon.threader) {
                ThreadManager.begin(decoder, TheMessage);
            } else {
                HandleMessage(decoder, TheMessage);
            }
        }
        decoder = null;

        /* ------------------------------------------- */
        /* At this point, AMQ has consumed the message */
        /* ------------------------------------------- */

        if (NamedCommon.ConnectionError && NamedCommon.ErrorStop) {
            uCommons.uSendMessage("*");
            uCommons.uSendMessage("*");
            uCommons.uSendMessage("*");
            uCommons.uSendMessage("-----------------------------------------------------");
            uCommons.uSendMessage("    Connection Error(s) Experienced.                 ");
            uCommons.uSendMessage("               Stopping Now                          ");
            uCommons.uSendMessage("        Will restart automatically.                  ");
            uCommons.uSendMessage("-----------------------------------------------------");
            if (!NamedCommon.Broker.equals("") && !NamedCommon.AlertQ.equals("")) {
                Hop.start("SourceDB connection errors experienced.", "",
                        uCommons.GetNextBkr(NamedCommon.Broker),
                        NamedCommon.AlertQ, "", "RPC-Issues");
            }
            System.exit(0);
        }

        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";
        FinaliseMessage(message);

        NamedCommon.masterStop = coreCommons.StopNow();
        if (NamedCommon.masterStop) uCommons.StopProcessNow();

        heavyUseCtr++;      // this is set back to 0 in WaitForMessage
        if (heavyUseCtr > NamedCommon.heavyUseMax) uCommons.StopProcessNow();
        uCommons.uSendMessage(listeningOn);

        lastMessage = System.nanoTime();
        mqActive   = lastMessage;
        dbActive   = lastMessage;
        IamWaiting.set(true);
    }

    private void FinaliseMessage(Message message) {
        //
        // uvreset seems to destroy this at RAB - need to find out why.
        //
        activeMQ.SetMessage(message);
        activeMQ.Acknowledge();
    }

    public void ResetMemory() {
        if (NamedCommon.fmvArray != null) {
            if (NamedCommon.fmvArray.dcount() > -1) {
                int eol = NamedCommon.fmvArray.dcount();
                for (int uda = 0; uda < eol; uda++) {
                    if (NamedCommon.fmvArray.extract(0) != null) NamedCommon.fmvArray.delete(0);
                }
            }
        }
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt); // MUST clear AES vars first.
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties Props = uCommons.LoadProperties(conf + NamedCommon.Broker);
        uCommons.BkrCommons(Props);
        isPing = false;
    }

    public static void HandleMessage(String[] args, String message) {

        startM = System.nanoTime();
        String uTask = args[0];
        String qNbr = args[2];
        NamedCommon.inputQueue = args[3];
        String reply2 = "";
        String msgRespnse = "";
        NamedCommon.replyReqd = true;
        switch (uTask) {
            case "015":
                NamedCommon.replyReqd = false;
                break;
            case "022":
                NamedCommon.replyReqd = false;
                break;
            case "025":
                NamedCommon.replyReqd = false;
                break;
            case "910":
                NamedCommon.replyReqd = false;
                break;
            case "920":
                NamedCommon.replyReqd = false;
                break;
            case "999":
                NamedCommon.replyReqd = false;
                break;
        }
        boolean sendResponse = false;

        reply2 = ReplyTo;
        if (NamedCommon.replyReqd) {
            if (reply2.equals("")) reply2 = NamedCommon.reply2Q;
            if (reply2.equals("")) reply2 = APImsg.APIget("replyto");
            if (reply2.equals("")) reply2 = "RunERRORS";
            if (reply2.contains("//")) reply2 = reply2.split("//")[1];
        }

        if (!MessageID.equals("") && !MessageID.equals(NamedCommon.MessageID)) {
            NamedCommon.MessageID = MessageID;
        } else {
            MessageID = NamedCommon.MessageID;
        }

        if (NamedCommon.debugging) uCommons.uSendMessage("       Message ID = " + MessageID);

        String replyText = "";

        replyText += TheMessage;
        replyText = replyText.replaceAll("body=", "");

        msgRespnse = MessageProtocol.handleProtocolMessage(uTask, qNbr, replyText);

        msgRespnse = msgRespnse.replaceAll("\\<\\<", "");
        msgRespnse = msgRespnse.replaceAll("\\>\\>", "");

        while (msgRespnse.indexOf("<nl>") > -1) msgRespnse = msgRespnse.replace("<nl>", "\r\n");

        if (NamedCommon.debugging) uCommons.uSendMessage("Response: " + msgRespnse);

        if (uTask.equals("055") && !msgRespnse.equals("")) {
            // Test: 21-04-2025 -- the finish message is being sent twice !!
//            sendResponse = true;
//        } else {
            sendResponse = (NamedCommon.replyReqd || reply2.equals("RunERRORS"));
            sendResponse = (sendResponse && !msgRespnse.equals(""));
            sendResponse = (sendResponse && !reply2.equals("") && !reply2.equals("null"));
        }

        if (!NamedCommon.isNRT) {
            if (sendResponse) {
                SendResponse(msgRespnse, reply2);
            }
        }

        GarbageCollector.CleanUp();
        lastM  = System.nanoTime();
        if (!NamedCommon.ZERROR) {
            uCommons.uSendMessage("rFuel has completed instruction for " + NamedCommon.xMap);
        } else {
            uCommons.uSendMessage("rFuel found errors in the instruction and did not process it.");
            System.out.println("");
            System.out.println(replyText);
            System.out.println("");
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage = "";
        }
        laps = (lastM - startM) / div;
        if (!NamedCommon.isNRT) uCommons.uSendMessage("Messaged handled in "+laps+" seconds");
        if (NamedCommon.ConnectionError) NamedCommon.ConnectionError = false;
    }

    private static void SendResponse(String msgRespnse, String reply2) {
        String nextBrk = uCommons.GetNextBkr(NamedCommon.Broker);

        boolean isTempQ = false;
        if (reply2.startsWith("temp")) isTempQ = true;
        if (reply2.startsWith("ID:"))  isTempQ = true;

        if (isTempQ) {
            if (NamedCommon.debugging) {
                uCommons.uSendMessage("*************************************************************");
                uCommons.uSendMessage("HandleMessage.completed(*)");
                uCommons.uSendMessage("  (ACK)   ReplyTo = " + reply2);
                uCommons.uSendMessage("        BrokerURL = " + nextBrk);
                uCommons.uSendMessage("   Correlation ID = " + NamedCommon.CorrelationID);
                uCommons.uSendMessage("*************************************************************");
            }
            Hop.start(msgRespnse, "", nextBrk, reply2, "ACK", NamedCommon.CorrelationID);
        } else {
            if (NamedCommon.debugging) {
                uCommons.uSendMessage("*************************************************************");
                uCommons.uSendMessage("HandleMessage.completed(*)");
                uCommons.uSendMessage("          ReplyTo = " + reply2);
                uCommons.uSendMessage("        BrokerURL = " + nextBrk);
                uCommons.uSendMessage("   Correlation ID = " + CorrelID);
                uCommons.uSendMessage("*************************************************************");
                uCommons.uSendMessage("Responder.Replyto() " + reply2);
            }
            if (NamedCommon.debugging) uCommons.uSendMessage("Responder.Replyto() " + reply2);
            Hop.start(msgRespnse, "", nextBrk, reply2, "", CorrelID);
        }

        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("***");
            uCommons.uSendMessage("*** " + NamedCommon.Zmessage);
            uCommons.uSendMessage("***");
        }
    }

}
