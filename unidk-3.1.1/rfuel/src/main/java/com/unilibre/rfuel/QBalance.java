package com.unilibre.rfuel;

/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.artemisMQ;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.core.coreCommons;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

public class QBalance {

    private static ArrayList<String> TasksArray = new ArrayList<>();
    private static ArrayList<String> qNameArray = new ArrayList<>();
    private static ArrayList<String> nbrQsArray = new ArrayList<>();
    private static String bkrUrl, taskOffice;
    private static int mCtr, nbrTasks, totJobs = 0, nbrSent, BackLog;
    private static String nextBrk = "";
    private static Connection connection;
    private static Session session;
    private static boolean reset = false, waiting = true;
    private static ArrayList<ArrayList<String>> dynQueues = new ArrayList<ArrayList<String>>();
    private static ArrayList<ArrayList<Integer>> qVolumes = new ArrayList<ArrayList<Integer>>();
    private static ArrayList<ArrayList<QueueBrowser>> qBrowsers = new ArrayList<ArrayList<QueueBrowser>>();
    private static ArrayList<ArrayList<Enumeration>> qEnums = new ArrayList<ArrayList<Enumeration>>();
    private static ArrayList<ArrayList<Queue>> qSessions = new ArrayList<ArrayList<Queue>>();
    private static long startM, finishM, lastActive, rightNow;
    private static double laps, div = 1000000000.00;

    public static void main (String[] args) throws Exception {
        uCommons.uSendMessage("Start Q-Balancing");
        NamedCommon.hostname = "";
        NamedCommon.inputQueue = "qBalancer";
        NamedCommon.AutoTests = true;       // Stops it trying to connect to SQL
        boolean StopMaster = coreCommons.StopNow();
        startM = System.nanoTime();
        lastActive = startM;
        Initialise();
        GarbageCollector.setStart(System.nanoTime());
        while (!StopMaster) {
            reset = false;
            //Initialise();
            boolean MainLoop = true;
            int sleeper = 0, hbeat = 0;
            waiting = true;
            while (MainLoop && !reset) {
                totJobs = 0;
                FindQueues();
                Thread.sleep(500);
                CheckMessageQueues();           // who's empty and who's not
                if (!reset) {
                    if (totJobs > 0) {
                        DistributeMessages();
                        sleeper = 0;
                        hbeat = 0;
                        waiting = true;
                        lastActive = System.nanoTime();
                    } else {
                        rightNow = System.nanoTime();
                        laps = (rightNow - lastActive) / div;
                        if (laps > NamedCommon.mqWait) {
                            uCommons.uSendMessage("<<heartbeat>> MQ idle timeout. Close and reconnect.");
                            CloseSession();
                            Initialise();
                            lastActive = System.nanoTime();
                        }
                        sleeper++;
                        if (sleeper > 5) {
                            Thread.sleep(5000);
                            sleeper = 0;
                            hbeat++;
                        }
                        if (hbeat > 50) {
                            if (waiting) uCommons.uSendMessage("heartbeat ... waiting for messages");
                            Thread.sleep(10000);
                            sleeper = 0;
                            hbeat = 0;
                            waiting = false;
                        }
                    }
                    StopMaster = coreCommons.StopNow();
                    if (StopMaster) break;
                    GarbageCollector.CleanUp();
                }
            }
        }
    }

    private static void Initialise() {
        if (System.getProperty("user.dir").contains("/home/andy")) {
            NamedCommon.upl = NamedCommon.DevCentre;
            NamedCommon.gmods = NamedCommon.upl + "lib\\";
            NamedCommon.BaseCamp = NamedCommon.upl;
        }
        NamedCommon.slash = "/";
        String basecamp = NamedCommon.BaseCamp + "/";
        String conf = basecamp + "conf/";
        String data = basecamp + "data/";
        String maps = basecamp + "maps/";
        String templates = basecamp + "templates/";
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);

        if (NamedCommon.Broker.equals("")) {
            Properties sProps = uCommons.LoadProperties("this.server");
            if (sProps.size() > 0) {
                String[] brokers = sProps.getProperty("brokers", "").split(",");
                for (int b = 0; b < brokers.length; b++) {
                    if (brokers[b].length() > 0) {
                        uCommons.uSendMessage("Checking : " + brokers[b]);
                        Properties bProps = uCommons.LoadProperties(brokers[b]);
                        String bTasks = bProps.getProperty("tasks");
                        if (bTasks.contains("012") || bTasks.contains("014")) {
                            NamedCommon.Broker = brokers[b];
                            break;
                        }
                    }
                }
                if (NamedCommon.Broker.equals("")) {
                    uCommons.uSendMessage("<<FAIL>> in responder.respond() - cannot find a broker.");
                    return;
                }
            } else {
                uCommons.uSendMessage("<<FAIL>> in responder.respond() - cannot find 'this.server'");
                return;
            }
        }

        // BUG !!! field of "." for 1   !!!
        nextBrk = NamedCommon.Broker.substring(0, (NamedCommon.Broker.length() - 4));

        Properties Props = uCommons.LoadProperties(conf + NamedCommon.Broker);
        uCommons.BkrCommons(Props);
        bkrUrl = NamedCommon.messageBrokerUrl; // Props.getProperty("url", "");
        TasksArray = new ArrayList<>(Arrays.asList(Props.getProperty("tasks").split("\\,")));
        qNameArray = new ArrayList<>(Arrays.asList(Props.getProperty("qname").split("\\,")));
        nbrQsArray = new ArrayList<>(Arrays.asList(Props.getProperty("responders").split("\\,")));
        nbrTasks = TasksArray.size();
        int nbrQueues = 0, jobctr = 0;
        for (int i = 0; i < nbrTasks; i++) {
            if (TasksArray.get(i).equals("010")) {
                TasksArray.remove(i);
                qNameArray.remove(i);
                nbrQsArray.remove(i);
                nbrTasks--;
                break;
            }
        }
        CreateSession();
    }

    private static void CloseSession() {
        if (!NamedCommon.artemis) {
            mqCommons.CloseConnection();
            pmqCommons.CloseConnection();
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        } else {
            artemisMQ.CloseConsumer();
        }
    }

    private static void CreateSession() {
        try {
            if (!NamedCommon.artemis) {
                ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(bkrUrl);
                connection = connectionFactory.createConnection();
                connection.setClientID("qBalancer");
                connection.start();
//                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                session = connection.createSession(false, NamedCommon.MQackMode);
            } else {
                String url = bkrUrl;
                String usr = NamedCommon.bkr_user;
                String pwd = NamedCommon.bkr_pword;
                String name= "qBalancer:"+NamedCommon.pid;
                String que = NamedCommon.inputQueue;
                try {
                    session = artemisMQ.getQBsession(url, usr, pwd, name, que);
                } catch (JMSException e) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "com.unilibre.core.responder.setupMessageQueueConsumer ERROR:";
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    uCommons.uSendMessage(e.getMessage());
                    return;
                }
            }
            uCommons.uSendMessage("The session is open on Broker " + bkrUrl);
        } catch (JMSException e) {
            uCommons.uSendMessage("ABORT ");
            uCommons.uSendMessage(e.getMessage());
            System.exit(0);
        }
    }

    private static void FindQueues() {
        qVolumes.clear();
        dynQueues.clear();
        qBrowsers.clear();
        qEnums.clear();
        qSessions.clear();

        for (int aa = 0; aa < nbrTasks; aa++) {
            int nbrQueues = Integer.valueOf(nbrQsArray.get(aa));
            ArrayList<Integer> nbrjobs = new ArrayList<>();
            ArrayList<String> taskGrp = new ArrayList<>();
            ArrayList<QueueBrowser> junkBrowser = new ArrayList<>();
            ArrayList<Enumeration> junkEnum = new ArrayList<>();
            ArrayList<Queue> junkSess = new ArrayList<>();
            qVolumes.add(aa, nbrjobs);
            dynQueues.add(aa, taskGrp);
            qBrowsers.add(aa, junkBrowser);
            qEnums.add(aa, junkEnum);
            qSessions.add(aa, junkSess);
            for (int qq = 0; qq <= nbrQueues; qq++) {
                String tmp1 = uCommons.RightHash("000" + qq, 3);
                String qname = qNameArray.get(aa) + "_" + tmp1;
                taskGrp.add(qq, qname);
                nbrjobs.add(qq, 0);
                junkBrowser.add(qq, null);
                junkEnum.add(qq, null);
                junkSess.add(qq, null);
            }
        }
    }

    private static void DistributeMessages() {
        String SendMessage = "", corr = "";
        Destination postoffice;
        MessageConsumer postman;
        boolean alldone = false;
        while (!alldone) {
            nbrSent = 0;
            BackLog = 0;
            for (int aa = 0; aa < nbrTasks; aa++) {
                taskOffice = dynQueues.get(aa).get(0);
                mCtr = qVolumes.get(aa).get(0);         // jobs to distribute
                BackLog += mCtr;
                if (mCtr > 0) {
                    try {
                        if (!NamedCommon.artemis) {
                            postoffice = qSessions.get(aa).get(0);
                            postman = session.createConsumer(postoffice);
                        } else {
                            String theQ = dynQueues.get(aa).get(0);
                            postman = artemisMQ.getQBConnection(theQ);
                        }
                    } catch (JMSException e) {
                        uCommons.uSendMessage("ERROR: cannot createConsumer() ");
                        uCommons.uSendMessage(e.getMessage());
                        reset = true;
                        break;
                    }
                    int nbrQueues = dynQueues.get(aa).size();
                    for (int qq = 1; qq < nbrQueues; qq++) {
                        String qname = dynQueues.get(aa).get(qq);
                        int jobctr = qVolumes.get(aa).get(qq);
                        if (jobctr == 0) {
                            uCommons.uSendMessage(taskOffice + "  " + mCtr);
                            Message message = null;
                            try {
                                message = postman.receive(1000);
                                if (message == null) continue;
                            } catch (JMSException e) {
                                uCommons.uSendMessage(e.getMessage());
                                reset = true;
                                break;
                            }
                            if (message instanceof TextMessage) {
                                TextMessage textMessage = (TextMessage) message;
                                try {
                                    SendMessage = textMessage.getText();
                                    corr = textMessage.getJMSCorrelationID();
                                    if (corr == null) corr = "";
                                } catch (JMSException e) {
                                    uCommons.uSendMessage(e.getMessage());
                                    reset = true;
                                    break;
                                }
                            } else {
                                SendMessage = message.toString();
                                try {
                                    corr = message.getJMSCorrelationID();
                                } catch (JMSException e) {
                                    uCommons.uSendMessage(e.getMessage());
                                    reset = true;
                                    break;
                                }
                            }

                            APImsg.instantiate();
                            uCommons.MessageToAPI(SendMessage);

                            if (corr.equals("")) corr = APImsg.APIget("correlationID");

                            if (corr == null) {
                                if (qname.startsWith("02")) {
                                    corr = "Automated-Delta-Stream";
                                } else {
                                    corr = "CorrelationID unknown";
                                }
                            }

                            activeMQ.SetMessage(message);
                            activeMQ.Acknowledge();

                            Hop.start(SendMessage, "", nextBrk, qname, "", corr);

                            finishM = System.nanoTime();
                            laps = (finishM - startM) / div;
//                            uCommons.uSendMessage("Time between this and the last message: "+laps);
                            startM = System.nanoTime();
                            qVolumes.get(aa).set(qq, 1);
                            nbrSent++;
                            mCtr--;
                            BackLog--;
                            qVolumes.get(aa).set(0, mCtr);
                            if (mCtr <= 0) break;
                        }
                    }
                    try {
                        postman.close();
                    } catch (JMSException e) {
                        uCommons.uSendMessage(e.getMessage());
                        reset = true;
                    }
                }
            }
            if (BackLog < 1) alldone = true;     // no messages enqueued
            if (nbrSent < 1) alldone = true;     // all queues are busy
        }
    }

    private static void CheckMessageQueues() {
        int cons = 0, jobctr = 0;
        totJobs = 0;
        String qname = "";
        Queue queue;
        QueueBrowser qBrowser;
        Enumeration e;
        //restartQs = new ArrayList<ArrayList<String>>();
        for (int aa = 0; aa < nbrTasks; aa++) {
            // ArrayList<String> restart = new ArrayList<String>();
            // restartQs.add(aa, restart);
            try {
                int nbrQueues = dynQueues.get(aa).size();
                for (int qq = 0; qq < nbrQueues; qq++) {
                    //restart.add("?");
                    qname = dynQueues.get(aa).get(qq);
                    queue = qSessions.get(aa).get(qq);
                    if (queue == null) {
                        queue = session.createQueue(qname);
                        qSessions.get(aa).set(qq, queue);
                    }

                    qBrowser = qBrowsers.get(aa).get(qq);
                    if (qBrowser == null) {
                        qBrowser = session.createBrowser(queue);
                        qBrowsers.get(aa).set(qq, qBrowser);
                    }

                    e = qEnums.get(aa).get(qq);
                    if (e == null) {
                        e = qBrowser.getEnumeration();
                        qEnums.get(aa).set(qq, e);
                    }
                    jobctr = 0;
                    while (e.hasMoreElements()) {
                        TextMessage message = (TextMessage) e.nextElement();

                        jobctr++;
                        if (qq == 0) totJobs++;

                        //check if the listener is alive or dead
                        //try {
                        //    cons = message.getIntProperty("consumerCount");
                        //} catch (NumberFormatException nfe) {
                        //    cons = 0;
                        //}
                        //if (cons == 0 && !restart.get(qq).equals("Y")) restart.set(qq, "Y");

                    }
                    qVolumes.get(aa).set(qq, jobctr);
                }
            } catch (JMSException jmse) {
                uCommons.uSendMessage(jmse.getMessage());
                reset = true;
                try {
                    session.close();
                    session = null;
                } catch (JMSException e1) {
                    // who cares? We're leaving !
                }
                CreateSession();
            }
        }
    }

}
