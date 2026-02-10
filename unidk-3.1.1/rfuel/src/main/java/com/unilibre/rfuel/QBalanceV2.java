package com.unilibre.rfuel;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import com.unilibre.MQConnector.artemisMQ;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.core.coreCommons;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Queue;
import javax.jms.*;
import java.util.*;

public class QBalanceV2 {

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
    private static Map<String, PriorityQueue<BufferedMessage>> phaseBuffers = new HashMap<>();

    // ----------------------------------------------------------------------------------------
    //  Phases:             012, 014, 017, etc.
    //  BufferedMessage:    JMS Message store, incorporating timestamp for FIFO
    //  phaseBuffers:       PriorityQueue of messages in dispatch queues (*_000)
    //                      Can poll this HashMap list to get the oldest message first
    //                      Then use a message filter to acknowledge (remove) that message
    //                      Then Hop that message to a worker queue.
    // ----------------------------------------------------------------------------------------

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
            boolean MainLoop = true;
            int sleeper = 0, hbeat = 0;
            waiting = true;
            while (MainLoop && !reset) {
                totJobs = 0;
                FindQueues();
                Thread.sleep(500);
                GetQueueVolumes();           // who's empty and who's not
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
                        if (bTasks.contains("012")) {
                            NamedCommon.Broker = brokers[b];
                            break;
                        }
                    }
                }
                if (NamedCommon.Broker.equals("")) {
                    uCommons.uSendMessage("<<FAIL>> - cannot find a broker.");
                    return;
                }
            } else {
                uCommons.uSendMessage("<<FAIL>> - cannot find 'this.server'");
                return;
            }
        }
        nextBrk = NamedCommon.Broker.substring(0, (NamedCommon.Broker.length() - 4));

        Properties Props = uCommons.LoadProperties(conf + NamedCommon.Broker);
        uCommons.BkrCommons(Props);
        bkrUrl = NamedCommon.messageBrokerUrl;
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
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
        phaseBuffers = new HashMap<>();
        for (String phase : TasksArray) {
            phaseBuffers.put(phase, new PriorityQueue<>(Comparator.comparingLong(b -> b.timestamp)));
        }
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

    private static void DistributeMessages() throws JMSException {
        String SendMessage, corr,  phase, qname, msgID, dispatch;
        Destination postoffice;
        MessageConsumer postman;
        boolean alldone = false;
        int nbrQueues, jobctr;
        while (!alldone) {
            nbrSent = 0;
            BackLog = 0;
            for (int aa = 0; aa < nbrTasks; aa++) {
                phase = TasksArray.get(aa);
                dispatch = dynQueues.get(aa).get(0);
                PriorityQueue<BufferedMessage> buffer = phaseBuffers.get(phase);

                if (buffer == null || buffer.isEmpty()) continue;

                nbrQueues = dynQueues.get(aa).size();

                for (int qq = 1; qq < nbrQueues; qq++) {  // skip [0] dispatcher queue
                    qname = dynQueues.get(aa).get(qq);
                    jobctr = qVolumes.get(aa).get(qq);

                    if (jobctr == 0 && !buffer.isEmpty()) {
                        BufferedMessage next = buffer.poll();  // oldest message

                        corr = next.corr;
                        if (corr == null || corr.isEmpty()) {
                            corr = "Automated-" + phase;  // fallback ID
                        }

                        Hop.start(next.payload, "", nextBrk, qname, "", corr);

                        qVolumes.get(aa).set(qq, 1);
                        qVolumes.get(aa).set(0, qVolumes.get(aa).get(0) - 1);  // reduce backlog

                        Destination sourceQueue = session.createQueue(dispatch);
                        msgID = next.message.getJMSMessageID();
                        MessageConsumer consumer = session.createConsumer(sourceQueue, "JMSMessageID = '" + msgID + "'");
                        Message msgToAck = consumer.receive(1000);  // 1 second timeout may not be enough
                        consumer.close();

                        if (!NamedCommon.ZERROR) {
                            if (msgToAck != null) msgToAck.acknowledge();
                        } else {
                            if (mqCommons.cmqSession != null) {
                                try {
                                    mqCommons.cmqSession.recover();
                                } catch (JMSException e) {
                                    uCommons.uSendMessage("Recover error: " + e.getMessage());
                                }
                            }
                        }

                        nbrSent++;
                        BackLog--;
                        break;  // one message per queue at a time
                    }
                }

            }
            if (BackLog < 1) alldone = true;     // no messages enqueued
            if (nbrSent < 1) alldone = true;     // all queues are busy
        }
    }

    private static void GetQueueVolumes() {
        int cons = 0, jobctr = 0;
        totJobs = 0;
        String qname = "";
        Queue queue;
        QueueBrowser qBrowser;
        Enumeration e;
        for (int aa = 0; aa < nbrTasks; aa++) {
            try {
                int nbrQueues = dynQueues.get(aa).size();
                for (int qq = 0; qq < nbrQueues; qq++) {
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
                        long tstamp = message.getJMSTimestamp();
                        String phase = TasksArray.get(aa);

                        BufferedMessage buf = new BufferedMessage(message, message.getJMSTimestamp(), qname, phase);
                        phaseBuffers.get(phase).add(buf);  // Add to phase-specific PriorityQueue

                        jobctr++;
                        if (qq == 0) totJobs++;
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

