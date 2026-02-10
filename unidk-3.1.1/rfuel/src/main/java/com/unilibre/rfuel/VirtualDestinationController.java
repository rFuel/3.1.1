package com.unilibre.rfuel;

import com.unilibre.commons.GarbageCollector;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.msgCommons;
import com.unilibre.commons.uCommons;
import com.unilibre.core.coreCommons;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class VirtualDestinationController {

    private String brokerURI = "";
    private String brokerUsr = "";
    private String brokerPwd = "";
    private String topic = "";
    private String qName = "";
    private String countdown = "<<heartbeat>> restart uSubscriber in $$ seconds.";
    private int rHash = String.valueOf(NamedCommon.mqWait).length() + 1;
    public int hbCnt=1;
    private long rightNow, lastHB = System.nanoTime(), lastPing = System.nanoTime();
    public long lastAction;
    private double laps, div = 1000000000.00;
    private int vListeners = 0, maxProc = 0;
    private boolean stopSW = false, showBEAT = true, restart=false;
    private ActiveMQConnectionFactory connectionFactory;
    private Connection receiverConnection;
    private Session receiverSession;
    private ArrayList<MessageConsumer> consumers = new ArrayList<>();
    private ArrayList<VirtualMessageListener> listeners = new ArrayList<>();
    private ArrayList<Integer> nbrMessages = new ArrayList<>();
    private CountDownLatch done;

    public void SetBroker(String usr, String pwd, String uri, int cons) {
        this.brokerUsr = usr;
        this.brokerPwd = pwd;
        this.brokerURI = uri;
        this.vListeners = cons;
        NamedCommon.bkr_user = usr;
        NamedCommon.bkr_pword= pwd;
    }

    public void SetMaxProc(int max) {
        maxProc = max;
    }

    public void SetLatch(int latch) {
        done = new CountDownLatch(latch);
    }

    public void SetStop(boolean stop) {
        stopSW = stop;
    }

    public boolean GetRestart() {
        if (!restart) restart = coreCommons.StopNow();
        return restart;
    }

    public void before(String topic) throws Exception {

        // Create MQ listeners in the virtual topic

        uCommons.uSendMessage("Connecting with MQ Broker:");
        uCommons.uSendMessage("  URL [ " + brokerURI + "]");
        uCommons.uSendMessage(" User [" + brokerUsr + "]");

        boolean keepTying=true;
        int failedTries=0;
        while (keepTying) {
            try {
                this.topic = topic;
                connectionFactory = new ActiveMQConnectionFactory(brokerURI);
                connectionFactory.setUserName(brokerUsr);
                connectionFactory.setPassword(brokerPwd);
                receiverConnection = connectionFactory.createConnection();
                receiverConnection.setClientID(NamedCommon.pid + ":vt-" + topic + ":cons");
                receiverSession = receiverConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                receiverConnection.start();
                // ****************************************************************************
                this.consumers.clear();
                this.listeners.clear();
                this.nbrMessages.clear();
                this.qName = "Consumer.4.VirtualTopic." + this.topic;
                Queue consQue = receiverSession.createQueue(qName);
                int lx = String.valueOf(this.done.getCount()).length();
                int thisOne;
                for (int qc = 1; qc <= this.vListeners; qc++) {
                    thisOne = consumers.size();
                    consumers.add(receiverSession.createConsumer(consQue));
                    listeners.add(new VirtualMessageListener(done));
                    listeners.add(new VirtualMessageListener(done));
                    listeners.get(thisOne).setBroker(this.brokerURI, "vt-" + this.topic);
                    listeners.get(thisOne).setMaxProcessed(maxProc);
                    consumers.get(thisOne).setMessageListener(listeners.get(thisOne));
                    nbrMessages.add(0);
                    if (uSubscriber.firstTime) uCommons.uSendMessage("   > " + qName + " # " + uCommons.RightHash(String.valueOf(qc), lx) + " is listening");
                }
                // ****************************************************************************
                keepTying=false;
                failedTries=0;
                if (NamedCommon.vtPing > 0) { DoPing(topic); }
            } catch (JMSException e) {
                uCommons.uSendMessage("...");
                uCommons.uSendMessage(e.getMessage());
                uCommons.uSendMessage("...");
                uCommons.uSendMessage("Waiting for an MQ connection. Sleep 5 seconds.");
                uCommons.uSendMessage("...");
                uCommons.Sleep(5);
                failedTries++;
                if (failedTries > 60) {
                    uCommons.uSendMessage("Giving up.");
                    System.exit(0);
                }
            }
        }
    }

    public void run() throws Exception {

        int timeout, loopCnt=0, countDown=0, pingTime=0;
        String msg = "", timeLeft = "";
        int msgCnt=0;
        if (NamedCommon.mqHeartBeat >= NamedCommon.mqWait) NamedCommon.mqHeartBeat = NamedCommon.mqWait - 2;
        stopSW = coreCommons.StopNow();
        GarbageCollector.setStart(System.nanoTime());
        GarbageCollector.CleanUp();
        while (!stopSW) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                uCommons.uSendMessage("Thread.sleep issue in WaitForReply()");
            }

            if (uSubscriber.monitor) {
                rightNow = System.nanoTime();
                laps = (rightNow - lastAction) / div;
                timeout = ((int) (laps + 0.5));

                laps = (rightNow - lastHB) / div;
                countDown = ((int) (laps + 0.5));

                laps = (rightNow - lastPing) / div;
                pingTime = ((int) (laps + 0.5));

                if (timeout > NamedCommon.mqWait) {
                    System.out.println(" ");
                    uCommons.uSendMessage(NamedCommon.block);
                    uCommons.uSendMessage("<<heartbeat>> Inactive for > " + NamedCommon.mqWait + " seconds.");
                    stopSW = true;
                    break;
                }

                if (countDown >= (NamedCommon.mqHeartBeat)) {
                    lastHB = System.nanoTime();
                    countDown = countDown * hbCnt;
                    timeLeft = uCommons.RightHash(String.valueOf(NamedCommon.mqWait - countDown), rHash);
                    msg = countdown.replace("$$", timeLeft);
                    uCommons.uSendMessage(msg);
                    hbCnt++;
                    uCommons.Sleep(2);
                }

                if (NamedCommon.vtPing > 0) {
                    if (pingTime > NamedCommon.vtPing) {
                        sendPingTest(this.topic);
                        lastPing = System.nanoTime();
                    }
                }

//                try {
//                    Thread.sleep(50);
//                } catch (InterruptedException e) {
//                    uCommons.uSendMessage("Thread.sleep issue in WaitForReply()");
//                }

                for (int qc = 1; qc <= this.vListeners; qc++) {
                    stopSW = (stopSW || listeners.get(qc - 1).getStopper());
                    if (stopSW) {
                        restart=true;
                        break;
                    }
                    msgCnt = nbrMessages.get(qc - 1);
                    if (msgCnt < listeners.get(qc - 1).getNumReceived()) {
                        msgCnt = listeners.get(qc - 1).getNumReceived();
                        nbrMessages.set((qc - 1), msgCnt);
                        lastHB = System.nanoTime();
                        lastAction = System.nanoTime();
                    }
                }
            }

            if (!stopSW) stopSW = coreCommons.StopNow();
            GarbageCollector.CleanUp();
            if (hbCnt > 10) stopSW = true;
        }
    }

    public void after() throws Exception {
        uCommons.uSendMessage("Disconnecting from MQ-URL (" + connectionFactory.getBrokerURL() + ")");

        if (uSubscriber.stopping) {
            uCommons.uSendMessage("Pre-shutdown statistics:");
            uCommons.uSendMessage("-----------------------------------------------------------------------------------");
            for (int qc = 0; qc < this.vListeners; qc++) {
                uCommons.uSendMessage("Consumer " + (qc + 1) + " processed " + listeners.get(qc).getNumReceived() + " Messages");
            }
        }

        receiverConnection.close();
        receiverSession.close();
        this.consumers.clear();
        this.listeners.clear();
        connectionFactory = null;
        receiverSession = null;
        receiverConnection = null;
    }

    public void DoPing(String topic) throws Exception {
        // I may want to use sendPingTest from other places in reFuel
        // so it may get moved to uCommons and become public
        if (NamedCommon.vtPing > 0) sendPingTest(topic);
    }

    private void sendPingTest(String topic) throws Exception {
        if (NamedCommon.vtPing == 0) return;
        uCommons.uSendMessage("Sending a ping-test to VirtualTopic." + topic);
        Connection connection = connectionFactory.createConnection();
        connection.setClientID("Sender");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createTopic("VirtualTopic." + topic);
        MessageProducer producer = session.createProducer(destination);
        TextMessage txtMessage = session.createTextMessage();
        String ping = "task<is>999<tm>correlationid<is>Ping<tm>";
        if (uSubscriber.GetFormat().toLowerCase().equals("json")) {
            txtMessage.setText(msgCommons.jsonifyMessage(ping));
        } else {
            txtMessage.setText(ping);
        }
        producer.send(txtMessage, DeliveryMode.NON_PERSISTENT, 4, NamedCommon.Expiry);
        connection.close();
    }

}