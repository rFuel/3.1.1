package com.unilibre.http2jms;


import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;

public class JMSProducer {

    private String threadName = "";
    public String clID= "";
    private ActiveMQConnectionFactory pmqFactory = null;
    private Connection pmqConnection = null;
    private Session pmqSession = null;
    private MessageProducer producer = null;
    private String lastQueue="";
    private int anHour = 1000 * 60 * 60;
    private long ttl;
    private boolean ZERROR=false;
    private String Zmessage = "";
    private String MQurl    = "";
    private String sendQue  = "";
    private String bkr_user = "";
    private String bkr_pword= "";

    public void shutdown() {
        if (pmqFactory != null) {
            System.out.println("Shutting down JMS Producer : " + pmqFactory.getClientID());
        }

        try {
            if (this.pmqConnection != null)  this.pmqConnection.close();
            if (this.pmqSession != null)     this.pmqSession.close();
            if (this.producer != null)       this.producer.close();
        } catch (JMSException e) {
            System.out.println(e.getMessage());
        }
        this.pmqConnection = null;
        this.pmqSession = null;
        this.producer = null;
        this.pmqFactory = null;
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void PrepareConnector(String pName, String url, String SendQ, String usr, String pwd, String tName) {
        boolean reset = true;
        if (this.MQurl.equals(url) && this.sendQue.equals(SendQ) & pmqSession != null) reset = false;
        if (reset) {
            this.clID = pName;
            this.MQurl = url;
            this.bkr_user = usr;
            this.bkr_pword= pwd;
            this.threadName = tName;
            this.sendQue = SendQ;
            this.ZERROR = false;
            this.Zmessage = "";
            shutdown();
            this.pmqFactory = CreateMQFactory(url, clID);
            this.pmqConnection = CreateMQConnection();
            this.pmqSession = CreateMQSession(pmqConnection);
        }
    }

    public boolean TestConnection(String mqUrl, String mqUsr, String mqPwd, String mqCli, String mqType) {
        if (!clID.equals(mqCli) && !mqCli.equals("")) clID = mqCli;
        NamedCommon.bkr_user        = mqUsr;
        NamedCommon.bkr_pword       = mqPwd;
        boolean okay =false;
        if (Reset(mqUrl, mqCli)) okay = true;
        return okay;
    }

    public void send(String message, String CorrID) {
        if (pmqSession == null) Restart();
        try {
            Destination send2Q;
            if (sendQue.startsWith("VirtualTopic")) {
                try {
                    send2Q = pmqSession.createTopic(sendQue);
                } catch (NullPointerException ee) {
                    ZERROR = true;
                    Zmessage = ee.getMessage();
                    return;
                }
            } else {
                send2Q = pmqSession.createQueue(sendQue);
            }

            TextMessage txtMessage = pmqSession.createTextMessage();
            txtMessage.setText(message);
            txtMessage.setJMSCorrelationID(CorrID);

            if (producer != null) producer.close();

            producer = pmqSession.createProducer(send2Q);

            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            ttl = (System.nanoTime() + 3000);
            producer.send(txtMessage, DeliveryMode.PERSISTENT, 4, ttl);
            producer.close();
            producer = null;
        } catch (JMSException e) {
            ZERROR = true;
            Zmessage = "JMS-Exception on send():: " + e.getMessage();
        }
    }

    public void setup(String CalledFrom, String clid) {
        clID = clid;
        if (CalledFrom.equals("RFUEL")) {
            shutdown();
            System.out.println("Create new JMS objects for " + clID);
            if (pmqFactory != null) {
                if (pmqFactory.getClientID().equals(clID)) {
                    System.out.println("   --> already created. Skipping");
                    return;
                }
            }
            if (pmqFactory == null ) pmqFactory = CreateMQFactory(MQurl, clID);
            if (pmqConnection == null ) pmqConnection = CreateMQConnection();
            if (pmqSession == null ) pmqSession = CreateMQSession(pmqConnection);
        } else {
            if (pmqFactory != null) shutdown();
            if (pmqConnection != null) shutdown();
            if (pmqSession != null) shutdown();
            pmqFactory = CreateMQFactory(MQurl, clID);
            if (!ZERROR) pmqConnection = CreateMQConnection();
            if (!ZERROR) pmqSession = CreateMQSession(pmqConnection);
        }
    }

    private void Restart() {
        shutdown();
        setup("RFUEL", clID);
    }

    public boolean Reset(String url, String cli) {
        uCommons.uSendMessage("close  OLD MQ factory, connection and session -----------------");
        try {
            if (pmqConnection != null)  pmqConnection.close();
            if (pmqSession != null)     pmqSession.close();
            if (producer != null)       producer.close();
        } catch (JMSException e) {
            System.out.println("pERROR: " + e.getMessage());
            return false;
        }
        pmqConnection = null;
        pmqSession = null;
        producer = null;
        pmqFactory = null;

        uCommons.uSendMessage("create NEW MQ factory, connection and session -----------------");

        if (!clID.equals(cli) && !cli.equals("")) clID = cli;
        if (pmqFactory == null ) pmqFactory = CreateMQFactory(url, clID);
        if (ZERROR) return false;
        if (pmqConnection == null ) pmqConnection = CreateMQConnection();
        if (ZERROR) return false;
        if (pmqSession == null ) pmqSession = CreateMQSession(pmqConnection);
        if (ZERROR) return false;
        return true;
    }

    private Session CreateMQSession(Connection inConnection) {
        Session theSession = null;

        try {
            theSession = inConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            ZERROR = true;
            Zmessage = "JMS-Exception Connecting to Producer : " + e.getMessage();
        }
        return theSession;
    }

    private Connection CreateMQConnection() {
        Connection theConnection = null;
        try {
            theConnection = pmqFactory.createConnection();
            theConnection.start();
        } catch (JMSException e) {
            ZERROR = true;
            Zmessage = "JMS-Exception createConnection() to pmqFactory : " + e.getMessage();
            theConnection = null;
        }

        if (theConnection == null) {
            ZERROR = true;
            System.out.println(Zmessage);
        }
        return theConnection;
    }

    private ActiveMQConnectionFactory CreateMQFactory(String url, String clientId) {
        ActiveMQConnectionFactory newFactory = new ActiveMQConnectionFactory(url);
        newFactory.setUserName(this.bkr_user);
        newFactory.setPassword(this.bkr_pword);
        newFactory.setClientID(clientId + this.threadName);
        return newFactory;
    }

}
