package com.unilibre.commons;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JMSProducer {

    private static String sp3 = "   ";
    public String clID= "";
    private ActiveMQConnectionFactory pmqFactory = null;
    private Connection pmqConnection = null;
    private Session pmqSession = null;
    private MessageProducer producer = null;
    private static String lastQueue="";
    private static int anHour = 1000 * 60 * 60;
    private static long ttl;
    private boolean ZERROR=false;
    private String Zmessage = "";
    private String MQurl    = "";
    private String sendQue  = "";

    public void shutdown() {
        if (pmqFactory != null) {
            uCommons.uSendMessage("Shutting down JMS Producer : " + pmqFactory.getClientID());
        }

        try {
            if (pmqConnection != null)  pmqConnection.close();
            if (pmqSession != null)     pmqSession.close();
            if (producer != null)       producer.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
        pmqConnection = null;
        pmqSession = null;
        producer = null;
        pmqFactory = null;
    }

    public boolean FactoryIsNull() { return pmqFactory == null; }

    public void PrepareConnector(String url, String SendQ) {
        boolean reset = true;
        if (this.MQurl.equals(url) && this.sendQue.equals(SendQ) & pmqSession != null) reset = false;
        if (reset) {
            this.MQurl = url;
            this.sendQue = SendQ;
            this.ZERROR = false;
            this.Zmessage = "";
            shutdown();
            pmqFactory = CreateMQFactory(url, clID);
            pmqConnection = CreateMQConnection();
            pmqSession = CreateMQSession(pmqConnection);
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

    public void sendMsg(String message) {
        if (pmqSession == null) {
            if (pmqFactory != null) shutdown();
            if (pmqConnection != null) shutdown();
            if (pmqSession != null) shutdown();
            pmqFactory = CreateMQFactory(MQurl, clID);
            if (!ZERROR) pmqConnection = CreateMQConnection();
            if (!ZERROR) pmqSession = CreateMQSession(pmqConnection);
        }
        try {
            Destination send2Q = null;
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

            if (NamedCommon.debugging) uCommons.uSendMessage("Send message to "+sendQue+" on broker "+MQurl);

            TextMessage txtMessage = pmqSession.createTextMessage();
            txtMessage.setText(message);
            txtMessage.setJMSCorrelationID(NamedCommon.CorrelationID);

            if (producer != null) producer.close();

            producer = pmqSession.createProducer(send2Q);

            if (NamedCommon.isRest) {
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            } else {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            }

            if (NamedCommon.Expiry != 0)  {
                producer.send(txtMessage, DeliveryMode.PERSISTENT, 4, System.nanoTime() + NamedCommon.Expiry);
            } else {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                producer.setPriority(4);
                producer.send(txtMessage);
            }
            producer.close();
            producer = null;
        } catch (JMSException e) {
            ZERROR = true;
            Zmessage = "JMS-Exception on send():: " + e.getMessage();
            if (NamedCommon.debugging) uCommons.uSendMessage(Zmessage);
        }
    }

    public boolean produce(String url, String usr, String pwd, String cli, String que, String msg) throws JMSException {

        if (url.equals("")) return false;
        if (usr.equals("")) return false;
        if (pwd.equals("")) return false;
        if (cli.equals("")) return false;
        if (que.equals("")) return false;
        if (msg.equals("")) return false;

        if (!NamedCommon.bkr_user.equals(usr)) {
            shutdown();
            NamedCommon.bkr_user = usr;
            NamedCommon.bkr_pword = pwd;
        }

        if (!clID.equals(cli) && !cli.equals("")) clID = cli;

        if (pmqSession == null) {
            uCommons.uSendMessage("Create new MQ factory, connection and session -----------------");
            if (clID.equals("")) clID = cli;
            if (pmqFactory == null ) pmqFactory = CreateMQFactory(url, clID);
            if (pmqConnection == null ) pmqConnection = CreateMQConnection();
            if (ZERROR) return false;
            if (pmqSession == null ) pmqSession = CreateMQSession(pmqConnection);
            if (ZERROR) return false;
        }

        Destination send2Q = null;
        if (producer == null) {
            System.out.println(" ");
            uCommons.uSendMessage("Create new MQ producer-----------------------------------------");
            uCommons.uSendMessage("  URL : " + url);
            uCommons.uSendMessage(" User : " + usr);
            uCommons.uSendMessage("---------------------------------------------------------------");
            System.out.println(" ");
            if (que.startsWith("VirtualTopic")) {
                send2Q = pmqSession.createTopic(que);
            } else {
                send2Q = pmqSession.createQueue(que);
            }
            producer = pmqSession.createProducer(send2Q);
            if (NamedCommon.isRest) {
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                producer.setTimeToLive(System.nanoTime() +  NamedCommon.Expiry);
            } else {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            }
            send2Q = null;
        }

        TextMessage txtMessage = null;
        txtMessage = pmqSession.createTextMessage();
        txtMessage.setText(msg);
        txtMessage.setJMSCorrelationID(NamedCommon.CorrelationID);
        producer.send(txtMessage);
        txtMessage = null;
        return true;
    }

    public boolean Reset(String url, String cli) {
        uCommons.uSendMessage("close  OLD MQ factory, connection and session -----------------");
        try {
            if (pmqConnection != null)  pmqConnection.close();
            if (pmqSession != null)     pmqSession.close();
            if (producer != null)       producer.close();
        } catch (JMSException e) {
            uCommons.uSendMessage("ERROR: " + e.getMessage());
            return false;
        }
        pmqConnection = null;
        pmqSession = null;
        producer = null;
        pmqFactory = null;

        uCommons.uSendMessage("creating NEW MQ factory, connection and session ---------------");

        if (!clID.equals(cli) && !cli.equals("")) clID = cli;
        if (pmqFactory == null ) pmqFactory = CreateMQFactory(url, clID);
        if (ZERROR) return false;
        if (pmqConnection == null ) pmqConnection = CreateMQConnection();
        if (ZERROR) return false;
        if (pmqSession == null ) pmqSession = CreateMQSession(pmqConnection);
        if (ZERROR) return false;
        uCommons.uSendMessage("successfully connected ----------------------------------------");
        return true;
    }

    private Session CreateMQSession(Connection inConnection) {
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Creating Session object on Broker Connection");
        Session theSession = null;

        try {
            theSession = inConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            ZERROR = true;
            Zmessage = "JMS-Exception Connecting to Producer : " + e.getMessage();
        }
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Session completed");
        return theSession;
    }

    private Connection CreateMQConnection() {
        Connection theConnection = null;
        try {
            theConnection = pmqFactory.createConnection();
            theConnection.start();
        } catch (JMSException e) {
            ZERROR = true;
            Zmessage = "JMS-Exception CreateMQConnection(producer) to pmqFactory : " + e.getMessage();
            theConnection = null;
        }
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Connection complete");

        if (theConnection == null) {
            ZERROR = true;
            uCommons.uSendMessage(Zmessage);
        }
        return theConnection;
    }

    private static ActiveMQConnectionFactory CreateMQFactory(String url, String clientId) {
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Attaching "+clientId+" to Broker on "+url);
        ActiveMQConnectionFactory newFactory = new ActiveMQConnectionFactory(url);
        newFactory.setUserName(NamedCommon.bkr_user);
        newFactory.setPassword(NamedCommon.bkr_pword);
        newFactory.setClientID(clientId);
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Attachment complete");
        return newFactory;
    }

}
