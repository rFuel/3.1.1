package com.unilibre.commons;

import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;

public class JMSConsumer implements MessageListener {

    public static String queueName = "";
    public static Destination tempDest = null;
    public static String sp3 = "   ";
    public static String clID= "Basic-Cons";
    public ActiveMQConnectionFactory cmqFactory = null;
    public Connection cmqConnection = null;
    public Session cmqSession = null;
    public MessageConsumer consumer = null;
    public boolean ZERROR = false;
    public String Zmessage= "";
    public String filter  = "";
    public String reply   = "";
    public TextMessage jmsText;
    private String url    = "";
    private long startM, nowM;
    private double div;
    private double laps;

    public void SetFilter( String inval) { filter = inval; }

    public String GetCorrelation() throws JMSException {
        if (this.jmsText == null) return "JMS Error - no TextMessage";
        return this.jmsText.getJMSCorrelationID().toString();
    }

    public boolean FactoryIsNull() {
        return cmqFactory == null;
    }

    public void Prepare(String url, String clid) {
//        System.out.println(" ");
//        System.out.println("JMSConsumer.Prepare()");
        this.ZERROR = false;
        this.Zmessage = "";
        this.filter = "";
        this.reply = "";
        this.jmsText = null;
        this.url = url;
        this.clID = clid;
        this.startM = System.nanoTime();
        this.nowM = startM;
        this.laps = 0;
        this.div = 1000000000.00;
        shutdown();
        cmqFactory = CreateMQFactory(url, clID);
        cmqConnection = CreateMQConnection(cmqFactory);
        cmqSession = CreateMQSession(cmqConnection);
//        System.out.println("div: " + String.valueOf(div));
//        System.out.println(" ");
    }

    public void setup(String CalledFrom, String replyQue) {

        try {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
        } catch (JMSException e) {
            //
        }

        try {

            if (CalledFrom.equals("RFUEL")) {
                if (   cmqFactory == null) cmqFactory    = CreateMQFactory(NamedCommon.messageBrokerUrl, clID);
                if (cmqConnection == null) cmqConnection = CreateMQConnection(cmqFactory);
                if (   cmqSession == null) cmqSession    = CreateMQSession(cmqConnection);
            } else {
                if (cmqFactory != null) shutdown();
                cmqFactory = CreateMQFactory(NamedCommon.messageBrokerUrl, NamedCommon.CorrelationID+"-Cons");
                cmqConnection = CreateMQConnection(cmqFactory);
                cmqSession = CreateMQSession(cmqConnection);
            }

            if (cmqSession == null) {
                ZERROR = true;
                Zmessage = "cMQ Connectors need to be reset.";
                return;
            }

            if (!replyQue.equals("") && !replyQue.startsWith("temp")) {
                tempDest = cmqSession.createQueue(replyQue);
            } else {
                tempDest = cmqSession.createTemporaryQueue();
            }

            queueName = tempDest.toString();

            if (this.filter.equals("")) {
                consumer = cmqSession.createConsumer(tempDest);
            } else {
                if (NamedCommon.debugging) uCommons.uSendMessage("Consumer created: " + queueName + " with filter on: ["+this.filter+"]");
                consumer = cmqSession.createConsumer(tempDest, this.filter);
            }
            consumer.setMessageListener(this);
        } catch (JMSException e) {
            ZERROR = true;
            Zmessage = "JMS-Exception on listener():: " + e.getMessage();
        }
    }

    public String consume(String listenTo) {
        if (listenTo.equals("")) return "No Consumer queue provided";
        try {
            if (!listenTo.startsWith("temp") && ! listenTo.startsWith("ID")) {
                tempDest = cmqSession.createQueue(listenTo);
                queueName= listenTo;
            } else {
                tempDest = cmqSession.createTemporaryQueue();
                queueName= "the temporary queue";
            }
            if (consumer != null) consumer.close();
            if (this.filter.equals("")) {
                consumer = cmqSession.createConsumer(tempDest);
            } else {
                if (NamedCommon.debugging) uCommons.uSendMessage("Consumer created: " + queueName + " with filter on: ["+this.filter+"]");
                consumer = cmqSession.createConsumer(tempDest, this.filter);
            }
            reply= "";
            consumer.setMessageListener(this);
        } catch (JMSException e) {
            System.out.println(e.getMessage());
        }

        //
        // ------------------------------------------------------------------
        // hang around and wait for reply to come from OnMessage method
        // ------------------------------------------------------------------
        //
//        System.out.println("div: " + String.valueOf(div));
//        System.out.println(" ");
        reply = "";
        startM = System.nanoTime();
        while (reply.equals("")) {
            try {
                Thread.sleep(500);
                nowM = System.nanoTime();
                laps = (nowM - startM) / div;
//                System.out.println(laps);
                if (laps > 10.00) {
                    uCommons.uSendMessage("Timeout waiting for reply.");
                    if (!reply.equals("")) reply = "FAIL timeout waiting for reply";
                    if (consumer != null) {
                        try {
                            consumer.close();
                            uCommons.uSendMessage("MQ consumer closed.");
                        } catch (JMSException e) {
                            System.out.println("Cannot close consumer " + e.getMessage());
                        }
                    }
                    break;
                }
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
        return reply;
    }

    public static Session CreateMQSession(Connection inConnection) {
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Creating Session object on Broker Connection");
        Session theSession = null;
        try {
            theSession = inConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            uCommons.uSendMessage("ERROR: " + e.getMessage());
        }
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Session completed");
        return theSession;
    }

    public static Connection CreateMQConnection(ActiveMQConnectionFactory consumerA_Factory) {
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Creating connection for (C) mqFactory "+consumerA_Factory.getBrokerURL());
        Connection theConnection = null;
        String err="";
        try {
            theConnection = consumerA_Factory.createConnection();
            theConnection.start();
            if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Connection complete");
        } catch (JMSException e) {
            err = "JMS-Exception CreateMQConnection(consumer) to pmqFactory : " + e.getMessage();
            uCommons.uSendMessage(sp3 + sp3 + err);
        }
        return theConnection;
    }

    public static ActiveMQConnectionFactory CreateMQFactory(String url, String clientId) {
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Attaching "+clientId+" to Broker on "+url);
        ActiveMQConnectionFactory newFactory = new ActiveMQConnectionFactory(url);
        newFactory.setUserName(NamedCommon.bkr_user);
        newFactory.setPassword(NamedCommon.bkr_pword);
        newFactory.setClientID(clientId);
        if (NamedCommon.debugging) uCommons.uSendMessage(sp3 + sp3 + "Attachment complete");
        return newFactory;
    }

    public void onMessage(Message message) {
        if (NamedCommon.debugging) uCommons.uSendMessage("Received a reply on " + queueName);
        String answer = "";
        if (message instanceof TextMessage) {
            TextMessage tm = (TextMessage) message;
            try {
                answer = tm.getText();
                this.jmsText = tm;
            } catch (JMSException e) {
                answer = "JMS-Exception onMessage.getText():: " + e.getMessage();
                ZERROR = true;
                Zmessage = answer;
                this.jmsText = null;
            }
        }
        reply = answer;
    }

    public void shutdown() {
//        if (cmqFactory != null) {
//            uCommons.uSendMessage("Shutting down JMS Consumer : " + cmqFactory.getClientID());
//        }

        try {
            if (cmqConnection != null) cmqConnection.close();
            if (cmqSession != null) cmqSession.close();
            if (consumer != null) consumer.close();
        } catch (JMSException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "MQ-Client shutdown() Error: " + e.getMessage();
            uCommons.uSendMessage(NamedCommon.Zmessage);
        }
        cmqConnection = null;
        cmqSession = null;
        consumer = null;
        cmqFactory = null;
        tempDest = null;
    }

}
