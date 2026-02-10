package com.unilibre.http2jms;

import com.unilibre.commons.NamedCommon;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JMSConsumer implements MessageListener {

    private String threadName = "";
    private Destination tempDest = null;
    public String clID= "Basic-Cons";
    private ActiveMQConnectionFactory cmqFactory = null;
    private Connection cmqConnection = null;
    private Session cmqSession = null;
    private MessageConsumer consumer = null;
    public String filter  = "";
    public String reply   = "";
    private String url    = "";
    private String bkr_user = "";
    private String bkr_pword= "";

    public void SetFilter( String inval) { this.filter = inval; }

    public void Prepare(String filter, String url, String clid, String usr, String pwd, String tName) {
        if (!this.url.equals(url)) {
            this.filter = filter;
            this.url = url;
            this.bkr_user = usr;
            this.bkr_pword= pwd;
            this.clID = clid;
            this.threadName = tName;
            shutdown();
            this.cmqFactory = CreateMQFactory(url, clID);
            this.cmqConnection = CreateMQConnection(cmqFactory);
            this.cmqSession = CreateMQSession(cmqConnection);
        }
    }

    public String consume(String listenTo) {
        if (listenTo.equals("")) return "No Consumer queue provided";
        try {
            if (!listenTo.startsWith("temp") && ! listenTo.startsWith("ID")) {
                tempDest = cmqSession.createQueue(listenTo);
            } else {
                tempDest = cmqSession.createTemporaryQueue();
            }
            if (consumer != null) consumer.close();
            if (this.filter.equals("")) {
                consumer = cmqSession.createConsumer(tempDest);
            } else {
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
        reply = "";
        while (reply.equals("")) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
        return reply;
    }

    public Session CreateMQSession(Connection inConnection) {
        Session theSession = null;
        try {
            theSession = inConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            System.out.println("cERROR: " + e.getMessage());
        }
        return theSession;
    }

    public Connection CreateMQConnection(ActiveMQConnectionFactory consumerA_Factory) {
        Connection theConnection = null;
        try {
            theConnection = consumerA_Factory.createConnection();
            theConnection.start();
        } catch (JMSException e) {
            System.out.println("JMS-Exception createConnection() to pmqFactory : " + e.getMessage());
        }
        return theConnection;
    }

    public ActiveMQConnectionFactory CreateMQFactory(String url, String clientId) {
        ActiveMQConnectionFactory newFactory = new ActiveMQConnectionFactory(url);
        newFactory.setUserName(bkr_user);
        newFactory.setPassword(bkr_pword);
        newFactory.setClientID(clientId + this.threadName);
        return newFactory;
    }

    public void onMessage(Message message) {
        String answer = "";
        if (message instanceof TextMessage) {
            TextMessage tm = (TextMessage) message;
            try {
                answer = tm.getText();
            } catch (JMSException e) {
                answer = "JMS-Exception onMessage.getText():: " + e.getMessage();
            }
        }
        reply = answer;
    }

    public void shutdown() {
        if (cmqFactory != null) {
            System.out.println("Shutting down JMS Consumer : " + cmqFactory.getClientID());
        }

        try {
            if (cmqConnection != null) cmqConnection.close();
            if (cmqSession != null) cmqSession.close();
            if (consumer != null) consumer.close();
        } catch (JMSException e) {
            System.out.println(NamedCommon.Zmessage);
        }
        cmqConnection = null;
        cmqSession = null;
        consumer = null;
        cmqFactory = null;
        tempDest = null;
    }

}
