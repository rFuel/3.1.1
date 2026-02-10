package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED  */

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTempQueue;

import javax.jms.*;

public class mqCommons {

    private static String url = "";
    public static ActiveMQConnectionFactory cmqFactory = null;
    public static Connection cmqConnection = null;
    public static Session cmqSession = null;
    public static Destination cmqDest = null;
    public static MessageConsumer consumer = null;

    public static ActiveMQConnectionFactory cGetMQFactory(String BkrUrl) {
        cmqFactory = new ActiveMQConnectionFactory(BkrUrl);
        cmqFactory.setUserName(NamedCommon.bkr_user);
        cmqFactory.setPassword(NamedCommon.bkr_pword);
        cmqFactory.setClientID(NamedCommon.inputQueue + ":Cons:" + NamedCommon.hostname+"--"+NamedCommon.pid+"--"+NamedCommon.mqCounter);
        cmqFactory.setConnectResponseTimeout(20000);
        NamedCommon.mqCounter++;
        url = BkrUrl;
        return cmqFactory;
    }

    public static Connection cGetConnection(String BkrUrl) {
        if (cmqFactory == null) cmqFactory = cGetMQFactory(BkrUrl);
        cmqConnection = null;
        if (NamedCommon.ZERROR) return cmqConnection;
        try {
            cmqConnection = cmqFactory.createConnection();
            cmqConnection.start();
        } catch (JMSException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "mqCommons.GetConnection - " + e.getMessage();
            uCommons.uSendMessage("JMSException caught in mqCommons.cGetConnection()");
        }
        return cmqConnection;
    }

    public static Session cGetMQsession(String BkrUrl) {
        if (cmqConnection == null) cmqConnection = cGetConnection(BkrUrl);
        cmqSession = null;
        if (NamedCommon.ZERROR) return cmqSession;
        boolean transacted = false;
        int ackMode = Session.AUTO_ACKNOWLEDGE;
        try {
            cmqSession = cmqConnection.createSession(transacted, ackMode);
        } catch (JMSException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "mqCommons.GetMQsession - " + e.getMessage();
            uCommons.uSendMessage("JMSException caught in mqCommons.cGetMQsession()");
        }
        return cmqSession;
    }

    public static Destination cGetQueue(String BkrUrl, String que) {
        if (cmqSession == null) cmqSession = cGetMQsession(BkrUrl);
        cmqDest = null;
        if (NamedCommon.ZERROR) return cmqDest;
        try {
            cmqDest = cmqSession.createQueue(que);
        } catch (JMSException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "GetQueue - " + e.getMessage();
            uCommons.uSendMessage("JMSException caught in mqCommons.cGetQueue()");
        }
        return cmqDest;
    }

    public static MessageConsumer cGetConsumer(String BkrUrl, String qname) {
        if (cmqDest == null) cmqDest = cGetQueue(BkrUrl, qname);
        consumer = null;
        if (NamedCommon.ZERROR) return consumer;
        try {
            consumer = cmqSession.createConsumer(cmqDest);
        } catch (JMSException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "GetConsumer - " + e.getMessage();
            uCommons.uSendMessage("JMSException caught in mqCommons.cGetConsumer()");
        }
        return consumer;
    }
    
    public static void cSendACK(String msg, String bkrUrl, String reply2Q, String incorrid) {
        NamedCommon.ZERROR = false;
        if (cmqSession == null) cmqSession = cGetMQsession(bkrUrl);
        try {
            ActiveMQTempDestination destination2 = new ActiveMQTempQueue(reply2Q);
            MessageProducer cProducer = cmqSession.createProducer(destination2);
            TextMessage txtMessage = cmqSession.createTextMessage(msg);
            txtMessage.setJMSDestination(destination2);
            txtMessage.setJMSCorrelationID(incorrid);
            cProducer.send(txtMessage);
            cProducer.close();

        } catch (JMSException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Destination: " + e.getMessage() + " does not exist.";
        }
    }

    public static void cReconnect(String BkrUrl, String que) {
        uCommons.uSendMessage("-------------- cReconnect("+BkrUrl+", "+que+") --------------");
        CloseConnection();
        cGetConsumer(BkrUrl, que);
    }

    public static void CloseConnection() {
        String clID = "";
        if (cmqFactory != null) clID = cmqFactory.getClientID();
        uCommons.uSendMessage("  ---  Closing " + clID);
        try {
            if (consumer != null)       consumer.close();
            if (cmqSession != null)     cmqSession.close();
            if (cmqConnection != null)  cmqConnection.close();
        } catch (JMSException e) {
            uCommons.uSendMessage("CloseMQ ERROR: "+e.getMessage());
        }
        cmqFactory = null;
        cmqConnection = null;
        cmqSession = null;
        cmqDest = null;
        consumer = null;
    }

}
