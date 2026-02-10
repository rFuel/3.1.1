package com.unilibre.MQConnector;


import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import javax.jms.*;
import static com.unilibre.MQConnector.commons.*;



public class artemisMQ {

    // ----------- AMQ Producer Objects --------------------
    private static ActiveMQConnectionFactory pFactory = null;
    private static MessageProducer producer    = null;
    private static Connection pConnection = null;
    private static Session pSession    = null;
    private static Queue pQueue      = null;

    // ----------- AMQ Consumer Objects --------------------
    private static ActiveMQConnectionFactory cFactory = null;
    private static MessageConsumer consumer    = null;
    private static Connection cConnection = null;
    private static Session          cSession    = null;
    private static Queue            cQueue      = null;

    private static String lastPurl = "", lastCurl = "";
    private static String lastPque = "", lastCque = "";
    private static Runtime garbo = Runtime.getRuntime();

    private static int AUTO_ACKNOWLEDGE = 1;
    private static int CLIENT_ACKNOWLEDGE = 2;

    public static void CloseConsumer() {
        try {
            if (consumer != null)     consumer.close();
            if (cSession != null)     cSession.close();
            if (cConnection != null)  cConnection.close();
        } catch (JMSException e) {
            commons.uSendMessage("CloseMQ Consumer ERROR: "+e.getMessage());
        }
        cFactory = null;
        cConnection = null;
        cSession = null;
        cQueue = null;
        consumer = null;
    }

    public static void CloseProducer() {
        try {
            if (producer != null)     producer.close();
            if (pSession != null)     pSession.close();
            if (pConnection != null)  pConnection.close();
        } catch (JMSException e) {
            commons.uSendMessage("CloseMQ Producer ERROR: "+e.getMessage());
        }
        pFactory = null;
        pConnection = null;
        pSession = null;
        pQueue = null;
        producer = null;
    }

    private static MessageProducer NewProducer (String que) {
        try {
            if (producer != null) producer.close();
            pQueue = ActiveMQJMSClient.createQueue(que);
            producer = pSession.createProducer(pQueue);
        } catch (JMSException e) {
            commons.uSendMessage("com.unilibre.MQConnector.artemisMQ.NewProducer ProducerClose ERROR: "+e.getMessage());
            producer = null;
        }
        lastPque = que;
        return producer;

    }

    private static MessageProducer rfProducer (String url, String usr, String pwd, String name, String que) throws JMSException {
        pFactory = null;
        pConnection = null;
        pSession = null;
        pQueue = null;
        producer = null;

        pFactory = new ActiveMQConnectionFactory(url);
        pFactory.setUser(usr);
        pFactory.setPassword(pwd);
        pFactory.setClientID("Producer:"+pid+":"+pCounter);
        pFactory.setConnectionTTL(120000);
        pCounter++;
        pConnection = pFactory.createConnection();
        pSession = pConnection.createSession(false, CLIENT_ACKNOWLEDGE);
        pQueue = ActiveMQJMSClient.createQueue(que);
        producer = pSession.createProducer(pQueue);
        pConnection.start();
        lastPurl = url;
        lastPque = que;
        return producer;
    }

    private static Session GetSession (String url, String usr, String pwd, String name, String que) throws JMSException {
        cFactory = null;
        cSession = null;
        cQueue = null;
        consumer = null;
        cConnection = null;

        cFactory = new ActiveMQConnectionFactory(url);
        cFactory.setUser(usr);
        cFactory.setPassword(pwd);
        cFactory.setClientID(name);
        cFactory.setConnectionTTL(120000);
        cConnection = cFactory.createConnection();
        cSession = cConnection.createSession(false, CLIENT_ACKNOWLEDGE);
        return cSession;
    }

    private static MessageConsumer GetConnection (String url, String usr, String pwd, String name, String que) throws JMSException {
        if (cSession == null) cSession = GetSession(url, usr, pwd, name, que);
        cQueue = ActiveMQJMSClient.createQueue(que);
        consumer = cSession.createConsumer(cQueue);
        cConnection.start();
        return consumer;
    }

    private static MessageConsumer rfConsumer (String url, String usr, String pwd, String name, String que) throws JMSException {
        cFactory = null;
        cConnection = null;
        cSession = null;
        cQueue = null;
        consumer = null;

        cFactory = new ActiveMQConnectionFactory(url);
        cFactory.setUser(usr);
        cFactory.setPassword(pwd);
        if (name.startsWith("Cons")) {
            name += ":"+cCounter;
        } else {
            name = "Consumer:"+pid+":"+cCounter;
        }
        cFactory.setClientID(name);
        cCounter++;
        cFactory.setConnectionTTL(120000);
        cConnection = cFactory.createConnection();
        cSession = cConnection.createSession(false, CLIENT_ACKNOWLEDGE);
        cQueue = ActiveMQJMSClient.createQueue(que);
        consumer = cSession.createConsumer(cQueue);
        cConnection.start();
        return consumer;
    }

    public static MessageConsumer getConsumer(String url, String usr, String pwd, String name, String que) throws JMSException {
        CloseConsumer();
        return rfConsumer(url, usr, pwd, name, que);
    }

    public static String consume (String url, String usr, String pwd, String name, String que) throws JMSException {
        String reply="";
        if (consumer == null) consumer = rfConsumer(url, usr, pwd, name, que);
        if (commons.ZERROR) return reply;

        TextMessage messageReceived;
        messageReceived = (TextMessage) consumer.receive(5000);

        if (messageReceived == null) return "";

        reply = messageReceived.getText();

        while (reply.length() < 1) {
            if (reply.length() > 1) break;
            commons.Sleep(150);
            messageReceived = (TextMessage) consumer.receive(5000);
            reply = messageReceived.getText();
        }

        messageReceived = null;
        return reply;
    }

    public static boolean produce (String url, String usr, String pwd, String name, String que, String message) throws JMSException {
        boolean first = false;
        if (producer == null) {
            producer = rfProducer(url, usr, pwd, name, que);
            first = true;
        }
        if (!lastPurl.equals(url)) {
            if (producer != null) {
                CloseProducer();
            } else {
                if (!first) NewProducer(que);
            }
        }

        if (!lastPque.equals(que) && !first) {
            NewProducer(que);
        }

        if (commons.ZERROR) return false;

        TextMessage msg = pSession.createTextMessage(message);
        producer.send(msg);

        msg = null;
        return true;
    }

    public static Session getQBsession (String url, String usr, String pwd, String name, String que) throws JMSException {
        return GetSession(url, usr, pwd, name, que);
    }

    public static MessageConsumer getQBConnection (String que) throws JMSException {
        // Queue Balancer - development required
        if (cSession == null) return null;
        return GetConnection("", "", "", "", que);
    }

    public static void cPrepare(String url, String usr, String pwd, String que) throws JMSException {
        if (consumer == null) consumer = rfConsumer(url, usr, pwd, "", que);
    }

    public static boolean isConnected() {
        if (consumer == null) {
            return false;
        } else {
            return true;
        }
    }
}
