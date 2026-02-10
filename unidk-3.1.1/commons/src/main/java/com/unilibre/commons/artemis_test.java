package com.unilibre.commons;

import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;

public class artemis_test implements MessageListener {

    private static String mqURL = "tcp://192.168.4.51:61616";
    private static String mqQue = "Tests_In";
    private static String CorrelationID = "";
    private static int Expiry = 0;
    private static ActiveMQConnectionFactory theFactory = null;
    private static Connection theConnection = null;
    private static Session theSession = null;
    private static Destination BrokerQ = null;

    public static void main(String[] args) throws JMSException {
        CorrelationID = "Header-Correl";
        Expiry  = 1000;
        new artemis_test();
    }

    public artemis_test() throws JMSException {
        BaseObjects();
        String message = "";
        message = "This is from com.unilibre.commons.artemis_test()";
        Produce(message);
        Consume();
    }

    private void BaseObjects() throws JMSException {
        System.out.println("---------------- Create Artemis Objects  ----------------");
        System.out.println("   Create the Factory");
        theFactory = new ActiveMQConnectionFactory(mqURL);
        theFactory.setUserName("admin");
        theFactory.setPassword("admin");
        theFactory.setClientID("test:4:artemis"+NamedCommon.pid);

        System.out.println("   Create the Connection");
        theConnection = theFactory.createConnection();
        theConnection.start();

        System.out.println("   Create the Session");
        theSession = theConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        System.out.println("   Create the Destination Queue");

        // -----------------------------------------------------------
        // Make sure Queues, Topics and TempQueues all work on Artemis
        // -----------------------------------------------------------
//        BrokerQ = theSession.createTopic(mqQue);
//        BrokerQ = theSession.createTemporaryQueue();
        BrokerQ = theSession.createQueue(mqQue);

    }

    private void Produce(String message) throws JMSException {
        System.out.println("---------------- Create JMS Producer  ----------------");
        if (message.equals("")) {
            if (!message.equals("")) System.out.println("   nothing to send");
            return;
        }

        System.out.println("   Create MessageProducer");
        MessageProducer producer = theSession.createProducer(BrokerQ);

        System.out.println("   Create TextMessage");
        TextMessage txtMessage = theSession.createTextMessage();
        txtMessage.setText(message);
        txtMessage.setJMSCorrelationID(CorrelationID);

        long ttl = (System.nanoTime() + Expiry);
        producer.send(txtMessage, DeliveryMode.PERSISTENT, 4, ttl);
        producer.close();
        producer = null;
    }

    private void Consume() throws JMSException {
        System.out.println("---------------- Create JMS Consumner ----------------");

        System.out.println("   Create MessageConsumer with filter on CorrelationID");
        String filter = "JMSCorrelationID = '" + CorrelationID + "'";

        System.out.println("   Create Listener");
        MessageConsumer consumer = theSession.createConsumer(BrokerQ, filter);

        System.out.println("   Listening ....");
        consumer.setMessageListener(this);
    }

    public void onMessage(Message message) {
        if (message instanceof TextMessage) {
            TextMessage tm = (TextMessage) message;
            try {
                System.out.println("---------------- Message Received --------------------");
                System.out.println(tm.getText());
                System.out.println(tm.getJMSCorrelationID());
                System.out.println("------------------------------------------------------");
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
    
}
