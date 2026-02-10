package com.unilibre.MQConnector;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempDestination;
import org.apache.activemq.command.ActiveMQTempQueue;

import javax.jms.*;

public class activeMQ  {

    private static ActiveMQConnectionFactory pFactory = null;
    private static Connection pConnection = null;
    private static Session pSession = null;
    private static Destination pDest = null;
    private static ActiveMQTempDestination pTempDest;
    private static MessageProducer producer = null;
    private static ActiveMQConnectionFactory cFactory = null;
    private static Connection cConnection = null;
    private static Session cSession = null;
    private static Destination cDest = null;
    private static ActiveMQTempDestination cTempDest;
    private static MessageConsumer consumer = null;
    private static boolean tmpQ = false, isDocker=false, transacted=false, showACK=true, priority=false;
    private static final int AUTO_ACKNOWLEDGE = 1;
    private static final int CLIENT_ACKNOWLEDGE = 2;
    private static int deliveryMode = 1, msgWait=5000, priorityLvl=0;
    private static long timeToLive = 0;
    private static String reply="", hostname="";
    private static Message cMessage;

    public static void SetACK(boolean inval) { showACK=inval;}

    public static void SetExpiry(long exp) {timeToLive = exp;}

    public static void SetTransacted(boolean inval) {
        transacted = inval;
    }

    public static void SetDocker(boolean inval) { isDocker = inval; }

    public static void SetHost(String inval) { hostname = inval; }

    public static void SetMessage(Message inval) {
        cMessage = inval;
    }

    public static void SetDeliveryMode (int mode) { deliveryMode = mode; }

    public static void SetConsumerWait (int wait) { msgWait = wait;}

    public static void SetPriority(boolean ptyOnOff, int ptyValue) {
        priority = ptyOnOff;
        priorityLvl = ptyValue;
    }

    public static boolean GetTransacted() {return transacted;}

    public static void  DeleteQueue(String brokerUrl, String queueName) {
        ActiveMQConnection connection = null;
        try {
            connection = (ActiveMQConnection) new ActiveMQConnectionFactory(brokerUrl).createConnection();
            connection.destroyDestination(new ActiveMQQueue(queueName));
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                    connection = null;
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void CloseConsumer() {
        try {
            if (consumer != null)     consumer.close();
            if (cSession != null)     cSession.close();
            if (cConnection != null)  cConnection.close();
            if (cTempDest != null) cTempDest.delete();
        } catch (JMSException e) {
            commons.uSendMessage("CloseMQ Consumer ERROR: "+e.getMessage());
        }
        cFactory = null;
        cConnection = null;
        cSession = null;
        cDest = null;
        cTempDest = null;
        consumer = null;
        tmpQ = false;
    }

    public static void CloseProducer() {
        try {
            ShutdownProducer();
            if (producer != null)  producer.close();
            if (pTempDest != null) pTempDest.delete();
        } catch (JMSException e) {
            commons.uSendMessage("CloseMQ Producer ERROR: "+e.getMessage());
        }
        pDest = null;
        pTempDest = null;
        producer = null;
        tmpQ = false;
    }

    private static void ShutdownProducer() {
        if (pSession != null) {
            try {
                pSession.close();
            } catch (JMSException e) {
                commons.uSendMessage(e.getMessage());
            }
        }
        if (pConnection != null) {
            try {
                pConnection.close();
            } catch (JMSException e) {
                commons.uSendMessage(e.getMessage());
            }
        }
        pDest = null;
        pSession = null;
        pConnection = null;
        pFactory = null;
    }

    public static MessageProducer rfProducer (String url, String usr, String pwd, String name, String que) {
        CloseProducer();
        if (!commons.inputQueue.equals("")) {
            name = commons.inputQueue;
        } else {
            name = que;
        }
        pFactory = null;
        pFactory = new ActiveMQConnectionFactory(url);
        pFactory.setUserName(usr);
        pFactory.setPassword(pwd);
        if (!hostname.equals("")) {
            pFactory.setClientID(hostname + ":" + name + ":prod");
        } else {
            pFactory.setClientID(commons.pid + ":" + name + ":prod");
        }
        try {
            pConnection = pFactory.createConnection();
            pConnection.start();
        } catch (JMSException e) {
            commons.uSendMessage("pmqCommons.pGetConnection - " + e.getMessage());
            commons.ZERROR = true;
            ShutdownProducer();
            return null;
        }

        if (commons.GetAckMode() == 0) commons.SetAckMode(CLIENT_ACKNOWLEDGE);

        try {
            pSession = pConnection.createSession(false, commons.GetAckMode());
        } catch (JMSException e) {
            commons.uSendMessage("com.unilibre.MQConnector.activeMQ.rfProducer ERROR - " + e.getMessage());
            commons.ZERROR = true;
            ShutdownProducer();
            return null;
        } catch (NullPointerException npe) {
            commons.uSendMessage("com.unilibre.MQConnector.activeMQ.rfProducer.pSession NullPointerException - " + npe.getMessage());
            commons.ZERROR = true;
            commons.ERRmsg = npe.getMessage();
            ShutdownProducer();
            return null;
        }

        try {
            pDest = pSession.createQueue(que);
            producer = pSession.createProducer(pDest);
            producer.setDeliveryMode(deliveryMode);
        } catch (JMSException e) {
            commons.ZERROR = true;
            commons.ERRmsg = e.getMessage() + " does not exist";
            ShutdownProducer();
            return null;
        }

        // -----------------------------------------------------------------------------
        // if it IS a temp-queue, the integration layer will have already created it.
        // if temp-queue does exists, just create a producer to it and send the reply.
        // if the temp-queue does not exist;
        //      1.  log the error
        //      2.  Send the error to RunERRORS
        //      3.  continue processing
        // this way, a backlog of messages CAN be processed but the replies will not be
        // received by the integration layer. Is this a problem - check with Ben & JC !!
        // -----------------------------------------------------------------------------

        if (producer == null) commons.ZERROR = true;
        return producer;
    }

    public static MessageConsumer rfConsumer (String url, String usr, String pwd, String name, String que) {
        CloseConsumer();
        commons.ZERROR = false;
        commons.ERRmsg = "";
        cFactory = new ActiveMQConnectionFactory(url);
        cFactory.setUserName(usr);
        cFactory.setPassword(pwd);
        if (isDocker) {
            cFactory.setClientID(hostname + ":" + que + ":cons");
        } else {
            cFactory.setClientID(commons.pid + ":" + que + ":cons");
        }
        commons.cCounter++;
        try {
            cConnection = cFactory.createConnection();
            cConnection.start();
        } catch (JMSException e) {
            commons.ZERROR = true;
            commons.ERRmsg = "MQConnector.activeMQ.rfConsumer.cConnection - " + e.getMessage();
            return null;
        }

        if (commons.GetAckMode() == 0) commons.SetAckMode(CLIENT_ACKNOWLEDGE);

        try {
            cSession = cConnection.createSession(transacted, commons.GetAckMode());
        } catch (JMSException e) {
            commons.ZERROR = true;
            commons.ERRmsg = "MQConnector.activeMQ.rfConsumer.cSession - " + e.getMessage();
            commons.uSendMessage(commons.ERRmsg);
            return null;
        }
        cDest = null;
        if (commons.ZERROR) return null;
        try {
            if (!tmpQ) {
                if (priority) {
                    que = que+"?consumer.priority="+priorityLvl;
                }
                cDest = cSession.createQueue(que);
                consumer = cSession.createConsumer(cDest);
            } else {
                ActiveMQTempDestination cTempDest = new ActiveMQTempQueue(que);
                consumer = cSession.createConsumer(cTempDest);
            }
        } catch (JMSException e) {
            commons.ZERROR = true;
            commons.ERRmsg = "MQConnector.activeMQ.rfConsumer.cDest - " + e.getMessage();
            commons.uSendMessage(commons.ERRmsg);
            return null;
        }

        if (consumer == null) commons.ZERROR = true;
        return consumer;
    }

    public static String consume(String url, String usr, String pwd, String name, String que) throws JMSException {
        reply="";
        tmpQ = false;
        if (que.toLowerCase().startsWith("temp-")) {
            tmpQ = true;
            que = que.split("\\/\\/")[1];
            commons.uSendMessage("com.unilibre.MQConnector.activeMQ.consume  CANNOT consume from a temporary queue!");
            return "";
        }

        if (consumer == null) consumer = rfConsumer(url, usr, pwd, name, que);
        if (commons.ZERROR) return reply;

        while (reply.equals("")) {
            TextMessage message = (TextMessage) consumer.receive(msgWait);
            if (message == null) return "";
            try {
                reply = message.getText();
                if (!reply.equals("")) {
                    cMessage = message;
                } else {
                    cMessage = null;
                }
            } catch (NullPointerException npe) {
                commons.uSendMessage("ERROR: "+npe.getMessage());
                if (transacted && commons.GetAckMode() == CLIENT_ACKNOWLEDGE) cSession.rollback();
                reply = "";
            }
        }
        return reply;
    }

    public static void Acknowledge() {
        if (cMessage == null) return;
        try {
            cMessage.acknowledge();
            if (transacted && cSession != null) cSession.commit();
            if (showACK) System.out.println("-------------------------------------- Acknowledged --------------------------------------");
        } catch (JMSException e) {
            System.out.println("Message Acknowledge ERROR: "+e.getMessage());
            try {
                cSession.recover();
                System.out.println("-------------------------------------- Recovered -----------------------------------------");
            } catch (JMSException ex) {
                System.out.println("Message Recovery ERROR: "+ex.getMessage());
            }
        }
    }

    public static boolean produce (String url, String usr, String pwd, String name, String que, String message) {
        boolean okay = false;
        commons.ZERROR = false;
        commons.ERRmsg = "";
        // ----------------------- Heritage uses temp-queues --------------------------
        tmpQ = que.toLowerCase().startsWith("temp-") || que.toUpperCase().startsWith("ID:");
        // ----------------------------------------------------------------------------

        if (tmpQ) {
            // Should NEVER come here. temp-queues are identified by AMQ with ID:... queue names
            if (pSession == null) { producer = rfProducer(url, usr, pwd, name, que); }
            if (que.contains("//")) que = que.split("\\/\\/")[1];
            if (que.equals("")) {
                commons.ZERROR = true;
                commons.ERRmsg = "The replyto queue is empty";
                return false;
            }
            try {
                ActiveMQTempDestination destination2 = new ActiveMQTempQueue(que);
                // if the temp queue is not found, AMQ clobbers the object and variables are set to null
                if (pSession == null) rfProducer(url, usr, pwd, name, que);
                MessageProducer tProducer = pSession.createProducer(destination2);
                TextMessage txtMessage = pSession.createTextMessage(message);
                txtMessage.setJMSDestination(destination2);
                txtMessage.setJMSCorrelationID(name);
                if (timeToLive > 0) {
                    txtMessage.setJMSExpiration(timeToLive);
                    tProducer.send(txtMessage, deliveryMode, 4, timeToLive);
                } else {
                    tProducer.send(txtMessage);
                }

                tProducer.close();
                tProducer = null;
                destination2 = null;
                txtMessage = null;
                okay = true;
            } catch (JMSException e) {
                commons.ERRmsg = "activeMQ.produce.ActiveMQTempDestination: " + e.getMessage() + " does not exist.";
                commons.ZERROR = true;
                return false;
            }
        } else {
            // rFuel has many queues per task, reset pDest for new queues.
            // DO NOT reset pSession if you can help it - makes ssl SLOW.

            if (pSession == null) {
                rfProducer(url, usr, pwd, name, que);
                if (commons.ZERROR) return false;
            }

            try {
                pDest = pSession.createQueue(que);
                producer = pSession.createProducer(pDest);
                producer.setDeliveryMode(deliveryMode);
            } catch (JMSException e) {
                commons.ZERROR = true;
                commons.ERRmsg = e.getMessage() + " does not exist";
                return false;
            }

            if (commons.ZERROR) return false;
            if (producer == null) producer = rfProducer(url, usr, pwd, name, que);
            if (commons.ZERROR) return false;

            try {
                TextMessage msg;
                msg = pSession.createTextMessage(message);
                msg.setJMSDestination(pDest);
                msg.setJMSCorrelationID(name);
                if (timeToLive > 0) {
                    msg.setJMSExpiration(timeToLive);
                    producer.send(msg, deliveryMode, 4, timeToLive);
                } else {
                    producer.send(msg);
                }
                msg = null;
                okay = true;
            } catch (JMSException e) {
                commons.ZERROR = true;
                commons.ERRmsg = e.getMessage();
                okay = false;
            }
        }
        return okay;
    }

}
