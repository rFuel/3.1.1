package com.unilibre.rfuel;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

// this is meant for vitual topics
// uMessageRouter works with queues


import com.unilibre.commons.APImsg;
import com.unilibre.commons.Hop;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Enumeration;

public class uMessageConsumer implements MessageListener  {

    private String activeMqBrokerUri;
    private String username;
    private String password;
    private String ConsID;
    private boolean isDestinationTopic;
    private String destinationName;
    private String selector;
    private String clientId;
    private String inbound;

    public uMessageConsumer(String activeMqBrokerUri, String username, String password) {
        super();
        this.activeMqBrokerUri = activeMqBrokerUri;
        this.username = username;
        this.password = password;
    }

    public void run() throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(username, password, activeMqBrokerUri);
        if (clientId != null) { factory.setClientID(clientId); }
        Connection connection = factory.createConnection();
        if (clientId != null) { connection.setClientID(clientId); }
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        setComsumer(session);
        connection.start();
    }

    private void setComsumer(Session session) throws JMSException {
        MessageConsumer consumer = null;
        if (isDestinationTopic) {
            Topic topic = session.createTopic(destinationName);
            if (selector == null) {
                consumer = session.createConsumer(topic);
            } else {
                consumer = session.createConsumer(topic, selector);
            }
        } else {
            Destination destination = session.createQueue(destinationName);
            if (selector == null) {
                consumer = session.createConsumer(destination);
            } else {
                consumer = session.createConsumer(destination, selector);
            }
        }
        consumer.setMessageListener(this);
        System.out.println("Created MessageListener " + this.getClientId() + " on "+this.destinationName);
    }

    @Override
    public void onMessage(Message message) {
        String TheMessage = "";
        uCommons.uSendMessage("Got a message on "+getClientId());
        TextMessage inMsg = (TextMessage) message;
        try {
            TheMessage = inMsg.getText();
        } catch (JMSException e) {
            TheMessage = "Invalid message (datatype) received.";
            System.out.println(TheMessage);
            System.out.println(e.getMessage());
        }
        uRoute(TheMessage);
    }

    private void uRoute(String theMessage) {
        uCommons.uSendMessage("Route the message. APImsg.instantiate()");
        APImsg.instantiate();
        uCommons.uSendMessage("MessageToAPI()");
        uCommons.MessageToAPI(theMessage);
        String mTask    = APImsg.APIget("task");
        int fnd = uSubscriber.FindTask(mTask);
        uCommons.uSendMessage("task="+mTask+"  found at "+fnd);
        if (fnd < 0) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Unrecognised arrived at topic: task="+mTask;
            uCommons.uSendMessage(NamedCommon.Zmessage);
            return;
        }

        String tType     = mTask.substring(0,1);
        String theQue    = "";
        String tempFlag  = "";
        String localbkr  = NamedCommon.messageBrokerUrl;
        String remotebkr = "";
        String outQue    = NamedCommon.reply2Q;
        String incorrid  = NamedCommon.CorrelationID;
        while (incorrid.contains(".")){ incorrid = incorrid.replace(".", "_"); }

        switch (tType) {
            case "01":
                if (mTask.equals("010")) {
                    theQue = uSubscriber.GetQue(fnd)+"_001";
                } else {
                    theQue = uSubscriber.GetQue(fnd)+"_000";
                }
                break;
            case "05":
                int lastQ=0, thisQ=0;
                try {
                    lastQ = uSubscriber.GetUsed(fnd);
                } catch (NumberFormatException nfe) {
                    uSubscriber.SetUsed(fnd, String.valueOf(lastQ));
                }
                thisQ = lastQ + 1;
                uCommons.uSendMessage("Handle uRest() last="+lastQ+"  this="+thisQ);
                if (thisQ > uSubscriber.GetMax(fnd)) thisQ = 1;
                uSubscriber.SetUsed(fnd, String.valueOf(thisQ));
                String qNbr = uCommons.RightHash("000"+thisQ, 3);
                theQue = uSubscriber.GetQue(fnd) + qNbr;
                break;
            default:
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "Unhandled task arrived at topic: task="+mTask;
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return;
        }

        if (outQue.startsWith("temp:")) tempFlag = "ACK";

        uCommons.uSendMessage("Hop.start()");
        Hop.start(theMessage, remotebkr, localbkr, theQue, tempFlag, incorrid);
    }

    private String getPropertyNames(Message message) throws JMSException {
        String props = "";
        @SuppressWarnings("unchecked")
        Enumeration properties = message.getPropertyNames();
        while (properties.hasMoreElements()) {
            String propKey = String.valueOf(properties.nextElement());
            props += propKey + "=" + message.getStringProperty(propKey) + " ";
        }
        return props;
    }

    public void setSelector(String selector) {
        this.selector = selector;
    }

    public boolean isDestinationTopic() {
        return isDestinationTopic;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public String getSelector() {
        return selector;
    }

    public String getClientId() {
        return clientId;
    }

    public void setDestinationTopic(boolean isDestinationTopic) {
        this.isDestinationTopic = isDestinationTopic;
    }

    public void setDestinationName(String destinationName) {
//        String pfx = "VirtualTopic.";
//        if (!destinationName.startsWith(pfx)) destinationName = pfx+destinationName;
        this.destinationName = destinationName;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setInbound(String inbound) {
        this.inbound = inbound;
    }

    public String getInbound() {
        return this.inbound;
    }
}
