package com.unilibre.commons;

import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;

public class pmqCommons {

    private static String url = "";
    public static ActiveMQConnectionFactory pmqFactory = null;
    public static Connection pmqConnection = null;
    public static Session pmqSession = null;
    public static Destination pmqDest = null;
    public static MessageProducer producer = null;
    public static boolean resetting = false;
    private static int mCounter=1;
    private static String lastBrk="";
    private static String lastSend2="";

    public static ActiveMQConnectionFactory pGetMQFactory(String BkrUrl) {
        if (NamedCommon.debugging) uCommons.uSendMessage("pGetMQFactory(\"" + BkrUrl + "\")");
        pmqFactory = new ActiveMQConnectionFactory(BkrUrl);
        pmqFactory.setUserName(NamedCommon.bkr_user);
        pmqFactory.setPassword(NamedCommon.bkr_pword);
        pmqFactory.setClientID(NamedCommon.inputQueue + ":Reply:" + NamedCommon.hostname + "--" + NamedCommon.pid + "--" + mCounter);
        mCounter++;
        url = BkrUrl;
        if (NamedCommon.debugging) uCommons.uSendMessage("pGetMQFactory(end): setClientID: "+pmqFactory.getClientID());
        return pmqFactory;
    }

    public static Connection pGetConnection(String BkrUrl, String queue) {
        if (NamedCommon.debugging) uCommons.uSendMessage("pGetConnection((\""+BkrUrl+"\", \""+queue+"\")");
        if (NamedCommon.ZERROR) return pmqConnection;
        try {
            pmqFactory = null;
            if (NamedCommon.debugging) uCommons.uSendMessage("pGetConnection needs a new factory");
            pmqFactory = pGetMQFactory(BkrUrl);
            pmqConnection = pmqFactory.createConnection();
            pmqConnection.start();
            if (NamedCommon.debugging) uCommons.uSendMessage("Connection started.");
        } catch (JMSException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "pmqCommons.pGetConnection - " + e.getMessage();
            uCommons.uSendMessage(NamedCommon.Zmessage);
            if (NamedCommon.ZERROR && !resetting) {
                System.out.println(" ");
                uCommons.uSendMessage("**************************** MQ ERROR ********************************");
                uCommons.uSendMessage("Connection to URL "+NamedCommon.messageBrokerUrl+" has failed.");
                uCommons.uSendMessage("******* Let's try again. *******");
                uCommons.Sleep(1);
                NamedCommon.ZERROR = pmqReconnect(BkrUrl, queue);
            }
            if (NamedCommon.ZERROR) return null;
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("pGetConnection(end)");
        return pmqConnection;
    }

    public static Session pGetMQsession(String BkrUrl, String queue) {
        if (NamedCommon.debugging) uCommons.uSendMessage("pGetMQsession(\""+BkrUrl+"\", \""+queue+"\")");
        pmqSession = null;
        if (pmqConnection == null) {
            if (NamedCommon.debugging) uCommons.uSendMessage("pGetMQsession needs a new connection");
            pmqConnection = null;
            pmqFactory = null;
            pmqConnection = pGetConnection(BkrUrl, queue);
        }
        if (NamedCommon.ZERROR) return pmqSession;
        boolean transacted = false, conIssue=false;
        int ackMode = Session.AUTO_ACKNOWLEDGE;
        try {
            pmqSession = pmqConnection.createSession(transacted, NamedCommon.MQackMode);
            if (NamedCommon.debugging) uCommons.uSendMessage("Session created.");
        } catch (JMSException e) {
            conIssue = true;
            NamedCommon.Zmessage = "pmqCommons.pGetMQsession - " + e.getMessage();
        } catch (NullPointerException npe) {
            conIssue = true;
            NamedCommon.Zmessage = "NullPointerException on the MQ-Connection handle";
        }
        if (conIssue) {
            NamedCommon.ZERROR = true;
            uCommons.uSendMessage(NamedCommon.Zmessage);
            if (NamedCommon.ZERROR && !resetting) {
                uCommons.uSendMessage("---------------------------------------------------");
                uCommons.uSendMessage("------       Attempt MQ re-connection        ------");
                uCommons.uSendMessage("---------------------------------------------------");
                NamedCommon.ZERROR = pmqReconnect(BkrUrl, queue);
            }
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("pGetMQsession(end)");
        return pmqSession;
    }

    public static Destination pGetQueue(String BkrUrl, String que) {
        if (NamedCommon.debugging) uCommons.uSendMessage("pGetQueue(\""+BkrUrl+"\", \""+que+"\")");
        if (pmqSession == null) {
            if (NamedCommon.debugging) uCommons.uSendMessage("pGetQueue needs a new session");
            pmqSession = pGetMQsession(BkrUrl, que);
        }
        if (NamedCommon.ZERROR) return pmqDest;
        if (pmqSession == null) {
            uCommons.uSendMessage("pGetMQsession has failed. ABORT now.");
            System.exit(0);
        }
        pmqDest = null;
        int tries = 0;
        boolean stopLoop = false;
        while (!stopLoop) {
            tries++;
            if (tries <= 2) {
                try {
                    pmqDest = pmqSession.createQueue(que);
                    if (NamedCommon.debugging) uCommons.uSendMessage("Destination queue created.");
                    stopLoop = true;
                } catch (JMSException e) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "pmqCommons.pGetQueue - " + e.getMessage();
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    NamedCommon.ZERROR = pmqReconnect(BkrUrl, que);
                    if (!NamedCommon.ZERROR) {
                        stopLoop = true;
                        NamedCommon.Zmessage = "";
                    }
                } catch (NullPointerException npe) {
                    uCommons.uSendMessage(npe.getMessage());
                    if (NamedCommon.ZERROR && !resetting) {
                        NamedCommon.ZERROR = !pmqReconnect(BkrUrl, que);
                    }
                }
            } else {
                stopLoop = true;
                CloseConnection();
            }
        }
        if (NamedCommon.ZERROR) return pmqDest;
        if (NamedCommon.debugging) uCommons.uSendMessage("pGetQueue(end)");
        return pmqDest;
    }

    public static MessageProducer GetProducer(String BkrUrl, String qName) {
        if (NamedCommon.debugging) uCommons.uSendMessage("GetProducer((\""+BkrUrl+"\", \""+qName+"\")");
        producer = null;
        if (pmqSession == null) pmqDest = null;
        if (pmqDest == null || !lastSend2.equals(qName)) {
            if (NamedCommon.debugging) uCommons.uSendMessage("GetProducer needs a new destination queue");
            pmqDest = pGetQueue(BkrUrl, qName);
        } else {
            try {
                pmqDest = pmqSession.createQueue(qName);
            } catch (JMSException e) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "pmqCommons.GetProducer - " + e.getMessage();
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return null;
            }
        }
        if (NamedCommon.ZERROR) {
            return null;
        } else {
            try {
                producer = pmqSession.createProducer(pmqDest);
                if (NamedCommon.debugging) uCommons.uSendMessage("Producer created.");
            } catch (JMSException e) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "pmqCommons.GetProducer - " + e.getMessage();
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return null;
            }
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("GetProducer(end)");
        return producer;
    }

    public static boolean SendMessage(String message, String BkrUrl, String qname, String replyto, String corrID) {
        if (NamedCommon.debugging) uCommons.uSendMessage("SendMessage(\""+BkrUrl+"\", \""+qname+"\", \""+replyto+"\", \""+corrID+"\")");
        resetting = false;
        Destination toQ = null;
        boolean status = true;
        if (!lastBrk.equals(BkrUrl)) {
            if (NamedCommon.debugging) {
                uCommons.uSendMessage("*****************");
                uCommons.uSendMessage("New MQ Broker - reset all connections.");
                uCommons.uSendMessage("*****************");
                lastSend2 = qname;
            }
            if (producer != null) {
                try {
                    producer.close();
                    pmqSession.close();
                    pmqConnection.close();
                    producer = null;
                    pmqSession = null;
                    pmqConnection = null;
                    pmqFactory = null;
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        }
        if (producer == null || !lastSend2.equals(qname)) {
            if (!lastSend2.equals(qname) && NamedCommon.debugging) {
                uCommons.uSendMessage("*****************");
                uCommons.uSendMessage("New replyTo queue - reset destination and close producer.");
                uCommons.uSendMessage("*****************");
            }
            if (producer != null) {
                try {
                    producer.close();
                    if (NamedCommon.debugging) uCommons.uSendMessage("Previous producer.closed");
                } catch (JMSException e) {
                    uCommons.uSendMessage("Producer.close() ERROR: " + e.getMessage());
                }
            }
            pmqDest  = null;
            producer = null;
            String errMsg = NamedCommon.Zmessage;
            boolean errSW = NamedCommon.ZERROR;
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage = "";
            if (NamedCommon.debugging) uCommons.uSendMessage("SendMessage needs a new message producer");
            producer = GetProducer(BkrUrl, qname);
            if (NamedCommon.ZERROR) {
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return false;
            }
            NamedCommon.ZERROR   = errSW;
            NamedCommon.Zmessage = errMsg;
        }
        if (!replyto.isEmpty()) toQ = pGetQueue(BkrUrl, replyto);
        try {
            TextMessage txtMessage = pmqSession.createTextMessage();
            txtMessage.setText(message);
            txtMessage.setJMSReplyTo(toQ);
            txtMessage.setJMSCorrelationID(corrID);
            producer.send(txtMessage);
            txtMessage.clearBody();
            txtMessage.clearProperties();
            if (NamedCommon.debugging) uCommons.uSendMessage("Message sent");
        } catch (JMSException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = e.getMessage();
            uCommons.uSendMessage(NamedCommon.Zmessage);
            status = false;
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("SendMessage(end)");
        lastBrk = BkrUrl;
        lastSend2 = qname;
        return status;
    }

    public static boolean pmqReconnect(String BkrUrl, String qname) {
        uCommons.uSendMessage("-------------- pmqReconnect("+BkrUrl+", "+qname+") --------------");
        uCommons.uSendMessage("-------------- disconnect old");
        resetting = true;
        boolean okay = true;
        CloseConnection();
        uCommons.uSendMessage("-------------- re-connect new");
        if (producer == null) producer = GetProducer(BkrUrl, qname);
        if (producer == null) okay = false;
        return okay;
    }

    public static void CloseConnection() {
        String clID = "";
        if (pmqFactory != null) clID = pmqFactory.getClientID();
        uCommons.uSendMessage("  ---  Closing " + clID);
        try {
            if (producer != null)       producer.close();
            if (pmqSession != null)     pmqSession.close();
            if (pmqConnection != null)  pmqConnection.close();
        } catch (JMSException e) {
            uCommons.uSendMessage("Close pMQ ERROR: "+e.getMessage());
        }
        pmqFactory = null;
        pmqConnection = null;
        pmqSession = null;
        pmqDest = null;
        producer = null;
    }

}
