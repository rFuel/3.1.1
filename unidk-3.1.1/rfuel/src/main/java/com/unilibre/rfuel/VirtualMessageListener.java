package com.unilibre.rfuel;

import com.unilibre.commons.*;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class VirtualMessageListener implements MessageListener {

    private int numReceived;
    private int numSent=0;
    private int maxProc=0;
    private boolean stopSW = false;
    private boolean isNRT  = false;
    private long lastActive = System.nanoTime();
    private final CountDownLatch done;
    private String brokerUri;
    private String topic;
    private static String defaultQ = "";
    private ArrayList<String> MSGkeys = new ArrayList<>();
    private ArrayList<String> MSGvals = new ArrayList<>();

    public VirtualMessageListener(CountDownLatch done) {
        this.done = done;
        GarbageCollector.setStart(System.nanoTime());
    }

    public int getNumReceived() {
        return numReceived;
    }

    public void setBroker(String broker, String topic) {
        brokerUri = broker;
        NamedCommon.bkr_url = brokerUri;
        NamedCommon.inputQueue = topic;
    }

    public void setMaxProcessed(int nbr) {
        maxProc = nbr;
    }

    public boolean getStopper() {
        return stopSW;
    }

    public long getActive() {
        return lastActive;
    }

    public void onMessage(Message message) {
        ++numReceived;
        done.countDown();
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";

        // -----------------------------------------/
        // Route the message to rFuel Queues ASAP   /
        // -----------------------------------------/

        NamedCommon.ZERROR = false;
        String theMessage = GetMessageText(message);
        if (theMessage.equals("")) {
            uCommons.uSendMessage("Message BODY was empty");
            return;
        }
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage(NamedCommon.Zmessage);
            return;
        }

        // kStreams pumps the messages into MQ as standard rFuel messages, not as json messages.

        if (uSubscriber.GetFormat().toLowerCase().equals("json") || theMessage.startsWith("{")) {
            if (NamedCommon.debugging) {
                uCommons.uSendMessage("Reformat message from JSON to rFuel structures ...");
                uCommons.uSendMessage("**");
                uCommons.uSendMessage(theMessage);
                theMessage = msgCommons.stringifyMessage(theMessage);
                uCommons.uSendMessage("**");
                uCommons.uSendMessage(theMessage);
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("..... Done.");
            } else {
                theMessage = msgCommons.stringifyMessage(theMessage);
                MSGLoader(theMessage);
            }
            if (theMessage.equals("")) return;
            lastActive = System.nanoTime();
        } else {
            if (NamedCommon.debugging) uCommons.uSendMessage("MessageToAPI()");
        }

        String mTask = MessageGet("task");
        NamedCommon.isWhse = true;
        NamedCommon.isRest = false;
        NamedCommon.isNRT  = false;
        switch (mTask) {
            case "010":
                NamedCommon.isWhse = true;
                defaultQ = "019_Finish";
                break;
            case "012":
                NamedCommon.isWhse = true;
                defaultQ = "019_Finish";
                break;
            case "014":
                NamedCommon.isWhse = true;
                defaultQ = "019_Finish";
                break;
            case "017":
                NamedCommon.isWhse = true;
                defaultQ = "019_Finish";
                break;
            case "022":
                NamedCommon.isNRT  = true;
                defaultQ = "019_Finish";
                break;
            case "050":
                NamedCommon.isRest = true;
                defaultQ = "019_DefaultReplies";
                break;
            case "055":
                NamedCommon.isRest = true;
                defaultQ = "019_DefaultReplies";
                break;
            case "090":
                defaultQ = "019_DefaultReplies";
                break;
            case "099":
                defaultQ = "019_DefaultReplies";
                break;
            default:
                defaultQ = "019_DefaultReplies";
                uCommons.uSendMessage("Errors are occuring:\n" + theMessage);
                int tryCnt=0;
                while (mTask.equals("")) {
                    NamedCommon.ZERROR   = false;
                    NamedCommon.Zmessage = "";
                    uCommons.Sleep(0);
                    tryCnt++;
                    if (tryCnt > 20) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "Cannot get the \"task\" from this message :-";
                        uCommons.uSendMessage(NamedCommon.Zmessage);
                        uCommons.uSendMessage(theMessage);
                        return;
                    }
                    uCommons.uSendMessage("Try # " + tryCnt + " - cound not find the task key:value pair.");
                    //
                    // retry to pull the message from AMQ message object
                    //
                    theMessage = GetMessageText(message);
                    if (NamedCommon.ZERROR) {
                        uCommons.uSendMessage(NamedCommon.Zmessage);
                        return;
                    }
                    mTask = MessageGet("task");
                }
        }


        isNRT  = mTask.equals("022");
        int fnd = uSubscriber.FindTask(mTask);
        if (fnd < 0 && !mTask.startsWith("9")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Unrecognised task arrived at topic: task=" + mTask;
            uCommons.uSendMessage(NamedCommon.Zmessage);
            uCommons.uSendMessage(theMessage);
            return;
        }

        if (NamedCommon.debugging) uCommons.uSendMessage("Route: task=" + mTask);
        String tType = mTask.substring(0, 2);
        String theQue = "", qNbr = "";
        String tempFlag = "";
        String localbkr = brokerUri;
        String remotebkr = "";
        String outQue = "";
        String ProcType="";
        String jmsCorrel = "";
        String jmsReply2 = "";
        try {
            jmsCorrel = message.getJMSCorrelationID();
            if (message.getJMSReplyTo() != null) jmsReply2 = message.getJMSReplyTo().toString();
            if (jmsReply2.startsWith("queue://")) jmsReply2 = jmsReply2.substring(8, jmsReply2.length());
        } catch (JMSException e) {
            jmsCorrel = "";
            jmsReply2 = "";
        }
        String msgReply2 = MessageGet("replyto");
        if (msgReply2.equals("")) {
            msgReply2 = jmsReply2;
            if (msgReply2.equals("")) {
                msgReply2 = defaultQ;
            }
            theMessage = "replyto<is>"+msgReply2+"<tm>" + theMessage;
        }
        String incorrid = MessageGet("correlationid");
        if (incorrid.equals("")) {
            incorrid = jmsCorrel;
            theMessage = "correlationid<is>"+incorrid+"<tm>" + theMessage;
        }
        while (incorrid.contains(".")) {
            incorrid = incorrid.replace(".", "_");
        }
        boolean sendit = true;
        
        if (tType.equals("01")) {
            ProcType = "uBulk";
        } else if (tType.equals("02")) {
            ProcType = "uBulk";
        } else if (tType.equals("05")) {
            ProcType = "uRest";
        } else if (tType.equals("99")) {
            ProcType = "uAdmin";
        }

        switch (ProcType) {
            case "uBulk":
                qNbr = GetNextQue(fnd);
                theQue = uSubscriber.GetQue(fnd) + "_" + qNbr;
                break;
            case "uRest":
                // round robin assignment of queues
                qNbr = GetNextQue(fnd);
                theQue = uSubscriber.GetQue(fnd) + "_" + qNbr;
                break;
            case "uAdmin":
                int nbrMsgGroups = uSubscriber.tasks.size(), nbrQueues;
                String lastQ = "";
                if (NamedCommon.vtPing > 0) {
                    uCommons.uSendMessage("Ping Tests ...");
                    for (int i = 0; i < nbrMsgGroups; i++) {
                        for (int q = 0; q < uSubscriber.qMax.get(i); q++) {
                            qNbr = GetNextQue(i);
                            theQue = uSubscriber.GetQue(i) + "_" + qNbr;
                            if (!theQue.equals(lastQ)) {
                                Hop.start(theMessage, remotebkr, localbkr, theQue, tempFlag, incorrid);
                                lastQ = theQue;
                            }
                        }
                    }
                    sendit = false;
                } else {
                    sendit = true;
                    theQue = uSubscriber.GetQue(0) + "_001"; // que 001 of ANY task
                }
                break;
            default:
                if (tType.startsWith("9")) {
                    theQue = uSubscriber.GetQue(0) + "_001"; // que 001 of ANY task
                    fnd = uSubscriber.FindTask("010");
                    if (fnd < 0) fnd = uSubscriber.FindTask("050");
                    if (fnd < 0) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "Invalid ADMIN task: task=" + mTask;
                        uCommons.uSendMessage(NamedCommon.Zmessage);
                        return;
                    } else {
                        theQue = uSubscriber.GetQue(fnd) + "_001";
                    }
                } else {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Unhandled task arrived at topic: task=" + mTask;
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    return;
                }
        }

        if (jmsReply2.startsWith("ID:")) tempFlag = "ACK";

        if (NamedCommon.debugging) uCommons.uSendMessage("Hop.start()");
        if (sendit && !theQue.equals("")) {
            Hop.start(theMessage, remotebkr, localbkr, theQue, tempFlag, incorrid);
            GarbageCollector.CleanUp();
        }

        if (maxProc > 0) {
            if (numReceived > maxProc) stopSW = true;
        }

    }

    private String MessageGet(String key) {
        String value = "";
        String uKey = key.toUpperCase();
        int fnd = MSGkeys.indexOf(uKey);
        if (fnd > -1) value = MSGvals.get(fnd);
        return value;
    }

    private void MSGLoader(String message) {
        MSGkeys.clear();
        MSGvals.clear();
        message = message.replaceAll("\\r?\\n", " ");
        while (message.toLowerCase().contains("<is>")) {
            message = message.replace("<is>", "=");
            message = message.replace("<IS>", "=");
            message = message.replace("<Is>", "=");
            message = message.replace("<iS>", "=");
        }
        message = message.replace("<TM>", NamedCommon.sep);
        message = message.replace("<Tm>", NamedCommon.sep);
        message = message.replace("<tM>", NamedCommon.sep);

        String[] values = message.split(NamedCommon.sep);
        int nbrValues = values.length;
        int posx;

        String[] tmp = new String[2];
        String key="", val="";
        for (int i = 0; i < nbrValues; i += 1) {
            posx = values[i].indexOf("=");
            if (posx > 0) {
                key = values[i].substring(0, posx).replaceAll("\\ ", "");
                val = values[i].substring(posx + 1, values[i].length());
                MSGkeys.add(key.toUpperCase());
                MSGvals.add(val);
            }
        }
    }

    private String GetMessageText(Message message) {
        String ans = "";
        if (message instanceof TextMessage) {
            TextMessage inMsg = (TextMessage) message;
            try {
                ans = inMsg.getText();
            } catch (JMSException e) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "JMS exception thrown in GetMessageText(): " + e.getMessage();
            }
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Invalid message (datatype) received.";
        }
        MSGLoader("rfTag<is>true<tm>" + ans);
        return ans;
    }

    private String GetNextQue(int fnd) {
        // round-robin assignment of inbound messages.
        // Send to 1, 2, 3, and so on...             .
        // loop back to 1 when thisQ > maxQue        .
        int lastQ=0, thisQ=0, qLoad=uSubscriber.GetLoad(fnd), qMax=uSubscriber.GetMax(fnd);
        try {
            lastQ = uSubscriber.GetUsed(fnd);
        } catch (NumberFormatException nfe) {
            uSubscriber.SetUsed(fnd, String.valueOf(lastQ));
        }

        if (qLoad > 1) {
            // ------ load queues according to ratio in the bkr file
            // ------ e.g. "loads=1:4"
            // ------ So Q1 gets 1 and Q2 gets 4 ==> 4 docker listeners on Q2
            if (lastQ >= 1) {
                if (numSent >= qLoad && qLoad > 1) {
                    thisQ = 1;
                    numSent = 0;
                } else {
                    numSent++;
                    thisQ = lastQ + 1;
                    if (thisQ > qMax) thisQ = 2;    // do not go back to 1
                }
            } else {
                numSent = 0;
                thisQ = lastQ + 1;
            }
        } else {
            thisQ = lastQ + 1;
            if (thisQ > qMax) thisQ = 1;
        }
        if (thisQ > uSubscriber.GetMax(fnd)) thisQ = 1;
        uSubscriber.SetUsed(fnd, String.valueOf(thisQ));
        String qNbr = uCommons.RightHash("000" + thisQ, 3);
        return qNbr;
    }

}