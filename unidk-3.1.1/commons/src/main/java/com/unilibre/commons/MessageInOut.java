package com.unilibre.commons;


import com.unilibre.MQConnector.artemisMQ;
import javax.jms.Destination;
import javax.jms.JMSException;

public class MessageInOut {

    public static String tmp = "";
    public static String consClientID = "AutoTestCONS";
    public static String prodClientID = "AutoTestPROD";
    public static String sendQue = "";
    private static String consQue = "";
    public static String message = "";
    public static String messageBrokerUrl = "";
    private static boolean getReply = true;
    public static boolean debug = false;
    private static int pCnt=0;
    private static long lastActive;
    private static long rightNow;
    private static double laps;
    private static double div = 1000000000.00;

    public boolean ZERROR = false;
    public String Zmessage = "";
    private String filter = "", reply = "";
    private JMSConsumer jmsc=null;
    private JMSProducer jmsp=null;
    private Destination dest=null;

    public void SetFilter(String corrid) { this.filter = "JMSCorrelationID = '" + corrid + "'"; }

    public void SetReply(String inval) { reply = inval; }

    public void SetZerror(boolean err, String msg) { ZERROR = err; Zmessage = msg; }

    public void SetDestination(Destination d) { this.dest = d; }

    public String GetReply() { return reply; }

    public boolean isZERROR() { return ZERROR; }

    public String ErrorString () { return Zmessage; }

    public Destination GetDestination() { return  dest; }

    public String GetFilter() {
        return this.filter;
    }

    public String Send2rFuel(String bkrUrl, String que, String inMsg) {
        filter = "";
        if (NamedCommon.debugging) {
            uCommons.uSendMessage("MessageInOut.Send2rFuel() ---------------------------------------------- ");
            uCommons.uSendMessage(NamedCommon.CorrelationID);
        }
        messageBrokerUrl = bkrUrl;
        message = inMsg;
        sendQue = que;        // same queue: acts like an http call
        consQue = que;        // ----------------------------------
        reply = "";
        new MessageInOut();
        if (!message.trim().equals("")) reply = WaitForReply();
        NamedCommon.MQgarbo.gc();
        return reply;
    }

    public String SendAndReceive(String bkrUrl, String sQue, String cQue, String corID, String inMsg, String clId) {
        consClientID = "Cons-"+clId;
        prodClientID = "Prod-"+clId;

        consClientID = "Cons-"+corID;
        prodClientID = "Prod-"+corID;

        if (debug) NamedCommon.debugging = debug;

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("MessageInOut.SendAndReceive() ---------------------------------------------- ");
            uCommons.uSendMessage(NamedCommon.CorrelationID);
        }
        messageBrokerUrl = bkrUrl;
        message = inMsg;
        sendQue = sQue;    // acts like a round trip process
        consQue = cQue;    // ------------------------------
        if (!filter.startsWith("JMSC")) {
            filter = "JMSCorrelationID = '" + corID + "'";
            tmp    = corID.replaceAll("\\.", "_");
            if (!corID.equals(tmp)) {
                if (corID.contains(".")) filter += " OR JMSCorrelationID = '" + corID.replaceAll("\\.", "_") + "'";
            }
        }
        if (!NamedCommon.artemis) {
            reply = "";
        } else {
            reply = "No message received on "+consQue+" for "+filter;
        }

        ZERROR = false;
        reply = GetTheAnswer(bkrUrl, sQue, cQue, corID, inMsg, clId);
        filter = "";

        if (ZERROR) {
            NamedCommon.Zmessage = reply;
            NamedCommon.ZERROR = true;
        }
        return reply;
    }

    public String WaitForReply() {
        int wait4rfuel = 60;
        long startTime = System.nanoTime();
        uCommons.uSendMessage("WaitForReply("+filter+")");
        while (reply.equals("")) {
            reply = jmsc.reply;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                reply = e.getMessage();
            }
            rightNow = System.nanoTime();
            laps = (rightNow - startTime) / div;
            if (laps > wait4rfuel) {
                reply = "WaitForReply(): Connectivity issue. Is rFuel running?";
                ZERROR = true;
            }
        }
        return reply;
    }

    public MessageInOut() {
        this.reply = "";
        this.filter= "";
        this.jmsc = new JMSConsumer();
        this.jmsp = new JMSProducer();
        this.dest = null;
    }

    public String GetTheAnswer(String bkrUrl, String sQue, String cQue, String corID, String inMsg, String clId) {
        String answer = "";

        if (message.trim().equals("")) return answer;

        if (!ZERROR) {
            if (getReply) {
                if (NamedCommon.artemis) {
                    if (NamedCommon.debugging) uCommons.uSendMessage("Sending via ArtemisMQ");
                    try {
                        // please implement a "need2rest" block HERE !!
                        artemisMQ.cPrepare(messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, consQue);
                        if (artemisMQ.produce(messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, prodClientID, sendQue, message)) {
                            String ans = "";
                            int lCnt = 0;
                            while (ans.equals("")) {
                                lCnt++;
                                if (lCnt > 2) {
                                    answer = "No response from rFuel";
                                    break;
                                }
                                // this a "consume now" processor so loop until you get the answer
                                ans = artemisMQ.consume(messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, consClientID, consQue);
                                if (!ans.equals("")) answer += ans;
                            }
                        } else {
                            answer = "FAILED to create an Artemis producer";
                        }
                    } catch (JMSException e) {
                        uCommons.uSendMessage(e.getMessage());
                        return answer;
                    }
                } else {
                    if (NamedCommon.debugging) uCommons.uSendMessage("Sending via ActiveMQ");

                    jmsc.clID = consClientID;
                    jmsc.SetFilter(filter);
                    jmsc.Prepare(bkrUrl, consClientID);

                    jmsp.clID = prodClientID;
                    jmsp.PrepareConnector(bkrUrl, sQue);

                    if (filter.equals("")) {
                        answer = jmsc.consume(cQue);
                        jmsp.sendMsg(message);
                    } else {
                        jmsp.sendMsg(message);
                        answer = jmsc.consume(cQue);
                    }
                    // -----------------------------------------------------
                    // -----------------------------------------------------
                    if (ZERROR) answer = Zmessage;
                }
            }
        }
        return answer;
    }

    public void Shutdown(){
        if (!jmsp.FactoryIsNull()) jmsp.shutdown();
        if (!jmsc.FactoryIsNull()) jmsc.shutdown();
    }
}
