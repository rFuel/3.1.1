package com.unilibre.rfuel;

import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.commons;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import org.checkerframework.common.value.qual.ArrayLen;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class uMessageRouter implements MessageListener {
    private static String url, usr, pwd, srcQueue, desQueue, bkr;
    private static ArrayList<String> btasks;
    private static ArrayList<String> Queues;
    private static int pause=0;

    public static void main(String[] args) throws JMSException {
        for (int i=0 ; i < 5; i++) { System.out.println(""); }
        //
        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
            String slash = "/";
            NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
        }
        NamedCommon.artemis = false;
        //
        Properties sysProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/this.server");
        bkr = sysProps.getProperty("brokers", "");
        if (bkr.equals("")) System.exit(1);
        Properties bkrProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/"+bkr);
        url = bkrProps.getProperty("url", "");
        usr = bkrProps.getProperty("bkruser", "");
        pwd = bkrProps.getProperty("bkrpword", "");
        String[] junk = bkrProps.getProperty("tasks", "").split("\\,");
        btasks = new ArrayList<>(Arrays.asList(junk));
        junk   = bkrProps.getProperty("qname", "").split("\\,");
        Queues = new ArrayList<>(Arrays.asList(junk));
        try {
            pause = Integer.valueOf(System.getProperty("pause", "0"));
        } catch (NumberFormatException nfe) {
            pause = 0;
        }
        if (usr.startsWith("ENC(")) usr = uCipher.Decrypt(usr);
        if (pwd.startsWith("ENC(")) pwd = uCipher.Decrypt(pwd);
        srcQueue = System.getProperty("from", "");
        desQueue = System.getProperty("to", "");
        uCommons.uSendMessage("Create connection to queue: " + srcQueue + "  on broker: " + url);
        activeMQ.SetDocker(false);
        activeMQ.SetHost("");
        NamedCommon.bkr_url     = url;
        NamedCommon.bkr_user    = usr;
        NamedCommon.bkr_pword   = pwd;
        new uMessageRouter();
    }

    public uMessageRouter() throws JMSException {
        MessageConsumer consumer = activeMQ.rfConsumer(url, usr, pwd, "ReRoute-consumer", srcQueue);
        if (commons.ZERROR) System.exit(1);
        consumer.setMessageListener(this);
        //
        // listens on conumer queue and sends to producer queue
    }

    public void onMessage(Message message) {
        boolean okay=false;
        if (message instanceof TextMessage) okay=true;
        if (!okay) return;
        String msg = "", correl = "", target = desQueue;
        try {
            msg = ((TextMessage) message).getText();
            uCommons.MessageToAPI(msg);
            if (target.toLowerCase().equals("message")) {
                target  = APImsg.APIget("task");
                int fnd = btasks.indexOf(target);
                if (fnd < 0) {
                    target = "";
                } else {
                    target = Queues.get(fnd) + "_000";
                }
            }
            correl = APImsg.APIget("correlationid");
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        if (!target.equals("")) {
            Hop.start(msg, bkr, bkr, target, "ACK", correl);
        } else {
            uCommons.uSendMessage("No task in message - sending back to RunERRORS!");
            Hop.start(msg, bkr, bkr, "RunERRORS", "ACK", correl);
        }
        FinaliseMessage(message);
        if (NamedCommon.ZERROR) {
            NamedCommon.ZERROR = false;
            uCommons.uSendMessage(NamedCommon.Zmessage);
            NamedCommon.Zmessage = "";
        }
        uCommons.Sleep(pause);
    }

    private void FinaliseMessage(Message message) {
        if (!NamedCommon.ZERROR) {
            try {
                message.acknowledge();
            } catch (JMSException e) {
                uCommons.uSendMessage("ERROR: " + e.getMessage());
            }
        } else {
            if (mqCommons.cmqSession != null) {
                try {
                    mqCommons.cmqSession.recover();
                    uCommons.Sleep(0);
                } catch (JMSException e) {
                    uCommons.uSendMessage("ERROR with JMS consumer session - cannot recover().");
                    uCommons.uSendMessage(e.getMessage());
                }
            }
        }
    }

}
