package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED  */


import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.artemisMQ;
import com.unilibre.MQConnector.commons;

import javax.jms.*;
import java.util.Properties;

public class Hop {

    public static MessageConsumer consumer;
    public static String reply;
    public static String reply2q;
    public static String message = "";
    private static boolean showSend = true;

    public static void start(String msg, String thisbkr, String remotebkr, String toQue, String outQue, String incorrid) {

        if (msg.equals("")) {
            uCommons.uSendMessage("-------------------------------------------------------");
            uCommons.uSendMessage("No message to send");
            return;
        }

        if (NamedCommon.debugging)  uCommons.uSendMessage("Hop.start()");
        if (NamedCommon.sentU2) return;
        if (NamedCommon.Expiry != 0) activeMQ.SetExpiry(NamedCommon.Expiry);

        String bkrUrl = "";
        String broker = thisbkr;
        if (broker.equals("")) broker = remotebkr;

        boolean namedBkr = NamedCommon.bkr_url.contains("tcp:");
        namedBkr = (namedBkr || NamedCommon.bkr_url.contains("failover:"));
        namedBkr = (namedBkr || NamedCommon.bkr_url.contains("ssl:"));

        if (!namedBkr ) {
            if (NamedCommon.bkr_url.equals("")) {
                if (!broker.endsWith(".bkr")) broker += ".bkr";
                String locn = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash;
                Properties Props = uCommons.LoadProperties(locn + broker);
                uCommons.BkrCommons(Props);
//                bkrUrl = Props.getProperty("url", NamedCommon.messageBrokerUrl);
                NamedCommon.bkr_url = NamedCommon.messageBrokerUrl;
            }
            bkrUrl = NamedCommon.bkr_url;
        } else {
            bkrUrl = NamedCommon.messageBrokerUrl;
        }

        if (NamedCommon.artemis) {
            try {
                artemisMQ.produce(bkrUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, "", toQue, msg);
            } catch (JMSException e) {
                e.printStackTrace();
                System.exit(0);
            }
        } else {
            if (!NamedCommon.ZERROR) {
                if (NamedCommon.isNRT) activeMQ.SetDeliveryMode(2);
                if (NamedCommon.inputQueue.equals(NamedCommon.dbhost)) {
                    commons.inputQueue = "uRestResponder";
                } else {
                    commons.inputQueue = NamedCommon.inputQueue;
                }
                // rFuel tries to create the MQ Factory, Connection and Session objects ONCE;
                //      the Producer is closed and recreated for every message - goes to a new queue
                // So, if the toQue is a temp-queue, it MUST exist - already be created by the client process
                // DO NOT create the temp-queue here!
                if (NamedCommon.isDocker) {
                    activeMQ.SetHost(NamedCommon.hostname);
                } else {
                    activeMQ.SetHost("");
                }
                activeMQ.produce(bkrUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, incorrid, toQue, msg);
                if (commons.ZERROR) {
                    NamedCommon.ZERROR = commons.ZERROR;
                    NamedCommon.Zmessage = commons.ERRmsg;
                    activeMQ.CloseProducer();
                }
                commons.inputQueue = "";
            }
        }
        if (NamedCommon.ZERROR) {
//            pmqCommons.SendMessage("Error is    : " + NamedCommon.Zmessage + "\nMessage was: " + NamedCommon.message, bkrUrl, "RunERRORS", "", incorrid);
            activeMQ.produce(bkrUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, incorrid, toQue, "Error is    : " + NamedCommon.Zmessage + "\nMessage was : " + NamedCommon.message);
            String zmsg = NamedCommon.Zmessage;
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage = "";
            uCommons.uSendMessage("******************* Hop() did NOT reply. *******************");
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = zmsg;
            System.out.println(" ");
        } else {
            String xtra = "";
            if (NamedCommon.Expiry != 0) {
                int wrk = (int) (NamedCommon.Expiry / 1000);
                int mins= wrk / 60;
                int hrs = mins / 60;
                xtra = "   expires in "+mins+" minutes.";
            }
            if (showSend) uCommons.uSendMessage("Hop() sent to [" + toQue + "]     message ["+incorrid+"]"+xtra);
        }
    }

    public static void Restart() {
        if (NamedCommon.artemis) {
            // do artemis here
        } else {
            activeMQ.CloseProducer();
            activeMQ.CloseConsumer();
        }
    }

    public static void setShow(boolean val) {
        showSend = val;
    }

}
