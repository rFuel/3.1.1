package com.unilibre.MQConnector;



import com.unilibre.cipher.uCipher;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;


public class FwdMessage {

    private static String brkUrl = "";
    private static String brkUsr = "";
    private static String brkPwd = "";
    private static String inMsg  = "";
    private static int AUTO_ACKNOWLEDGE = 1;
    private static int CLIENT_ACKNOWLEDGE = 2;

    public static void main(String[] args) {
        String broker = System.getProperty("broker", "");
        String dropQue = System.getProperty("queue", "");

        if (broker.equals("")) {
            System.out.println("No broker provided. Stopping now.");
            System.exit(0);
        }

        if (dropQue.equals("")) {
            System.out.println("No dropQue provided. Stopping now.");
            System.exit(0);
        }

        System.out.println("Setup for Broker: " + broker);
        String vtQueue = "VirtualTopic.rFuel";
        LoadBrokerProperties(broker);

        System.out.println("Connect to: " + brkUrl);
        ActiveMQConnectionFactory mqFactory = null;
        Connection mqConnection = null;
        Session mqSession = null;
        Destination VTQ = null;
        MessageProducer mqProducer = null;

        System.out.println("Forward to: " + vtQueue);
        System.out.println("listening for messages on " + dropQue);

        if (commons.GetAckMode() == 0) commons.SetAckMode(CLIENT_ACKNOWLEDGE);

        try {
            mqFactory = new ActiveMQConnectionFactory(brkUrl);
            mqFactory.setUserName(brkUsr);
            mqFactory.setPassword(brkPwd);
            mqFactory.setClientID("FwdMsg-4-"+dropQue);
            mqConnection = mqFactory.createConnection();
            mqSession = mqConnection.createSession(false, commons.GetAckMode());
            VTQ = mqSession.createTopic(vtQueue);
            mqProducer = mqSession.createProducer(VTQ);
            mqProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        } catch (JMSException e) {
            e.printStackTrace();
        }

        while (true) {
            try {
                inMsg = activeMQ.consume(brkUrl, brkUsr, brkPwd, "FwdMessage", dropQue);
                if (inMsg.toLowerCase().equals("stop")) System.exit(0);
                if (inMsg.length() > 1) {
                    TextMessage txtMessage = mqSession.createTextMessage();
                    txtMessage.setText(inMsg);
                    mqProducer.send(txtMessage);
                    System.out.println("Forwarded: " + inMsg);
                }
            } catch (JMSException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

    private static void LoadBrokerProperties(String broker) {
        String runDir = System.getProperty("user.dir");
        String pfx="", record="", line, key, val;
        if (runDir.toLowerCase().contains("/home/andy")) pfx = "\\\\rfuel22\\all\\upl\\";
        record = commons.ReadDiskRecord(pfx + "conf/"+broker);
        if (record.equals("")) return;
        String[] lines = record.split("\\r?\\n");
        int eol = lines.length, eqPos=0;
        for (int l=0 ; l < eol ; l++) {
            line= lines[l];
            eqPos = line.indexOf("=");
            if (line.contains("=") && eqPos < line.length()) {
                key = line.substring(0, eqPos);
                val = line.substring(eqPos+1, line.length());
                if (key.toLowerCase().equals("url")) {
                    if (val.startsWith("ENC(")) val = Decode(val);
                    brkUrl = val;
                }
                if (key.toLowerCase().equals("bkruser")) {
                    if (val.startsWith("ENC(")) val = Decode(val);
                    brkUsr = val;
                }
                if (key.toLowerCase().equals("bkrpword")) {
                    if (val.startsWith("ENC(")) val = Decode(val);
                    brkPwd = val;
                }
                if (!brkUrl.equals("") && ! brkUsr.equals("") && !brkPwd.equals("")) break;
            }
        }
    }

    private static String Decode(String val) {
        val = val.substring(4, val.length());
        val = val.substring(0, val.length() - 1);
        val = uCipher.Decrypt(val);
        return val;
    }
}
