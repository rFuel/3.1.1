package com.unilibre.kafka;


import com.unilibre.MQConnector.commons;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONObject;
import javax.jms.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.Callable;

public class kafka2amq implements Callable {

    private final String DELIM = "<im>", LOAD="Load", SEP="--";
    private ArrayList<String> logEvents, reqKeys, reqVals, failures;
    private String mqQue, mqUsr, mqPwd, mqCli, mqType, mqUrl, mqTopic, mTemplate, processor;
    private String CorrelationID;
    private int deliveryMode, pcnt;
    private ActiveMQConnectionFactory mqFactory;
    private Connection mqConnection = null;
    private MessageProducer mqProducer;
    private Session mqSession;
    private Destination mqDest;
    private TextMessage msg;
    private boolean isArtemis, mqvTopic, isConnectedMQ;

    public kafka2amq(int tno) {
        this.logEvents  = new ArrayList<>();
        this.reqKeys    = new ArrayList<>();
        this.reqVals    = new ArrayList<>();
        this.failures   = new ArrayList<>();
        this.mqUrl      = "";
        this.mqQue      = "";
        this.mqUsr      = "";
        this.mqPwd      = "";
        this.mqCli      = "";
        this.mqType     = "";
        this.mqTopic    = "";
        this.mTemplate  = "";
        this.processor  = "K2MQ-" + tno;
        this.CorrelationID="";
        this.deliveryMode= 1;
        this.pcnt       = 100;
        this.mqFactory  = null;
        this.mqConnection=null;
        this.mqSession  = null;
        this.mqDest     = null;
        this.mqProducer = null;
        this.isArtemis  = false;
        this.mqvTopic   = false;
        this.isConnectedMQ=false;
        if (mqType.toLowerCase().equals("artemis")) this.isArtemis  = true;
    }

    public ArrayList<String> JobControl() {
        jobHandler();
        DisconnectMQ();
        return failures;
    }

    public Object call() {
        jobHandler();
        DisconnectMQ();
        return failures;
    }

    private void jobHandler() {
        if (!isConnectedMQ) ConnectMQ();
        if (!isConnectedMQ) {
            SendMessage("FAIL: no connection with AMQ.");
            return;
        }
        String message = "";
        int eoi = logEvents.size();
        SendMessage("Starting now - process "+ eoi+" items");
        for (int m=0 ; m<eoi ; m++) {
            message = logEvents.get(m);
            DoMessage(message);
        }
    }

    private void DoMessage(String line) {
        String passport, issuer, logid, account, file, itemId, record, payload, message;
        JSONObject obj = new JSONObject(line);
        Iterator<String> keys = obj.keys();
        String key, val;
        while(keys.hasNext()) {
            key = keys.next();
            val = obj.getString(key);
            reqKeys.add(key.toUpperCase());
            reqVals.add(val);
        }
        // these properties are NEVER encrypted -----------
        String eventTime    = GetJSONvalue(obj,"time");
        String eventDate    = GetJSONvalue(obj,"date");
        if (eventDate.equals("") || eventTime.equals("")) {
            String dts = GetJSONvalue(obj,"dts");
            eventDate = dts.substring(0,7);
            eventTime = dts.substring(8,99);
        }
        // ------------------------------------------------
        passport = GetJSONvalue(obj, "passport");               // MUST get this first for decryption
        issuer   = GetJSONvalue(obj,"issuer");
        logid    = GetJSONvalue(obj,"sourceinstance");
        account  = GetJSONvalue(obj,"sourceaccount");
        file     = GetJSONvalue(obj,"file");
        itemId   = GetJSONvalue(obj,"item");
        record   = GetJSONvalue(obj,"record");
        // ------------------------------------------------
        String timedate = eventDate.replaceAll("\\-", "");
        timedate+= eventTime.replaceAll("\\:", "");


        payload  = account + DELIM + file + DELIM + itemId + DELIM + record;
        CorrelationID = LOAD + SEP + logid + SEP + account + SEP + file + SEP + itemId;

        message     = mTemplate;
        message = Resolve(message, "$map$", file);
        message = Resolve(message, "$passport$", passport);
        message = Resolve(message, "$issuer$", issuer);
        message = Resolve(message, "$item$", itemId);
        message = Resolve(message, "$dacct$", account);
        message = Resolve(message, "$loadts$", timedate);
        message = Resolve(message, "$logid$", logid);
        message = Resolve(message, "$record$", payload);

        boolean proceed = SendToMQ(message);
        // AMQ drop-out - CANNOT lose this message
        //              - must handle here so kStream can Commit the read.
        while (!proceed) {
            // Step 1: retry sending 5 times
            int tryCnt=1;
            while (!proceed && tryCnt < 5) {
                DisconnectMQ();
                Sleep(2);                   // 2 seconds !!
                ConnectMQ();
                Sleep(0);                   // 0.5 second.
                proceed = SendToMQ(message);
                tryCnt++;
            }
            // Step 2: AMQ is down - add the message to failures list
            if (!proceed) failures.add(message);
        }
    }

    private void Sleep(int s) {
        if (s ==0) {
            s = 500;
        } else {
            s = s * 1000;
        }
        try {
            Thread.sleep(s);
        } catch (InterruptedException e) {
            // do nothing.
        }
    }

    private String GetJSONvalue(JSONObject obj, String property) {
        String ans = "";
        if (reqKeys.indexOf(property.toUpperCase()) >= 0) {
            ans = reqVals.get(reqKeys.indexOf(property.toUpperCase()));
        }
        return ans;
    }

    private boolean SendToMQ(String message) {
        boolean mqPassed = false;
        try {
            msg = mqSession.createTextMessage(message);
            msg.setJMSDestination(mqDest);
            msg.setJMSCorrelationID(CorrelationID);
            mqProducer.send(msg);
            mqPassed = true;
            msg = null;
        } catch (JMSException e) {
            SendMessage("******************************************************");
            SendMessage(e.getMessage());
        }
        return mqPassed;
    }

    private void SendMessage(String msg) {
        System.out.println(new Date() + " " + processor + " " + msg);
    }

    private String Resolve(String inmsg, String repl, String invar) {
        while (inmsg.contains(repl)) {
            inmsg = inmsg.replace(repl, invar);
        }
        return inmsg;
    }

    private void ConnectMQ () {
        try {
            mqFactory = new ActiveMQConnectionFactory(mqUrl);
            mqFactory.setUserName(mqUsr);
            mqFactory.setPassword(mqPwd);
            mqFactory.setClientID( mqCli);
            mqConnection = mqFactory.createConnection();
            mqSession = mqConnection.createSession(false, commons.GetAckMode());
            mqDest = mqSession.createQueue(mqQue);
            mqProducer = mqSession.createProducer(mqDest);
            mqProducer.setDeliveryMode(deliveryMode);
            isConnectedMQ = true;
        } catch (JMSException e) {
            SendMessage("******************************************************");
            SendMessage(e.getMessage());
            isConnectedMQ = false;
        }
    }

    private void DisconnectMQ() {
        try {
            if (mqProducer != null) {
                mqProducer.close();
                mqProducer = null;
            }
            if (mqDest != null) mqDest = null;
            if (mqSession != null) {
                mqSession.close();
                mqSession = null;
            }
            if (mqConnection != null) {
                mqConnection.close();
                mqConnection = null;
            }
            if (mqFactory != null) mqFactory = null;
            isConnectedMQ = false;
        } catch (JMSException e) {
            SendMessage("******************************************************");
            SendMessage(e.getMessage());
        }

    }

    public void SetMQurl(String inval) { mqUrl = inval; }
    public void SetMQque(String inval) { mqQue = inval; }
    public void SetMqusr(String inval) { mqUsr = inval; }
    public void SetMqpwd(String inval) { mqPwd = inval; }
    public void SetMQcli(String inval) { mqCli = inval; }
    public void SetMQtopic(String inval) {mqTopic = inval; }
    public void SetTemplate(String inval) {mTemplate = inval; }
    public void SetLogEvents(ArrayList<String> inval) { logEvents = inval; }

}
