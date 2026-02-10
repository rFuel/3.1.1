package com.unilibre.rfuel;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import com.unilibre.cipher.uCipher;
import com.unilibre.commons.License;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import com.unilibre.core.coreCommons;

import java.util.ArrayList;
import java.util.Properties;

public class uSubscriber {

    // invoked from supervisor with this.conf
    // template is found in conf/vtopic.base

    private static String BkrUri;
    private static String BkrUsr;
    private static String BkrPwd;
    private static String VTopic;
    private static String mFormat;
    private static int VListeners;
    private static int maxProc=0;
    public static boolean firstTime = true;
    public static boolean stopping = false;
    public static boolean monitor = false;
    public static ArrayList<String> tasks = new ArrayList<>();
    public static ArrayList<String> qNames = new ArrayList<>();
    public static ArrayList<Integer> qMax  = new ArrayList<>();
    public static ArrayList<Integer> qLoad  = new ArrayList<>();
    public static ArrayList<Integer> LastUsed  = new ArrayList<>();

    public static void main(String[] args) throws Exception {

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
        }
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);

        uCommons.uSendMessage("*");
        uCommons.uSendMessage("*");
        uCommons.uSendMessage("----------------------------------------------------------------------------------");
        uCommons.uSendMessage("rFuel Virtual Topic subscriber");
        uCommons.uSendMessage("process task=[any]");
        uCommons.uSendMessage("     on  que=[any]");
        uCommons.uSendMessage("*");
        uCommons.uSendMessage("*");
        uCommons.uSendMessage("Starting uSubscriber()");

        mFormat = System.getProperty("format", "");
        String broker = System.getProperty("bkr", "");
        if (License.GetDomain("hostname")) NamedCommon.hostname = License.domain;
        uCommons.uSendMessage(NamedCommon.hostname + " will subscribe to broker ["+broker+"]");

        // Subscribe to a Virtual Topic on the Broker --------------------------
        //      Consume: set up listeners to recieve messages
        //      Produce: redirect the messages to their queue       Hop.Start
        // ---------------------------------------------------------------------
        SubscribeTo(broker);

        boolean stopNow = coreCommons.StopNow();
        if (stopNow) {
            System.out.println(" ");
            while (stopNow) {
                uCommons.uSendMessage("<<heartbeat>> Waiting for the pid to die.");
                uCommons.Sleep(2);
                System.exit(0);
            }
        }
        uCommons.uSendMessage("Stop");
        System.exit(0);
    }

    public static void SubscribeTo(String broker) throws Exception {
        if (!NamedCommon.ZERROR) GetDetails(broker);
        if (!NamedCommon.ZERROR) SetupTopicListeners();
    }

    public static void GetDetails(String broker) throws NumberFormatException {
        uCommons.uSendMessage("Loading broker details: "+NamedCommon.BaseCamp+"/conf/"+broker);

        if (!broker.endsWith(".bkr")) broker += ".bkr";
        Properties Props = uCommons.LoadProperties(NamedCommon.BaseCamp+"/conf/"+broker);
        if (NamedCommon.StopNow.contains("FAIL")) NamedCommon.ZERROR = true;
        if (NamedCommon.ZERROR) return;

        monitor = Props.getProperty("monitor", "false").toLowerCase().equals("true");

        BkrUri = Props.getProperty("url", "##");
        BkrUsr = Props.getProperty("bkruser", NamedCommon.bkr_user);
        if (BkrUsr.contains("ENC(")) {
            int lx = BkrUsr.indexOf("ENC(");
            String tmp = BkrUsr.substring(4, (BkrUsr.length() - 1));
            BkrUsr = uCipher.Decrypt(tmp);
        }
        BkrPwd = Props.getProperty("bkrpword", NamedCommon.bkr_pword);
        if (BkrPwd.contains("ENC(")) {
            int lx = BkrPwd.indexOf("ENC(");
            String tmp = BkrPwd.substring(4, (BkrPwd.length() - 1));
            BkrPwd = uCipher.Decrypt(tmp);
        }

        VTopic = Props.getProperty("topic", "");
        if (VTopic.equals("")) return;
        String nbrL = Props.getProperty("listeners", "0");
        VListeners = 0;
        try {
            VListeners = Integer.parseInt(nbrL);
        } catch (NumberFormatException nfe) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Invalid setting for \"listeners\" in Broker: "+broker;
        }

        nbrL = Props.getProperty("maxproc", "0");
        try {
            maxProc = Integer.parseInt(nbrL);
            if (maxProc > 0) uCommons.uSendMessage("************* Process " + maxProc + " messages then restart.");
        } catch (NumberFormatException nfe) {
            maxProc = 0;
        }

        String[]  btasks = Props.getProperty("tasks", "").split("\\,");
        String[]  Queues = Props.getProperty("qname", "").split("\\,");
        String[]  maxQs  = Props.getProperty("responders", "").split("\\,");
        String   loadChk = Props.getProperty("loads", "");

        if (loadChk.equals("")) {
            for (int i=0; i< btasks.length; i++) { loadChk += "1:1,"; }
            loadChk = loadChk.substring(0,loadChk.length()-1);
        }

        String[] qLoads = loadChk.split("\\,");
        String addTask = "";

        for (int tl=0; tl < btasks.length; tl++) {
            if (tasks.indexOf(btasks[tl]) < 0 && !btasks[tl].equals("")) {
                addTask = "  adding task: [" + btasks[tl] + "]";
                if (qLoads.length > tl) addTask += " at a ratio of: " + qLoads[tl];
                uCommons.uSendMessage(addTask);
                SetTask(btasks[tl]);
                SetQue(Queues[tl]);
                SetMax(maxQs[tl]);
                SetLoad(qLoads[tl]);
                AddUsed(tl, "0");
            }
        }
        uCommons.uSendMessage("*");
        uCommons.uSendMessage("=================================================");
        uCommons.uSendMessage("*");
    }

    private static void SetTask(String val) { tasks.add(val); }

    private static void SetQue(String val) { qNames.add(val); }

    private static void SetMax(String val) { qMax.add(Integer.parseInt(val)); }

    private static void SetLoad(String val) {
        int loader=0;
        try {
            loader = Integer.valueOf(val.split("\\:")[1]);
        } catch (NumberFormatException e) {
            uCommons.uSendMessage("Error in load property ["+val+"] is invalid.");
            loader=0;
        }
        qLoad.add(loader);
    }

    public static int FindTask(String task) { return tasks.indexOf(task); }

    private static void AddUsed(int pos, String val) { LastUsed.add(pos, Integer.parseInt(val)); }

    public static void SetUsed(int pos, String val) { LastUsed.set(pos, Integer.parseInt(val)); }

    public static String GetTask(int fnd) { return tasks.get(fnd); }

    public static String GetQue(int fnd) { return qNames.get(fnd); }

    public static int GetMax(int fnd) { return qMax.get(fnd); }

    public static Integer GetLoad(int fnd) {
        int ans = qLoad.get(fnd);
        if (ans ==0) ans = 1;
        return ans;
    }

    public static int GetUsed(int fnd) { return LastUsed.get(fnd); }

    private static void SetupTopicListeners() throws Exception {
        VirtualDestinationController dController = new VirtualDestinationController();
        dController.SetLatch(VListeners);
        dController.SetBroker(BkrUsr, BkrPwd, BkrUri, VListeners);
        dController.SetMaxProc(maxProc);
        uCommons.uSendMessage("Starting VirtualTopic."+VTopic+" with "+VListeners+" Consumers");
        dController.lastAction = System.nanoTime();
        boolean stopNow = dController.GetRestart();
        while (!stopNow) {
            dController.hbCnt = 1;
            dController.before(VTopic);     // startup MQ listeners
            dController.run();              // process and timeout management
            dController.after();            // closedown MQ objects
            stopNow = dController.GetRestart();
            if (!stopNow) {
                NamedCommon.Reset();
                uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
                Properties runProps = uCommons.LoadProperties("rFuel.properties");
                uCommons.SetCommons(runProps);
                uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
            }
            firstTime = false;
            dController.lastAction = System.nanoTime();
        }
        uCommons.uSendMessage("VirtualTopic."+VTopic+" will stop now.");
    }

    public static String GetFormat() {
        return mFormat;
    }

    public static String getTopic() {
        return VTopic;
    }

}
