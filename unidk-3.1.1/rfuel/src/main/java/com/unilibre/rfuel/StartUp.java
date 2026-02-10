package com.unilibre.rfuel;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.core.coreCommons;
import com.unilibre.core.h2dbServer;
import com.unilibre.core.responder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class StartUp {

    public static void main(String[] args) throws Exception {

        if (args.length > 0) {
            System.out.println("------------------------------------------------------------------");
            System.out.println("Ignoring the args passed in :-");
            for (int i=0 ; i< args.length; i++) {
                System.out.println((i+1) + "  " + args[i]);
            }
            System.out.println("------------------------------------------------------------------");
            uCommons.Sleep(2);
        }

        Process();          // Keeps the program running
    }

    private static void Process () {
        // ------- [DEV / PROD housekeeping -------------------------------------------
        if (NamedCommon.upl.equals("")) NamedCommon.upl = System.getProperty("user.dir");
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        if (NamedCommon.upl.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            NamedCommon.gmods = NamedCommon.BaseCamp + NamedCommon.slash + "lib" + NamedCommon.slash;
        }

        NamedCommon.masterStop = coreCommons.StopNow();
        if (NamedCommon.masterStop) uCommons.StopProcessNow();

        // -----------------------------------------------------------------------------

        String uTask = System.getProperty("task", "999");
        String uQue = System.getProperty("que", "1");
        NamedCommon.que = uQue;
        NamedCommon.Broker = System.getProperty("bkr", "");
        if (NamedCommon.Broker.equals("")) {
            Properties pChecker = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/this.server");
            NamedCommon.Broker = pChecker.getProperty("brokers", "");
            if (NamedCommon.Broker.equals("")) {
                uCommons.uSendMessage("[FATAL] Cannot find an AMQ broker in command string or in this.server.");
                return;
            }
            uCommons.uSendMessage("[INFO] Setting Broker to: "+NamedCommon.Broker);
        }

//        NamedCommon.debugging = true;
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        SourceDB.PreLoadAES();
        Properties bprops = uCommons.LoadProperties(NamedCommon.Broker);
        NamedCommon.IAM = "UNKNOWN";
        if (bprops.getProperty("tasks").indexOf(uTask) >= 0) {
            String[] tmpTasks = bprops.getProperty("tasks").split(",");
            String[] tmpNames = bprops.getProperty("qname").split(",");
            int tdx = Arrays.asList(tmpTasks).indexOf(uTask);
            String job = tmpNames[tdx];
            if (NamedCommon.isDocker) {
                job += "_" + NamedCommon.pid;
            } else {
                job += "_" + uCommons.RightHash("000" + uQue, 3);
            }
            NamedCommon.IAM = job;
            tmpNames=null;
            tmpTasks=null;
            job=null;
        }
        bprops =null;

        NamedCommon.debugging = System.getProperty("debug", "false").toLowerCase().equals("true");

        uCommons.uSendMessage("Checking that UPLCTL and TBL tables exist.");
        SqlCommands.ConnectSQL();
        if (NamedCommon.ZERROR) SqlCommands.ReconnectService();
        ArrayList<String> ddl = new ArrayList<>();
        String[] tCols = {"-TblKey", ".Old", "-New"};
        String createTable = SqlCommands.CreateTable(NamedCommon.rawDB, "upl", "TBL", tCols);
        ddl.add(createTable);
        tCols = null;
        tCols = new String[]{"-BatchID", "-RunType", "-Serial", "-Map", "-Task", "*Source", "*SelCmd", "*Target", "@rowsIN", "@rowsCHECKED", "@rowsPROCESSED", "@rowsEMPTY", "@rowsNULL", "@rows2BIG", "@rowsOUT", "@RunTime"};
        createTable = SqlCommands.CreateTable(NamedCommon.rawDB, "upl", "UPLCTL", tCols);
        ddl.add(createTable);
        tCols = null;
        SqlCommands.ExecuteSQL(ddl);
        ddl.clear();
        SqlCommands.DisconnectSQL();

        if (!uTask.startsWith("09")) {
            if (NamedCommon.Broker.equals("")) {
                responder.uTask = uTask;
                responder.GetBroker();
            }
            Properties props = uCommons.LoadProperties(NamedCommon.Broker);
            uCommons.BkrCommons(props);
            boolean bkrExists = (!props.getProperty("tasks", "").equals(""));
            while (!bkrExists) {
                uCommons.uSendMessage("Broker [" + NamedCommon.Broker + "] was not provided in shell-script! Will look in conf/this.server ... looping until found.");
                uCommons.Sleep(30);
                NamedCommon.ZERROR = false;
                NamedCommon.Zmessage="";
                responder.GetBroker();
                props = uCommons.LoadProperties(NamedCommon.Broker);
                uCommons.BkrCommons(props);
                bkrExists = (!props.getProperty("tasks", "").equals(""));
                NamedCommon.masterStop = coreCommons.StopNow();
                if (NamedCommon.masterStop) uCommons.StopProcessNow();
            }
//            NamedCommon.messageBrokerUrl = props.getProperty("url");
            String BrokerName = props.getProperty("name");
            ArrayList<String> TasksArray = new ArrayList<>(Arrays.asList(props.getProperty("tasks").split("\\,")));
            ArrayList<String> qNameArray = new ArrayList<>(Arrays.asList(props.getProperty("qname").split("\\,")));
            int tPos = TasksArray.indexOf(uTask);
            if (tPos < 0) {
                uCommons.uSendMessage("Task: [" + uTask + "] is not defined in broker [" + NamedCommon.Broker + "]");
                System.exit(1);
            }
            String inputQueue = qNameArray.get(tPos) + "_";
            int lxq = uQue.length();
            for (int ii = lxq; ii < 3; ii++) { inputQueue += "0"; }
            inputQueue += uQue;
            NamedCommon.inputQueue = inputQueue;
        } else {
            if (uTask.equals("090")) {
                NamedCommon.inputQueue = "uStartUp";
            } else {
                NamedCommon.inputQueue = "uShutDown";
            }
        }

        // -----------------------------------------------------------------------------

        String sProcs = uCommons.ReadDiskRecord("/proc/1/cgroup", true);
        if (!NamedCommon.isDocker) NamedCommon.isDocker = (sProcs.contains("/docker/"));
        if (!NamedCommon.isDocker) NamedCommon.isDocker = (sProcs.contains("/kubepods"));
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";

        NamedCommon.ThisRunKey.clear();
        NamedCommon.ThisRunVal.clear();
        NamedCommon.dbActive = System.nanoTime();
        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);
        // I need this line for Kiwibank ONLY
        if (!NamedCommon.hostname.equals("")) uCommons.SetMemory("domain", NamedCommon.hostname);

        if (!uTask.equals ("099") && NamedCommon.ReturnCodes.isEmpty()) { uCommons.SetupReturnCodes(); }

        NamedCommon.task = uTask;

        if (!NamedCommon.isDocker) NamedCommon.hostname = "";

        NamedCommon.isWhse = false;
        NamedCommon.isRest = false;
        NamedCommon.isNRT  = false;
        NamedCommon.isKafka= false;

        String jobName;
        switch (uTask) {
            case "010":
                jobName = "uStart()";
                NamedCommon.isWhse = true;
                break;
            case "012":
                jobName = "uFetch()";
                NamedCommon.isWhse = true;
                NamedCommon.H2Server    = h2dbServer.CreateServer();
                break;
            case "014":
                jobName = "uBurst()";
                NamedCommon.isWhse = true;
                NamedCommon.H2Server    = h2dbServer.CreateServer();
                break;
            case "017":
                jobName = "uFlipLoad()";
                NamedCommon.isWhse = true;
                break;
            case "022":
                jobName = "uStreams()";
                NamedCommon.isNRT  = true;
                break;
            case "025":
                jobName = "Send2Kafka()";
                NamedCommon.isWhse = true;
                NamedCommon.isNRT  = true;
                NamedCommon.isKafka= true;
                break;
            case "050":
                jobName = "uRESTful Access()";
                NamedCommon.isRest = true;
                break;
            case "055":
                jobName = "uRESTful WriteBack()";
                NamedCommon.isRest = true;
                break;
            case "090":
                jobName = "uStartUp()";
                break;
            case "099":
                jobName = "uShutDown()";
                break;
            default:
                jobName = "WARNING: Unrecognised task [" + uTask + "]";
        }

        if (NamedCommon.H2Server) {
            System.out.println(" ");
            System.out.println("-----------------------------------------------------------");
            System.out.println("This process will run in Resilient mode. H2 is available");
            System.out.println("-----------------------------------------------------------");
            System.out.println(" ");
            h2dbServer.Shutdown();
        }

        uCommons.uSendMessage("*");
        uCommons.uSendMessage("*");
        uCommons.uSendMessage("----------------------------------------------------------------------------------");
        uCommons.uSendMessage(jobName);
        uCommons.uSendMessage("process task=" + uTask);
        uCommons.uSendMessage("     on  que=" + uQue);
        uCommons.uSendMessage(" ");

        switch (uTask) {
            case "090":
                uCommons.StartUp();
                break;
            case "099":
                uCommons.ShutDown();
                break;
            default:
                uCommons.CleanupSettings(NamedCommon.BaseCamp+"/conf/", "SLOW");
                uCommons.CleanupSettings(NamedCommon.BaseCamp, "uojlog");
                uCommons.uSendMessage(" ");
                uCommons.uSendMessage("Starting " + jobName);
                uCommons.uSendMessage(" ");
                uCommons.uSendMessage("----------------------------------------------------------------------------------");
                char ans;
                boolean stopFlg = false;
                GarbageCollector.setStart(System.nanoTime());
                while (!stopFlg) {
                    System.out.println(" ");
                    responder.respond(uTask, uQue);
                    System.out.println(" ");
                    if (responder.StopFlag) {
                        stopFlg = true;
                    } else {
                        if (NamedCommon.ZERROR) {
                            NamedCommon.ZERROR = false;
                            uCommons.uSendMessage(NamedCommon.Zmessage);
                            if (NamedCommon.Zmessage.toLowerCase().startsWith("paus")) {
                                System.out.println(" ");
                                uCommons.Sleep(60);
                            } else {
                                stopFlg = true;
                            }
                        }
                    }
                    uCommons.CleanupSettings(NamedCommon.BaseCamp+"/conf/", "SLOW");
                    uCommons.CleanupSettings(NamedCommon.BaseCamp, "uojlog");
                }
        }

        uCommons.CleanupSettings(NamedCommon.BaseCamp+"/conf/", "SLOW");
        uCommons.CleanupSettings(NamedCommon.BaseCamp, "uojlog");

        if (NamedCommon.sConnected) SourceDB.DisconnectSourceDB();
        if (NamedCommon.tConnected) SqlCommands.DisconnectSQL();
        uCommons.uSendMessage("this process is now stopping");
    }

}
