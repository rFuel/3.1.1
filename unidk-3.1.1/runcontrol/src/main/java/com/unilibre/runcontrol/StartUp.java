package com.unilibre.runcontrol;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.commons.ConnectionPool;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SqlCommands;
import com.unilibre.commons.uCommons;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class StartUp {

    private static String basecamp;
    private static String inpField;
    private static String sysd;
    private static String conf;
    private static String data;
    private static String maps;
    private static String logs;
    private static String Burl;
    private static String move;
    private static String supv;
    private static String tmpl;
    private static String scrp;
    private static String vque;
    private static String Bname;
    private static String thisBkr;
    private static String confTxt;
    private static String exe;
    private static String preFix = "/opt";
    private static String zeros = "000";
    private static Properties Props;
    private static ArrayList<String> HoldList;
    private static ArrayList<String> TagList;
    private static ArrayList<String> InKeys;
    private static ArrayList<String> InVals;
    private static ArrayList<String> rfJobs;
//    private static boolean isDocker = false;
    private static boolean supervisord = true;
    private static boolean systemd = false;

    public static void main(String[] args) {

        // this class is run from $basecamp/lib/rfuel in the startconsumers() block
        //      there should not be any -D parameters unless this is a docker run.

        System.out.println("   ");
        System.out.println("   ");

        inpField = System.getProperty("param", "");
        String mgtType = System.getProperty("manager", "");

        if (mgtType.toLowerCase().equals("systemd")) supervisord = false;
        systemd = (!supervisord);
        basecamp = System.getProperty("user.dir") + "/";
        if (basecamp.contains("/home/andy")) {
            // *****************************
            NamedCommon.BaseCamp = NamedCommon.DevCentre + NamedCommon.slash;
            basecamp = NamedCommon.BaseCamp;
            // *****************************
        } else {
            NamedCommon.BaseCamp = basecamp;
        }
        System.out.println(" ");
        System.out.println(" ");
        System.out.println("---------------------------------------------------------------");
        System.out.println("Setting BaseCamp: " + NamedCommon.BaseCamp);
        conf = NamedCommon.BaseCamp + "conf" + NamedCommon.slash;
        data = NamedCommon.BaseCamp + "data" + NamedCommon.slash;
        maps = NamedCommon.BaseCamp + "maps" + NamedCommon.slash;
        logs = NamedCommon.BaseCamp + "logs" + NamedCommon.slash;
        sysd = NamedCommon.BaseCamp + "systemd" + NamedCommon.slash;

        if (systemd) {
            SystemD();
            return;
        }

        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);

        NamedCommon.PartitionTables = false;
        if (NamedCommon.SqlDBJar.equals("MSSQL")) CheckForPatitioning();

        HoldList = new ArrayList<String>();
        TagList = new ArrayList<String>();
        InKeys = new ArrayList<>();
        InVals = new ArrayList<>();
        rfJobs = new ArrayList<>();

        // set up tags to be replaced during this process

        TagList.add("$bkr$");
        TagList.add("$qname$");
        TagList.add("$queNbr$");
        TagList.add("$cpath$");
        TagList.add("$xms$");
        TagList.add("$xmx$");
        TagList.add("$task$");
        TagList.add("$qCnt$");
        TagList.add("$logs$");
        TagList.add("$cmd$");
        TagList.add("$otel.endpoint$");

        // -Dparam=key-val:key-val;etc...

        String[] inFields = inpField.split("\\:");
        for (int i=0; i<inFields.length; i++) {
            if (!inFields[i].equals("")) {
                InKeys.add(inFields[i].split("\\-")[0]);
                InVals.add(inFields[i].split("\\-")[1]);
            }
        }

        NamedCommon.isDocker = false;
        if (InKeys.indexOf("run") > -1) {
            if (InVals.get(InKeys.indexOf("run")).toLowerCase().equals("docker")) {
                System.out.println("setup for docker :-");
                NamedCommon.isDocker = true;
                supervisord = false;
                systemd = false;
            }
        }

        System.out.println("      Looking in: "+ conf + " for \"base\" config(s)");
        supv = ReadDiskRecord(conf + "superv.base");
        tmpl = ReadDiskRecord(conf + "template.base");
        scrp = ReadDiskRecord(conf + "script.base");
        move = ReadDiskRecord(conf + "move.base");
        vque = ReadDiskRecord(conf + "vtopic.base");
        confTxt = supv;

        if (systemd) {
            supv = ReadDiskRecord(conf + "something1.base");
            tmpl = ReadDiskRecord(conf + "something2.base");
            scrp = ReadDiskRecord(conf + "something3.base");
            vque = ReadDiskRecord(conf + "something4.base");
            confTxt = supv;
        }

        if (InKeys.indexOf("bkr") < 0) {
            DeleteAllScripts();
            Properties sProps = LoadProperties(conf + "this.server");
            if (sProps.size() > 0) {
                String[] brokers = sProps.getProperty("brokers", "").split(",");
                for (int b = 0; b < brokers.length; b++) {
                    thisBkr = brokers[b];
                    if (thisBkr.length() > 0) {
                        System.out.println("***************************************************");
                        System.out.println("Working with : " + thisBkr);
                        String pFile = conf + thisBkr;
                        ProcessBroker(pFile);
                        CheckMultiMove();
                        System.out.println("       ");
                    }
                }
            } else {
                System.out.println("<<FAIL>> in responder.respond() - cannot find 'this.server'");
            }
        } else {
            thisBkr = InVals.get(InKeys.indexOf("bkr"));
            if (thisBkr.length() > 0) {
                System.out.println("   ");
                System.out.println("***************************************************");
                System.out.println("Working with : " + thisBkr);
                String pFile = conf + thisBkr;
                ProcessBroker(pFile);
                CheckMultiMove();
                System.out.println("       ");
            }
        }

        if (NamedCommon.isDocker) confTxt = "";

        if (!confTxt.equals("") && !systemd) {
            System.out.println("==============================================================");
            System.out.println("preparing "+conf + "this.conf");
            WriteCnf(confTxt, conf + "this.conf");
            System.out.println("    --------------------   ");
            String cmd = "#!/bin/bash\n";
            cmd += "#--------------------------------------------------------\n";
            cmd += "#\n";
            cmd += "supervisord -c " + conf + "this.conf\n";
            WriteCnf(cmd, NamedCommon.BaseCamp + "Start_rFuel.u");
            File file = new File(NamedCommon.BaseCamp + "Start_rFuel.u");
            exe = basecamp + "Start_rFuel.u";
            if (!file.setExecutable(true, false)) System.out.println("FAILED setExecutable " + exe);
            if (!file.canExecute()) System.out.println("Unexecuteable file " + exe);
            Execute("/bin/sh ./Start_rFuel.u");
            System.out.println("    --------------------   ");
            System.out.println(" ");
            if (NamedCommon.isDocker) {
                System.out.println("  ");
                System.out.println("  ");
                System.out.println("[running in docker container] ================================");
                System.out.println("  ");
                System.out.println("  ");
            }
        }
        System.out.println("Done.");
    }

    private static void SystemD() {
        String server = ReadDiskRecord(conf + "this.server");
        String rfuelBase = ReadDiskRecord(conf + "rfuel.base");
        String brokerBase = ReadDiskRecord(conf + "broker.base");
        String queueBase = ReadDiskRecord(conf + "queue.base");
        String script = ReadDiskRecord(conf + "script.base");
        String vtscrp = ReadDiskRecord(conf+"vtopic.base");

        if (server.equals("")) return;
        if (rfuelBase.equals("")) return;
        if (brokerBase.equals("")) return;
        if (queueBase.equals("")) return;

        String svc       = ".service";
        String classpath = "$(echo " + preFix + basecamp +"lib/*.jar | sed 's/ /:/g')";

        ArrayList<String> services  = new ArrayList<>();
        ArrayList<String> queues    = new ArrayList<>();
        Properties pServer = LoadProperties(conf + "this.server");
        System.out.println(NamedCommon.block);
        if (pServer.size() > 0) {
            // ---------------------------------------------------------------  //
            //  Glossary of substitution variables:                             //
            //  $bName$         Broker Name from bkr file name=                 //
            //  $bkr$           Broker file name excluding ".bkr"               //
            //  $qName$         Queue name from bkr file qname=                 //
            //  $qNumber$       Queue number from bkr file responders=          //
            //  $xms$           Min memory from bkr file xms=                   //
            //  $xmx$           Max memory from bkr file xmx=                   //
            //  $qcp$           classpath                                       //
            //  $exe$           $qName$_$qNumber$.u                             //
            //  $sysdexe$       The rFuel command to run                        //
            // ---------------------------------------------------------------  //

            // 1. Create the rfuel service                                      //

            System.out.println("1. Recreate the rfuel service");
            WriteCnf(rfuelBase, sysd + "rfuel.service");

            // 2. Get all active brokers on this rFuel server                   //

            System.out.println("2. Get all active brokers on this rFuel server");

            String[] brokers = pServer.getProperty("brokers", "").split(",");
            String broker;
            String bkrFile;
            String bkrName;
            String task;
            String qName;
            String qNumber;
            String svcName;
            String bkrBase;
            String exec;
            String sXms;
            String sXmx;
            Properties pBroker;
            String[] tasks;
            String[] qnames;
            String[] qnbrs;
            String[] xms;
            String[] xmx;
            for (int b = 0; b < brokers.length; b++) {

                // 3. create a service for each broker, tied to rfuel.service   //

                bkrFile="";
                broker="";
                bkrName="";
                task="";
                qName="";
                qNumber="";
                svcName="";
                bkrBase="";
                exec="";
                sXms="";
                sXmx="";

                thisBkr = brokers[b];

                pBroker = LoadProperties(conf + thisBkr);
                System.out.println("3. Create a service for "+thisBkr);
                bkrFile = thisBkr.replace(".bkr", "");
                bkrName = pBroker.getProperty("name", bkrFile);
                svcName = bkrFile + svc;

                if (thisBkr.length() > 0) {
                    System.out.println("   ). Setting up " + svcName);

                    if (services.indexOf(svcName) < 0) {
                        bkrBase = ValueSubs(brokerBase, bkrName, bkrFile, task, qName, qNumber, sXms, sXmx, classpath, exec);
                        WriteCnf(bkrBase, sysd+svcName);
                        services.add(svcName);
                    }

                    preFix = pBroker.getProperty("rootD", "");

                    // preFix can be a different root directory for rfuel           //
                    // preFix   can be a run-time root - e.g. /u1 or empty-string   //
                    // basecamp is the current working directory (e.g. /upldev1/)   //
                    // So, the classpath could be /u1/upldev1 or just /upldev1/     //

                    // 3. create a service for the virtual topic - if one exists    //

                    String VTopic = pBroker.getProperty("topic", "");
                    if (!VTopic.equals("")) {
                        System.out.println("   ). Setting service for the virtual topic " + VTopic);
                        String vtName = "vt_"+VTopic;
                        System.out.println("      ). Prepare: " + vtName+svc);
                        if (services.indexOf(vtName) < 0) {
                            services.add(vtName);
                            exec = vtName+".u";

                            String vtSVC  = ValueSubs(queueBase, bkrName, bkrFile, task, qName, qNumber, sXms, sXmx, classpath, exec);
                            WriteCnf(vtSVC, sysd+vtName+svc);

                            // check the upl script exists else create it

                            String chk = ReadDiskRecord(basecamp+vtName+".u");
                            if (chk.equals("")) {
                                System.out.println("Create script for  " + basecamp+vtName+".u");
                                if (vtscrp.equals("")) {
                                    System.out.println("ERROR: Cannot file vtopic.base. Please install from rFuel:S3");
                                    return;
                                }
                                exec = vtName+".u";
                                sXms = "512m";
                                sXmx = "1024m";

                                chk = ValueSubs(vtscrp, bkrName, bkrFile, task, qName, qNumber, sXms, sXmx, classpath, exec);

                                exec = "";
                                sXms = "";
                                sXmx = "";
                                WriteCnf(chk, basecamp+vtName+".u");
                            }
                        }
                    }

                    // 4. create a service for each task-queue-number

                    System.out.println("4. Create a service for each task-queue-number");
                    tasks = pBroker.getProperty("tasks").split("\\,");
                    qnames= pBroker.getProperty("qname").split("\\,");
                    qnbrs = pBroker.getProperty("responders").split("\\,");
                    xms   = pBroker.getProperty("xms").split("\\,");
                    xmx   = pBroker.getProperty("xmx").split("\\,");

                    String template, queName, queNumber, jobRunner, jobName;
                    int nbrTasks = tasks.length;
                    int nbrListeners;

                    for (int t=0; t < nbrTasks; t++) {
                        try {
                            nbrListeners = Integer.valueOf(qnbrs[t].trim());
                        } catch (NumberFormatException nfe) {
                            System.out.println("ERROR: number of listeners ["+qnbrs[t]+"] - IS NOT AN INTEGER.");
                            return;
                        }

                        task = tasks[t];
                        System.out.println("   ). Setting up task: "+task);

                        for (int ll=0; ll< nbrListeners; ll++) {
                            template = queueBase;
                            jobRunner= script;
                            qNumber  = zeros+(ll+1);
                            qNumber  = qNumber.substring((qNumber.length()-3), qNumber.length());
                            qName  = qnames[t];
                            sXms   = xms[t];
                            sXmx   = xmx[t];

                            System.out.println("      ). Setting up "+qName+"_"+qNumber);

                            // rebuild the script runner

                            jobName = qName + "_" + qNumber + ".u";
                            exec = "./"+jobName;
                            jobRunner = ValueSubs(script, bkrName, bkrFile, task, qName, qNumber, sXms, sXmx, classpath, exec);
                            WriteCnf(jobRunner, basecamp + jobName);

                            // add the script runner to a service

                            template = ValueSubs(queueBase, bkrName, bkrFile, task, qName, qNumber, sXms, sXmx, classpath, exec);
                            WriteCnf(template, sysd + qName + qNumber + svc);
                        }

                    }

                }
            }
        } else {
            System.out.println("<<FAIL>> in responder.respond() - cannot find 'this.server'");
        }

    }

    private static String ValueSubs(String template, String bkrName, String bkrFile, String task, String qName, String qNumber, String sXms, String sXmx, String classpath, String exec) {
        while (template.contains("$bkr$")) {
            template = template.replace("$bkr$", thisBkr);
        }
        while (template.contains("$bFile$")) {
            template = template.replace("$bFile$", bkrFile);
        }
        while (template.contains("$bName$")) {
            template = template.replace("$bName$", bkrName);
        }
        // scripts
        while (template.contains("$qname$")) {
            template = template.replace("$qname$", qName);
        }
        // scripts
        while (template.contains("$queNbr$")) {
            template = template.replace("$queNbr$", qNumber);
        }

        while (template.contains("$qCnt$")) {
            template = template.replace("$qCnt$", String.valueOf(Integer.valueOf(qNumber)));
        }
        // base
        while (template.contains("$qName$")) {
            template = template.replace("$qName$", qName);
        }
        // base
        while (template.contains("$qNumber$")) {
            template = template.replace("$qNumber$", qNumber);
        }
        while (template.contains("$qcp$")) {
            template = template.replace("$qcp$", classpath);
        }
        while (template.contains("$cpath$")) {
            template = template.replace("$cpath$", classpath);
        }
        while (template.contains("$xms$")) {
            template = template.replace("$xms$", sXms);
        }
        while (template.contains("$xmx$")) {
            template = template.replace("$xmx$", sXmx);
        }
        while (template.contains("$task$")) {
            template = template.replace("$task$", task);
        }
        while (template.contains("$exe$")) {
            template = template.replace("$exe$", exec);
        }

        return template;
    }

    private static void ProcessBroker(String pFile) {
        if (!pFile.endsWith(".bkr")) pFile += ".bkr";
        String indent = "    ";
        System.out.println(indent + "Looking for broker config " + pFile);
        Props = LoadProperties(pFile);
        Burl = Props.getProperty("url", "#--this.bkr--URL--#");
        Bname = Props.getProperty("name", "#--this.bkr--NAME--#");
        preFix = Props.getProperty("rootD", "");
        System.out.println(indent + "****** Setting up Broker  " + Bname);

        String scrpTxt = "";

        ArrayList tasks = new ArrayList<>(Arrays.asList(Props.getProperty("tasks").split("\\,")));
        ArrayList qname = new ArrayList<>(Arrays.asList(Props.getProperty("qname").split("\\,")));
        ArrayList listeners = new ArrayList<>(Arrays.asList(Props.getProperty("responders").split("\\,")));
        ArrayList qxms = new ArrayList<>(Arrays.asList(Props.getProperty("xms").split("\\,")));
        ArrayList qxmx = new ArrayList<>(Arrays.asList(Props.getProperty("xmx").split("\\,")));

        String qnm, qcp, qxs, qxx, qts, qct;
        int cnt = tasks.size();
        int lctr, lx;
        qcp = "$(echo " + preFix + basecamp +"lib/*.jar | sed 's/ /:/g')";

        // ------------------
        // Setup supervisorD for VitualTopics

        String VTopic = Props.getProperty("topic", "");
        if (!VTopic.equals("")) {
            qnm = "vt_"+VTopic;
            qct = "0";
            String exec = "vt_"+VTopic+".u";
            HoldList.add(thisBkr);
            HoldList.add(qnm);
            HoldList.add(qct);
            HoldList.add(qcp);
            HoldList.add("512m");
            HoldList.add("1024m");
            HoldList.add("");
            HoldList.add("");
            HoldList.add(logs);
            HoldList.add("./" + exec);
            scrpTxt = Replacements(vque);
            WriteCnf(scrpTxt, NamedCommon.BaseCamp + exec);
            File file = new File(NamedCommon.BaseCamp + exec);
            if (!file.setExecutable(true, true)) System.out.println("FAILED setExecutable " + exec);
            if (!file.canExecute()) System.out.println("User permission issue for " + exec);
            confTxt += "\n" + Replacements(tmpl);
            HoldList.clear();
            System.out.println(indent + "===========================================================");
            System.out.println(indent + thisBkr + " > VirtualTopic > " + qnm);
        }

        // Set up supervision of each task in the broker

        String doTask="", doQue="";

        if (NamedCommon.isDocker) {
            doTask = InVals.get(InKeys.indexOf("task"));
            doQue = InVals.get(InKeys.indexOf("que"));
            System.out.println("Docker setup for task="+doTask+" on queue="+doQue);
        }

        for (int i = 0; i < cnt; i++) {
            qts = String.valueOf(tasks.get(i));
            if (NamedCommon.isDocker) {
                if (!qts.equals(doTask)) {
                    System.out.println(">>> skipping " + qts + " as not required");
                    continue;
                }
            }
            qnm = String.valueOf(qname.get(i));
            qct = String.valueOf(listeners.get(i));
            qxs = String.valueOf(qxms.get(i));
            qxx = String.valueOf(qxmx.get(i));
            lctr = Integer.valueOf(qct);
            if (lctr ==0) System.out.println(">>> No listeners for this queue.");
            System.out.println(indent + "===========================================================");
            System.out.println(indent + thisBkr + " > " + qts + " > " + qnm);

            if (NamedCommon.isDocker) {
                qct = ("000" + doQue);
                lctr = 1;
            }

            for (int ii = 1; ii <= lctr; ii++) {
                if (!NamedCommon.isDocker) qct = ("000" + (ii));
                lx = qct.length();
                qct = qct.substring((lx - 3), lx);
                String exe = qnm + "_" + qct + ".u";
                if (NamedCommon.isDocker) exe = "dcrun.u";
//                System.out.println(indent + "    > " + exe);

                // set up values for each replacement tag

                HoldList.add(thisBkr);
                HoldList.add(qnm);
                HoldList.add(qct);
                HoldList.add(qcp);
                HoldList.add(qxs);
                HoldList.add(qxx);
                HoldList.add(qts);
                if (!NamedCommon.isDocker) {
                    HoldList.add(String.valueOf(ii));
                } else {
                    HoldList.add(qct.replace("0",""));
                }
                HoldList.add(logs);
                HoldList.add(exe);
                scrpTxt = Replacements(scrp);
                WriteCnf(scrpTxt, NamedCommon.BaseCamp + exe);
                File file = new File(NamedCommon.BaseCamp + exe);
                if (!file.setExecutable(true, true)) System.out.println("FAILED setExecutable " + exe);
                if (!file.canExecute()) System.out.println("User permission issue for " + exe);
                if (NamedCommon.isDocker) rfJobs.add(NamedCommon.BaseCamp + exe);
                confTxt += "\n" + Replacements(tmpl);
                HoldList.clear();
            }
        }
    }

    private static void CheckMultiMove() {
        // Assumptuion: MoveRaw and MoveData are already in superv.base
        if (!NamedCommon.MultiMovers) {
            if (confTxt.contains("MoveRaw")) {
                return;
            } else {
                NamedCommon.maxFdir = 1;
            }
        }

        if (NamedCommon.maxFdir >= 1) {
            System.out.println("    ==============================================================");
            String directoryPath = NamedCommon.BaseCamp;
            if (!directoryPath.endsWith(NamedCommon.slash)) directoryPath += NamedCommon.slash;
            directoryPath += "data/ins/";
            String dPath, temp;
            File directory, xfile;
            for (int d = 1; d <= NamedCommon.maxFdir; d++) {
                dPath = directoryPath + uCommons.RightHash("000" + d, 3);
                System.out.println("    Preparing " + dPath);
                directory = new File(dPath);
                if (directory.isDirectory()) {
                    // 1.  Empty the directory of old files
                    for (File file : directory.listFiles()) {
                        if (!file.delete()) {
                            NamedCommon.MultiMovers = false;
                            System.out.println("***");
                            System.out.println("ERROR: CANNOT delete " + file.getName());
                            System.out.println("ERROR: CANNOT use multiple data inserters");
                            System.out.println("***");
                            NamedCommon.MultiMovers = false;
                            uCommons.Sleep(10);
                            break;
                        }
                    }

                    // 2. Delete the directory (force permission compliance)
                    if (!directory.delete()) {
                        NamedCommon.MultiMovers = false;
                        System.out.println("***");
                        System.out.println("ERROR: CANNOT delete " + directory.getName());
                        System.out.println("ERROR: CANNOT use multiple data inserters");
                        System.out.println("***");
                        NamedCommon.MultiMovers = false;
                        uCommons.Sleep(10);
                        break;
                    }
                    uCommons.Sleep(2);

                    // 3. Make the directory (force permission compliance)
                    if (!directory.mkdir()) {
                        NamedCommon.MultiMovers = false;
                        System.out.println("***");
                        System.out.println("ERROR: CANNOT delete " + directory.getName());
                        System.out.println("ERROR: CANNOT use multiple data inserters");
                        System.out.println("***");
                        NamedCommon.MultiMovers = false;
                        uCommons.Sleep(10);
                        break;
                    }
                } else {
                    // create it (force permission compliance)
                    if (!directory.mkdir()) {
                        NamedCommon.MultiMovers = false;
                        System.out.println("***");
                        System.out.println("ERROR: CANNOT delete " + directory.getName());
                        System.out.println("ERROR: CANNOT use multiple data inserters");
                        System.out.println("***");
                        uCommons.Sleep(10);
                        break;
                    }
                }

                // Ensure permissions are compliant to this user (usually unilibre)
                if (!directory.canRead() || !directory.canWrite()) {
                    System.out.println("***");
                    System.out.println("ERROR: Read / Write permission error in: " + directory.getName());
                    System.out.println("ERROR: CANNOT use multiple data inserters");
                    System.out.println("***");
                    uCommons.Sleep(10);
                    NamedCommon.MultiMovers = false;
                    break;
                }

                // Add movers to the config file.
                System.out.println("    Preparing MoveRaw"+d);
                temp = tmpl;
                temp = temp.replace("$_$", "$$");
                temp = temp.replace("$qname$", "MoveRaw");
                temp = temp.replace("$queNbr$", String.valueOf(d));
                temp = temp.replace("$cmd$", "MoveRaw"+d+".u");
                confTxt += "\n" + temp;

                System.out.println("    Preparing MoveData"+d);
                temp = temp.replace("MoveRaw", "MoveData");
                confTxt += "\n" + temp;

                String qcp = "$(echo " + preFix + basecamp +"lib/*.jar | sed 's/ /:/g')";
                temp = move;
                temp = temp.replace("$cpath$", qcp);
                temp = temp.replace("$qname$", "MoveRaw");
                temp = temp.replace("$queNbr$", String.valueOf(d));
                temp = temp.replace("$cnt$", String.valueOf(d));
                temp = temp.replace("$schema$", "raw");
                WriteCnf(temp, NamedCommon.BaseCamp + "MoveRaw"+d+".u");
                xfile = new File(NamedCommon.BaseCamp + "MoveRaw"+d+".u");
                xfile.setExecutable(true);
                xfile = null;

                temp = temp.replace("=raw ", "= ");
                temp = temp.replace("Raw", "Data");
                WriteCnf(temp, NamedCommon.BaseCamp + "MoveData"+d+".u");
                xfile = new File(NamedCommon.BaseCamp + "MoveData"+d+".u");
                xfile.setExecutable(true);
                xfile = null;
            }
        } else {
            NamedCommon.MultiMovers = false;
        }
    }

    private static void DeleteAllScripts() {
        String lookin = basecamp;
        final String matchStr = ".u";
        File dir = new File(lookin);
        File[] matchFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(matchStr);
            }
        });
        int nbrmatches = 0;
        try {
            nbrmatches = matchFiles.length;
        } catch (NullPointerException nfe) {
            nbrmatches = 0;
        }
        if (nbrmatches > 0) {
            for (int ff = 0; ff < nbrmatches; ff++) {
                if (matchFiles[ff].exists()) matchFiles[ff].delete();
            }
        }
    }

    private static void WriteCnf(String confTxt, String fname) {
        System.out.println("      ). writing  to   "+fname);
        BufferedWriter bWriter = null;
        try {
            bWriter = new BufferedWriter(new FileWriter(fname, false));
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
        ArrayList<String> lines = new ArrayList<>(Arrays.asList(confTxt.split("\\r?\\n")));
        int ll = lines.size();
        String line;
        for (int l = 0; l < ll; l++) {
            try {
                bWriter.write(lines.get(l));
                bWriter.newLine();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                System.exit(0);
            }
        }
        try {
            bWriter.flush();
            bWriter.close();
        } catch (IOException e) {
            System.out.println("[FATAL]   Cannot close " + fname);
            System.exit(0);
        }
    }

    private static void ShowCnf(String confTxt) {
        ArrayList<String> lines = new ArrayList<>(Arrays.asList(confTxt.split("\\r?\\n")));
        int ll = lines.size();
        for (int l = 0; l < ll; l++) {
            System.out.println(lines.get(l));
        }
    }

    private static Properties LoadProperties(String fname) {
        if (!fname.contains("/")) fname = conf + fname;
        Properties props = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(fname);
        } catch (FileNotFoundException e) {
            System.out.println("Cannot find " + fname);
            System.exit(0);
        }
        if (is != null) {
            try {
                props.load(is);
            } catch (IOException e) {
                System.out.println("Cannot load " + fname);
                e.printStackTrace();
                System.exit(0);
            }
        } else {
            System.out.println("Please load '" + fname + "'");
            System.exit(0);
        }
        return props;
    }

    private static String ReadDiskRecord(String infile) {
        String instrFile = String.valueOf(infile);
        String rec = "";
        try {
            BufferedReader BRin = new BufferedReader(new FileReader(instrFile));
            String line;
            while ((line = BRin.readLine()) != null) {
                rec = rec + line + "\n";
            }
            BRin.close();
        } catch (IOException e) {
            System.out.println("read failure on " + instrFile);
            System.exit(1);
        }
        rec.replaceAll("\\|", " ");
        return rec;
    }

    private static String Replacements(String inVal) {

        ArrayList<String> lines = new ArrayList<>(Arrays.asList(inVal.split("\\r?\\n")));

        /*   HoldList holds the data values       */
        /*   TagList  holds the replacement tags  */

        String xmlLine = "", theTag = "", datum = "", xmlRequest = "";
        int nbrLines = lines.size(), fPos = 0;
        boolean todo = true;

        for (int ll = 0; ll < nbrLines; ll++) {
            xmlLine = lines.get(ll);
            if (!xmlLine.contains("$")) {
                xmlRequest += xmlLine + "\r\n";
            } else {
                todo = true;
                while (todo) {
                    int nbrTags = xmlLine.length() - xmlLine.replace("$", "").length();
                    nbrTags = nbrTags / 2;

                    for (int xx = nbrTags; xx > 0; xx--) {
                        theTag = FieldOf(xmlLine, "\\$", (xx * 2));
                        theTag = "$" + FieldOf(theTag, "\\$", 1) + "$";
                        fPos = TagList.indexOf(theTag);
                        if (fPos >= 0) {
                            datum = HoldList.get(fPos);
                            xmlLine = xmlLine.replace(theTag, datum);
                        } else {
                            System.out.println(theTag + " may be a replacement string - ignoring");
                        }
                    }
                    todo = false;
                }
                if (xmlLine.contains("__")) xmlLine = xmlLine.replace("__", "$");
                if (xmlLine != "") xmlRequest += xmlLine + "\r\n";
            }
        }
        return xmlRequest;
    }

    private static String FieldOf(String str, String findme, int occ) {
        String ans;
        String[] tmpStr;
        tmpStr = str.split(findme);
        if (occ <= tmpStr.length) {
            ans = tmpStr[occ - 1];
        } else {
            ans = "";
        }
        return ans;
    }

    public static void Execute(String cmd) {
        Runtime rt = Runtime.getRuntime();
        try {
            System.out.println("command [" + cmd + "]");
            Process p = rt.exec(cmd);

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(p.getErrorStream()));

            boolean first = true;
            String s;
            while ((s = stdInput.readLine()) != null) {
                if (first) {
                    System.out.println("Output from the command:");
                    first = false;
                }
                System.out.println("  > " + s);
            }

            first = true;
            while ((s = stdError.readLine()) != null) {
                if (first) {
                    System.out.println("Errors from the command:");
                    first = false;
                }
                System.out.println("  > " + s);
            }

        } catch (IOException e) {
            System.out.println("exec() failed for :: " + cmd);
            System.out.println(e.getMessage());
            System.exit(1);
        }
    }

    private static void CheckForPatitioning() {
        System.out.println("-------------------------------------------------------------------");
        System.out.println("Checking target DB for table-partitioning please wait ....");

        if (NamedCommon.PartScheme.equals("")) {
            System.out.println("   no partition scheme - nothing to check");
            System.out.println("-------------------------------------------------------------------");
            return;
        }

        NamedCommon.runSilent = true;
        SqlCommands.ConnectSQL();
        NamedCommon.runSilent = false;
        ArrayList<String> ddl = new ArrayList<>();

        try {
            // This block is VERY handy code for "less" security minded sites.
            // the Alter database, create partition function, create partition scheme
            // statements ALL require elevated privileges.

            ResultSet rs;
            int ctr=0;
            if (NamedCommon.uCon == null) NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
            Statement stmt = NamedCommon.uCon.createStatement();
            String cmd;

//                    // check if FileGroups have been created
//                    System.out.println("   . Check FileGroups");
//                    String cmd = "SELECT count(*) as answer FROM sys.filegroups where name like 'fg_rfuel%;";
//                    rs = stmt.executeQuery(cmd);
//                    while (rs.next()) { ctr=rs.getInt("answer"); }
//                    rs.close();
//                    if (ctr != NamedCommon.MaxProc) {
//                        System.out.println("   . FileGroup mismatch: "+ctr+" found. "+NamedCommon.MaxProc+" expected");
//                        System.out.println("   . Recreating Partitioning Logic");
//                        // DropCreatePartitioning();
//                        System.out.println("Stopping now. Please re-run 'rfuel -start' --------------------------");
//                        System.exit(0);
//                    }
//
//                    // Check that the partition function has been created
//                    cmd = "SELECT count(*) as answer FROM sys.partition_functions WHERE name = 'pf_rfuel';";
//                    rs = stmt.executeQuery(cmd);
//                    while (rs.next()) { ctr=rs.getInt("answer"); }
//                    rs.close();
//                    if (ctr != 1) {
//                        System.out.println("   . Partition Function is missing");
//                        System.out.println("   . Recreating Partitioning Logic");
//                        // DropCreatePartitioning();
//                        System.out.println("Stopping now. Please re-run 'rfuel -start' --------------------------");
//                        System.exit(0);
//                    }

            // Check that the partition scheme has been created

            cmd = "SELECT count(*) as answer FROM sys.partition_schemes WHERE name = '$$';";
            cmd = cmd.replace("$$", NamedCommon.PartScheme);
            rs = stmt.executeQuery(cmd);
            while (rs.next()) { ctr=rs.getInt("answer"); }
            rs.close();
            if (ctr == 1) NamedCommon.PartitionTables = true;
            stmt.close();
            stmt = null;
            rs.close();
            rs = null;
            cmd= null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        System.out.println("   Done. Table partitioning is " + NamedCommon.PartitionTables);
        System.out.println("-------------------------------------------------------------------");
    }

    private static void DropCreatePartitioning() {
        // 1. Remove ALL Tables from ALL filegroups
        //    ------ safer to drop tables !! ------

        // 2. Remove partition scheme

        // 3. Remove partition function

        // 4. Remove FileGroups

        // 5. Create FileGroups, partition function, partition scheme
        //    ------------ see rFuel installer ----------------------
        //    --------------- create-sql-content.sql ----------------
    }

}
