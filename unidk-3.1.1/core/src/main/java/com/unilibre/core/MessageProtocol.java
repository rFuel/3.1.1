package com.unilibre.core;
/* * Copyright UniLibre on 2015. ALL RIGHTS RESERVED  **/

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import com.unilibre.MQConnector.commons;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.kafka.kProducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class MessageProtocol {

    public static String messageText = "";
    public static String conf;
    public static String data;
    public static String maps;
    public static String basecamp;
    private static String lastMsg="", lastShost="";
    private static String responseText = "", temp = "", messageHold;
    private static String nextCor = "";
    private static String nextBrk = "";
    private static String inTask = "", nextTask = "", nextQue = "", qTask="";
    private static String thisTask = "", thisQue = "", thisCor = "", Genesis="";
    public static String DeltaTS="";
    private static final String fail = "<<FAIL>>";
    private static final String lSplitter = "--#SPLIT";
    private static final String shell = "shell";
    public static int offSet = 0;
    private static int nextCtr = 0, tPos = 0, queMax = 0, tqueMax = 0, queNbr = 0, nextMax=0;
    private static boolean metaGrps = false;
    private static boolean TrigMsg = false;
    private static boolean flowThru = true;
    private static long finishM = 0;
    private static double laps=0;
    private static final double div = 1000000000.00;
    private static Properties bkrProps;
    private static Properties runProps;
    private static DecimalFormat df = new DecimalFormat("#0.00");
    public static ArrayList<String> UplCtl;
    private static ArrayList<String> TasksArray = new ArrayList<>();
    private static ArrayList<String> qNameArray = new ArrayList<>();
    private static ArrayList<String> Responders = new ArrayList<>();

    public static String handleProtocolMessage(String task, String qNbr, String message) {

        if (NamedCommon.ZERROR) NamedCommon.ZERROR=false;
        // DO NOT do   NamedCommon.Reset();  here !! It Must be done in responder()
        NamedCommon.message = message;
        uCipher.SetAES(false, "","");
        runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        NamedCommon.SystemSchema = NamedCommon.SqlSchema;
        if (NamedCommon.protocol.equals("")) {
            uCommons.uSendMessage("No protocol identified");
            System.exit(0);
        }
        responseText = "";
        message = message.replaceAll(";\\\\", ";");
        message = message.replaceAll(">\\\\", ">");
        message = message.replaceAll("\\r?\\n", "");
        if (message.startsWith("{")) {
            message = stringifyMessage(message);
            message+= "MESSAGE<is>"+NamedCommon.MessageID+"<tm>";
        }
        if (NamedCommon.ReturnCodes.size() == 0) uCommons.SetupReturnCodes();
        if (NamedCommon.escChars[0][0] == null) NamedCommon.ResetEscChars();

        SourceDB.message = message;
        if (!NamedCommon.tConnected) uCommons.uSendMessage("Setup JDBC string and connect to target DB");
        while (!NamedCommon.tConnected || !NamedCommon.sConnected) {
            uCommons.Message2Properties(message);
            while (!NamedCommon.tConnected) {
                NamedCommon.runSilent = true;
                uCommons.uSendMessage("Waiting for target DB connection. Please check rFuel.properties.");
                uCipher.SetAES(false, "", "");
                runProps = uCommons.LoadProperties("rFuel.properties");
                uCommons.SetCommons(runProps);
                uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
                uCommons.Sleep(10);
                NamedCommon.ZERROR = false;
                ConnectionPool.ConFails.clear();
                uCommons.Message2Properties(message);
            }
            if (!u2Commons.TestAlive()) SourceDB.ConnectSourceDB();
            while(!NamedCommon.sConnected) {
                NamedCommon.runSilent = true;
                uCommons.uSendMessage("Waiting for source DB connection. Please check rFuel.properties.");
                uCipher.SetAES(false, "", "");
                runProps = uCommons.LoadProperties("rFuel.properties");
                uCommons.SetCommons(runProps);
                uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
                uCommons.Sleep(10);
                SourceDB.ReconnectService();
            }
        }
        if (NamedCommon.ZERROR) return NamedCommon.Zmessage;
        NamedCommon.datAct = APImsg.APIget("dacct");

        inTask = APImsg.APIget("task");

        if (NamedCommon.MessageID.equals(lastMsg)) {
            if (!NamedCommon.uplSite.contains(NamedCommon.UniLibre))  return "";
        }
        NamedCommon.startM = System.nanoTime();

        String thisShost = APImsg.APIget("shost");
        if (thisShost.equals("")) thisShost = "base";
        if (lastShost.equals("")) lastShost = thisShost;

        if (!thisShost.equals(lastShost) && !NamedCommon.isNRT) {
            SourceDB.DisconnectSourceDB();
            SourceDB.ConnectSourceDB();
        }
        lastShost = thisShost;

        if (NamedCommon.lastM == 0) {
            laps = 0;
        } else {
            laps = (NamedCommon.startM - NamedCommon.lastM);
            laps = laps / div;
        }

        NamedCommon.lastM = NamedCommon.startM;

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("---------------------------------------------------------------");
            uCommons.uSendMessage(message);
            uCommons.uSendMessage("---------------------------------------------------------------");
        }
        // -------------------------------------------------------------------------
        qTask = task;
        NamedCommon.isKafka= (qTask.equals("025"));
        NamedCommon.isRest = (qTask.startsWith("05"));
        NamedCommon.isNRT  = (qTask.startsWith("02") && !NamedCommon.isKafka);
        NamedCommon.isWhse = (qTask.startsWith("01"));
        if (NamedCommon.isNRT) NamedCommon.isWhse = true;

        NamedCommon.U2File = null;
        NamedCommon.isAssoc= false;
        if (NamedCommon.isWhse && NamedCommon.tHostList.size() > 1) {
            NamedCommon.SqlDatabase = "$DB$";
            NamedCommon.SqlSchema   = "$SC$";
        }
        nextBrk = uCommons.GetNextBkr(NamedCommon.Broker);

        boolean proceed = true;
        Genesis="";

        basecamp = NamedCommon.BaseCamp + NamedCommon.slash;
        conf = basecamp + "conf" + NamedCommon.slash;
        data = basecamp + "data" + NamedCommon.slash;
        maps = basecamp + "maps" + NamedCommon.slash;
//        if (!NamedCommon.isWebs && !NamedCommon.isNRT && !NamedCommon.Broker.equals("")) {
        if (!NamedCommon.isWebs && !NamedCommon.Broker.equals("")) {
            bkrProps = uCommons.LoadProperties(conf + NamedCommon.Broker);
            uCommons.BkrCommons(bkrProps);
        }

        if (License.domain.equals("")) License.domain = "@#@";
        if (basecamp.toLowerCase().contains(License.domain)) NamedCommon.gmods = basecamp + "lib" + NamedCommon.slash;

        if (NamedCommon.isWhse) {
            if (NamedCommon.rawDB.equals("")) NamedCommon.rawDB = NamedCommon.SqlDatabase;
            NamedCommon.SelectCmd.clear();
            NamedCommon.SelectList.clear();
        }

        if (NamedCommon.isValid) {
            String today = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
            today = today.replaceAll("\\-", "");
            // ----- catch it the next time around ----- //
            if (!NamedCommon.licChecked.equals(today)) NamedCommon.isValid = false;
        }

        // -------------------------------------------------------------------

        message = message.replaceAll("\\r?\\n", "");

//        if (NamedCommon.isNRT || NamedCommon.isRest) {
//            uCommons.MessageToAPI(message);
//            if (NamedCommon.ZERROR) return NamedCommon.Zmessage;
//        }

        inTask = APImsg.APIget("task");

        NamedCommon.xMap = APImsg.APIget("map");
        NamedCommon.isPrt = false;
        if (NamedCommon.xMap.endsWith("prt")) {
            NamedCommon.isPrt = true;
            NamedCommon.Proceed = false;
        }

        if (NamedCommon.isRest) {
            NamedCommon.showLineage = (APImsg.APIget("showlineage").toLowerCase().equals("true"));
            if (!APImsg.APIget("sparse").equals("")) {
                if (APImsg.APIget("sparse").equals("true"))  NamedCommon.Sparse = true;
                if (APImsg.APIget("sparse").equals("false")) NamedCommon.Sparse = false;
            }
        }

        // -------------------------------------------------------------------

        if (NamedCommon.isWhse || NamedCommon.isKafka) {
            if (NamedCommon.SqlSchema.equals(NamedCommon.rawSC)) {
                NamedCommon.Zmessage = "CANNOT use schema raw - this is reserved for UniLibre";
                NamedCommon.ZERROR = true;
                uCommons.uSendMessage(NamedCommon.Zmessage);
            }
            if (NamedCommon.SqlSchema.equals("uni") && NamedCommon.uniBase) {
                NamedCommon.Zmessage = "CANNOT use schema uni - this is reserved for UniLibre";
                NamedCommon.ZERROR = true;
//                uCommons.uSendMessage(NamedCommon.Zmessage);
                responseText = NamedCommon.Zmessage;
                return responseText;
            }

            if (!NamedCommon.Presel.equals("Unknown")) {
                if (!inTask.equals("017") && !NamedCommon.Presel.equals("")) {
                    String preSelFile = data + NamedCommon.Presel;
                    if (!preSelFile.endsWith(".txt")) preSelFile += ".txt";
                    String preSel = uCommons.ReadDiskRecord(preSelFile);
                    if (!preSel.equals("")) {
                        String[] sTmp1 = preSel.split("\\r?\\n");
                        for (int ss = 0; ss < sTmp1.length; ss++) {
                            NamedCommon.SelectCmd.add(sTmp1[ss].split("\t")[0]);
                            NamedCommon.SelectList.add(sTmp1[ss].split("\t")[1]);
                        }
                    }
                    File rTmp = new File(preSelFile);
                    rTmp.delete();
                }
            }
            // Why was this line active ??
//            NamedCommon.DropIt = false;
            if (NamedCommon.RunType.equals("REFRESH")) NamedCommon.DropIt = true;
            offSet = Integer.valueOf(qNbr);
            NamedCommon.burstCnt = offSet;
            NamedCommon.BatchID = uCommons.MakeBatchID();
//            NamedCommon.BatchID = new SimpleDateFormat("yyyyMMddHHmmssSSSSSS").format(Calendar.getInstance().getTime());
            NamedCommon.datCt = 0;
            if (NamedCommon.isNRT && !NamedCommon.isKafka) {
                tPos = 0;
                thisTask = task;
                thisQue = "1";
                tqueMax = 1;
                nextTask = task;
                nextQue = "1";
                nextMax = 1;
                queNbr = 1;
                queMax = 1;
            } else {
                NamedCommon.rowID = 1;
                int nbrTasks = 0;
                TasksArray = new ArrayList<>(Arrays.asList(bkrProps.getProperty("tasks").split("\\,")));
                qNameArray = new ArrayList<>(Arrays.asList(bkrProps.getProperty("qname").split("\\,")));
                Responders = new ArrayList<>(Arrays.asList(bkrProps.getProperty("responders").split("\\,")));
                ArrayList<Integer> qNbrsArray = new ArrayList<>();
                nbrTasks = TasksArray.size();
                for (int ii = 0; ii < nbrTasks; ii++) { qNbrsArray.add(ii, 0); }
                if (NamedCommon.task.equals("015")) {
                    // if not included in the broker conf, then behave as a 014 message.
                    tPos = TasksArray.indexOf(NamedCommon.task);
                    if (tPos < 0)  tPos = TasksArray.indexOf("014");
                } else {
                    if (NamedCommon.task.startsWith("9")) {
                        tPos = 0;
                    } else {
                        tPos = TasksArray.indexOf(NamedCommon.task);
                    }
                }
                if (tPos < 0) {
                    proceed = false;
                    responseText = "<<FAIL>> Unrecognisable task in broker [" + inTask + "]";
                } else {
                    thisTask = TasksArray.get(tPos);
                    thisQue = qNameArray.get(tPos);
                    tqueMax = Integer.valueOf(Responders.get(tPos));
                    if (tPos < (nbrTasks - 1)) tPos++;
                    nextTask = TasksArray.get(tPos);
                    nextQue = qNameArray.get(tPos);
                    nextMax = Integer.valueOf(Responders.get(tPos));
                    queNbr = qNbrsArray.get(tPos);
                    queMax = Integer.valueOf(Responders.get(tPos));
                }
            }
        } else {
            nextTask = inTask;
            nextQue = qNbr;
            queNbr = Integer.valueOf(qNbr);
            queMax = queNbr;
            tqueMax = queNbr;
        }
        if (queNbr == 0) queNbr = 1;
        boolean legal = false;

        if (!NamedCommon.isValid) {
            legal = License.IsValid();
        } else {
            legal = true;
        }

        if (legal) {
            // Already done this in responder loops -------------------------------
//            uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
            if (NamedCommon.isWhse && !NamedCommon.licBulk) {
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**         Not licenced for uBulk");
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**");
                legal = false;
            }
            if (NamedCommon.isRest && !NamedCommon.licRest) {
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**         Not licenced for uRest");
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**");
                legal = false;
            }
            if (NamedCommon.isNRT && !NamedCommon.licNRT) {
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**         Not licenced for uStreams");
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**");
                legal = false;
            }
        }

        if (!legal) {
            uCommons.uSendMessage("<><><><><><><><><><><><><><><><><><><><><><>");
            uCommons.uSendMessage("rFuel License failure - cannot proceed on "+License.domain);
            uCommons.uSendMessage("<><><><><><><><><><><><><><><><><><><><><><>");
            responseText = "rFuel License Violation - please email support@unilibre.com.au";
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = responseText;
            if (!NamedCommon.isNRT && !NamedCommon.isWebs) {
                nextCtr = 0;
                nextCor = "LICENSE_VIOLATION";
                nextCtr = Send2NextProcess(nextBrk, 0, nextCor);
            }
            if (NamedCommon.isWebs) {
                String descr = NamedCommon.ReturnCodes.get(Integer.valueOf("500"));
                if (descr.equals("")) descr = "ReturnCode [500] not found.";
                responseText = DataConverter.ResponseHandler("500", descr, responseText, APImsg.APIget("FORMAT").toUpperCase());
            }
            return responseText;
        } else {
            NamedCommon.isValid = true;
            messageText = message;
            messageHold = message;
            if (!NamedCommon.isNRT) {
                if (!inTask.startsWith("9") && !inTask.equals(qTask)) {
                    responseText = "Message for [" + APImsg.APIget("correlationid") + "]" +
                            " was sent to the wrong task queue. " +
                            "Your task was [" + inTask + "]. You sent it to [" +
                            qTask + "]";
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = responseText;
                    return responseText;
                }
            }

            if (NamedCommon.ZERROR) proceed = false;

            if (proceed && !NamedCommon.isDocker) {
                // Docker containers don't need the BP.UPL STOP setting as the
                // containers are orchistrated in tools like OC and Kube.

                if (!APImsg.APIget("task").equals("910") && !NamedCommon.isNRT) {
                    if (NamedCommon.isWebs) SourceDB.SetDBtimeout(true);
                    String chkReply = u2Commons.CheckU2Controls();
                    proceed = (chkReply.contains("<<PASS>>"));
                    if (!proceed && APImsg.APIget("task").equals("999")) {
                        if (chkReply.contains(" STOP ")) {
                            proceed = true;
                            NamedCommon.ZERROR = false;
                            NamedCommon.Zmessage = "";
                        }
                    }
                }
            }

            if (!NamedCommon.sConnected && !NamedCommon.isNRT && !NamedCommon.task.equals("014")) SourceDB.ConnectSourceDB();

            if (proceed && !NamedCommon.ZERROR) {
                boolean metaMap = false;
                if (APImsg.APIget("task").equals("910")) NamedCommon.xMap = "dummy.map";
                if (NamedCommon.isWhse) {
                    thisCor = SetCorrel(thisTask);
                    nextCor = SetCorrel(nextTask);
                    // metaMap flags that this is a grp or a gog process.
                    metaMap = (!NamedCommon.xMap.endsWith("map") && !NamedCommon.isPrt);
                    metaMap = (metaMap && !NamedCommon.isNRT);
                    if (!NamedCommon.BulkLoad) NamedCommon.Komma = ",";
                    if (!CheckMsgTask()) {
                        uCommons.uSendMessage("Message error: resolve and resubmit");
                        proceed = false;
                    }
                    if (!APImsg.APIget("task").startsWith("9")) {
                        if (!NamedCommon.xMap.equals("")) uCommons.GetMap(maps + NamedCommon.xMap);
                        if (NamedCommon.ZERROR) {
                            NamedCommon.burstCnt = Send2NextProcess(nextBrk, 999, thisCor + NamedCommon.xMap);
                            return "";
                        }
                    }
                    if (proceed) {
                        if (!NamedCommon.isNRT && !(inTask.startsWith("9") || inTask.equals("010") || inTask.equals("017"))) {
                            if (!NamedCommon.datOnly) {
                                CheckUPLCTL();
                                if (!NamedCommon.ZERROR) {
                                    UplCtl = new ArrayList<>();
                                    for (int u = 0; u <= 15; u++) {UplCtl.add("");}
                                    UplCtl.set(3, NamedCommon.xMap);
                                    UplCtl.set(4, inTask);
                                    if (metaMap) {
                                        /* ------ *.grp *.gog ------ */
                                        metaGrps = true;
                                        if (!inTask.equals("010")) {
                                            nextQue = thisQue;
                                            queMax = tqueMax;
                                            queNbr = queMax;
                                        }
                                        SetupRun();
                                        proceed = false;
                                    }
                                } else {
                                    proceed = false;
                                    return responseText;
                                }
                            }
                        }
                    }
                }

                String dacct="";
                dacct = APImsg.APIget("dacct");
                if (!dacct.isEmpty()) {
                    boolean saveZERR = NamedCommon.ZERROR;
                    String saveZMSG  = NamedCommon.Zmessage;
                    String AcctList = uCommons.ReadDiskRecord(conf+"DACCT");
                    NamedCommon.ZERROR = saveZERR;
                    NamedCommon.Zmessage=saveZMSG;
                    if (!AcctList.contains(dacct)) {
                        AcctList += "\n" + dacct;
                        uCommons.WriteDiskRecord(conf + "DACCT", AcctList);
                    }
                }

                if (!APImsg.APIget("ttl").equals("")) {
                    try {
                        NamedCommon.Expiry = Long.valueOf(APImsg.APIget("ttl"));
                        NamedCommon.Expiry = NamedCommon.Expiry * 100000;
                    } catch (NumberFormatException nfe) {
                        NamedCommon.Expiry = 2000000000;
                    }
                }

                if (NamedCommon.Expiry < 10000) NamedCommon.Expiry = 10000;

                if (proceed && !NamedCommon.ZERROR) {
                    if (NamedCommon.debugging) uCommons.uSendMessage("HandleMessageTask() * start");
                    if (NamedCommon.reply2Q.equals("")) NamedCommon.reply2Q = "RunERRORS";
                    if (NamedCommon.CorrelationID.equals("")) NamedCommon.CorrelationID = "No_CorrelationID";

                    commons.SetAckMode(NamedCommon.MQackMode);

                    String saveKomma = NamedCommon.Komma;

                    uCommons.uSendMessage("   .) Process MAP: " + NamedCommon.xMap);
                    uCommons.uSendMessage("   .) Using   DSD: " + uCommons.APIGetter("list"));

                    // APImap has been setup
                    // APImsg has been setup
                    // ... is this message valid or is it missing mandatory components ?

                    if (!metaMap) proceed = uCommons.OkayToProcess(inTask);
                    if (proceed) {
                        TrigMsg = false;
                        switch (inTask) {
                            case "010":
                                SetupRun();
                                break;
                            case "012":
                                ExecuteFetch();
                                if (!NamedCommon.ZERROR) UpdateUPLCTL();
                                if (!NamedCommon.Proceed) ManageGenesis();
                                if (!NamedCommon.ZERROR) MapTrigger();
                                break;
                            case "014":
                                if (!APImsg.APIget("INCR_LOADDTE").equals("")) {
                                    NamedCommon.BatchID = APImsg.APIget("INCR_LOADDTE");
                                }
                                flowThru = true;
                                NamedCommon.BurstRestarted = false;
                                ExecuteBurst();
                                if (!NamedCommon.ZERROR && !NamedCommon.BurstRestarted) UpdateUPLCTL();
                                if (!NamedCommon.Proceed) ManageGenesis();
                                if (!NamedCommon.ZERROR && !NamedCommon.BurstRestarted) MapTrigger();
                                break;
                            case "015":
                                flowThru = false;
                                uCommons.uSendMessage("CreateViews(in development)");
                                // get the map and execute postsql commands     //
                                // may need a property called "view=blah.sql"   //
                                CreateViews();
                                break;
                            case "017":
                                FlipLoaded();       // UplCtl is updated in this method.
                                ManageGenesis();    // Should never be used IF it's being done in 014.
                                if (!NamedCommon.ZERROR) MapTrigger();
                                break;
                            case "022":
                                // Stream data into an SQL compliant database.
                                List<String> tmpList = new ArrayList<>(Arrays.asList(APImsg.APIget("payload").split("<im>")));
                                boolean resetEnc = false;
                                if (!NamedCommon.encRaw && !APImsg.APIget("encrypt").equals("")) {
                                    NamedCommon.encRaw = APImsg.APIget("encrypt").toLowerCase().equals("true");
                                    resetEnc = true;
                                }
                                if (NamedCommon.serial.equals("")) NamedCommon.serial = NamedCommon.pid;
                                NamedCommon.isNRT = true;
                                NamedCommon.Komma = ",";
                                //
                                // -----------------------------------------------------------------------------------------
                                //  In kStream, I pack a few things into a property called "payload". These things are;
                                //      1.  account
                                //      2.  file
                                //      3.  itemId
                                //      4.  record
                                // -----------------------------------------------------------------------------------------
                                //
                                if (tmpList.size() == 4) {
                                    uStreamsInitialise(tmpList);
                                    if (!NamedCommon.ZERROR) uStreamsFetch();
                                    if (!NamedCommon.ZERROR) uStreamsBurst();
                                    if (!NamedCommon.ZERROR) CreateViews();
                                    NamedCommon.Komma = saveKomma;
                                } else {
                                    uCommons.uSendMessage("Badly formed uStream record. Ignoring.");
                                    NamedCommon.ZERROR = true;
                                    NamedCommon.Zmessage = "";
                                }
                                if (resetEnc) NamedCommon.encRaw = false;
                                laps = (System.nanoTime() - NamedCommon.startM);
                                laps = laps / div;
                                uCommons.uSendMessage("Process time: " + laps);
                                break;
                            case "023":
                                // ************************ //
                                //  NOT IN USE !!!          //
                                // ************************ //
                                if (!NamedCommon.encRaw && !APImsg.APIget("encrypt").equals("")) {
                                    NamedCommon.encRaw = APImsg.APIget("encrypt").toLowerCase().equals("true");
                                }
                                if (NamedCommon.serial.equals("")) NamedCommon.serial = NamedCommon.pid;
                                NamedCommon.isNRT = true;
                                UplCtl = new ArrayList<>();
                                for (int u = 0; u <= 15; u++) {UplCtl.add("");}
                                UplCtl.set(3, NamedCommon.xMap);
                                UplCtl.set(4, inTask);
                                NamedCommon.Komma = ",";
                                if (!NamedCommon.ZERROR) uStreamsBurst();
                                if (!NamedCommon.ZERROR) CreateViews();
                                NamedCommon.Komma = saveKomma;
                                break;
                            case "025":
                                // Stream data into a Kafka Topic
                                Send2Kafka();
                                laps = (System.nanoTime() - NamedCommon.startM);
                                laps = laps / div;
                                uCommons.uSendMessage("Process time: " + laps);
                                break;
                            case "050":
                                // ------------------------------------------------------------------------
                                // Handle a group of read events
                                // ------------------------------------------------------------------------

                                uCommons.uSendMessage("--------------------");
                                uCommons.uSendMessage("050 - Reader service");
                                uCommons.uSendMessage("--------------------");

                                if (APImsg.APIget("grp").equals("")) {
                                    uRestLoader();
                                } else {
                                    // NamedCommon.restMaps is cleared and rebuilt in uCommons.MessageToAPI
                                    uCommons.uSendMessage("        grp: " + APImsg.APIget("grp"));
                                    uCommons.uSendMessage("--------------------");
                                    int nbrMaps = NamedCommon.restMaps.size();
                                    ArrayList<String> replies = new ArrayList<>();
                                    String tmpHold;
                                    for (int m = 0; m < nbrMaps; m++) {
                                        NamedCommon.xMap = NamedCommon.restMaps.get(m);
                                        NamedCommon.item = NamedCommon.restItems.get(m);
                                        APImsg.APIset("map", NamedCommon.xMap);
                                        APImsg.APIset("item", NamedCommon.item);
                                        if (NamedCommon.xMap.equals("") || NamedCommon.item.equals("")) continue;
                                        uRestLoader();
                                        JSONObject jRec = new JSONObject(responseText);
                                        tmpHold = jRec.get("body").toString();
                                        jRec = null;
                                        jRec = new JSONObject(tmpHold);
                                        tmpHold = jRec.get("response").toString();
                                        jRec = null;
                                        jRec = new JSONObject(tmpHold);
                                        JSONObject jObj = new JSONObject();
                                        jObj.put("Source", NamedCommon.xMap);
                                        jObj.put("ItemId", NamedCommon.item);
                                        jObj.put("Record", jRec);
                                        replies.add(jObj.toString());
                                        jObj = null;
                                        jRec = null;
                                    }
                                    JSONArray jArr = new JSONArray();
                                    String jString, source, item, record;
                                    for (int i = 0; i < replies.size(); i++) {
                                        jString = replies.get(i);
                                        try {
                                            JSONObject jRow = new JSONObject(jString);
                                            source = jRow.get("Source").toString();
                                            item = jRow.get("ItemId").toString();
                                            record = jRow.get("Record").toString();
                                            jRow = null;
                                            jRow = new JSONObject(record);
                                            JSONObject newRow = new JSONObject();
                                            newRow.put("Source", source);
                                            newRow.put("ItemId", item);
                                            newRow.put("Record", jRow);
                                            jArr.put(newRow);
                                            newRow = null;
                                            jRow = null;
                                        } catch (JSONException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    responseText += "]";
                                    if (APImsg.APIget("format").toLowerCase().equals("xml")) {
                                        responseText = uConnector.xml2json(responseText);
                                    } else {
                                        responseText = jArr.toString();
                                    }
                                    jArr = null;
                                }
                                break;
                            case "055":
                                uCommons.uSendMessage("--------------------");
                                uCommons.uSendMessage("055 - Writer service");
                                uCommons.uSendMessage("--------------------");
                                uCommons.uSendMessage("      mscat: " + APImsg.APIget("mscat"));
                                if (NamedCommon.sendACK) {
                                    uCommons.uSendMessage("HandleMicroService() :: sending ACK");
                                    String resp = DataConverter.ResponseHandler("200", "OK", "ACK", "xml");
                                    Hop.start(resp, "", nextBrk, NamedCommon.inputQueue, "ACK", NamedCommon.CorrelationID.replaceAll("\\.", "_"));
                                }
                                responseText = coreCommons.HandleMicroService(message);
                                if (responseText.equals("") && NamedCommon.isWebs) {
                                    uCommons.uSendMessage("   >. Read uRESPONSES " + NamedCommon.zID);
                                    responseText = u2Commons.ReadAnItem("uRESPONSES", NamedCommon.zID, "1", "", "");
                                    if (responseText.equals("")) {
                                        uCommons.uSendMessage("   >. Try again.");
                                        uCommons.Sleep(0);
                                        responseText = u2Commons.ReadAnItem("uRESPONSES", NamedCommon.zID, "1", "", "");
                                        if (responseText.equals("")) {
                                            uCommons.uSendMessage("   >. Response cannot be found!");
                                        } else {
                                            uCommons.uSendMessage("   >. Found the response");
                                        }
                                    } else {
                                        uCommons.Sleep(0);
                                    }
                                    uCommons.uSendMessage("   >. Delete uRESPONSES " + NamedCommon.zID);
                                    u2Commons.DeleteAnItem("uRESPONSES", NamedCommon.zID);
                                }
                                responseText = DataConverter.ResponseHandler("200", "DONOTALTER", responseText, APImsg.APIget("FORMAT").toUpperCase());
                                break;
                            case "910":
                                // Load-UDE-Programs
                                LoadBP();
                                break;
                            case "920":
                                // Load-Customer-Programs
                                LoadUplBp.LoadCustPgms();

                                break;
                            case "930":
                                MarkerMessage();
                                break;
                            case "990":
                                // Encrypt a string
                                String inStr = APImsg.APIget("string");
                                if (!inStr.trim().equals("")) {
                                    uCipher.isLic = false;
                                    if (inStr.indexOf("<nl>") < 0) {
                                        responseText = inStr + "=ENC(" + uCipher.Encrypt(inStr) + ")";
                                    } else {
                                        String[] inList = inStr.split("<nl>");
                                        responseText = "ENC(";
                                        for (int lst = 0; lst < inList.length; lst++) {
                                            responseText += uCipher.Encrypt(inList[lst].trim()) + "<nl>";
                                        }
                                        responseText += ")";
                                    }
                                    uCipher.isLic = true;
                                } else {
                                    responseText = "No string in message to encrypt.";
                                    NamedCommon.ZERROR = true;
                                    NamedCommon.Zmessage = responseText;
                                }
                                break;
                            case "999":
                                PingTest();
                                boolean tmpSW = NamedCommon.isWebs;
                                NamedCommon.isWebs = false;
                                if (NamedCommon.ZERROR) {
                                    responseText = DataConverter.ResponseHandler("401", NamedCommon.Zmessage, "FAIL", APImsg.APIget("FORMAT").toUpperCase());
                                    NamedCommon.uStatus = "401";
                                } else {
                                    responseText = DataConverter.ResponseHandler("200", "ok", responseText, APImsg.APIget("FORMAT").toUpperCase());
                                }
                                NamedCommon.isWebs = tmpSW;
                                u2Commons.ClearVoc("");
                                break;
                            default:
                                responseText = "Error on inTask [" + inTask + "]";
                                NamedCommon.ZERROR = true;
                                NamedCommon.Zmessage = responseText;
                                break;
                        }
                    }
                }
            } else {
                if (!NamedCommon.isRest) {
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                } else {
                    // format the Zmessage for uRest - it's a setup error
                    String eStatus = "424";
                    responseText = NamedCommon.Zmessage.replaceAll("\\<", "");
                    responseText = responseText.replaceAll("\\>", "");
                    String eDescr = NamedCommon.ReturnCodes.get(Integer.valueOf(eStatus));
                    if (eDescr.equals("")) eDescr = "ReturnCode [" + eDescr + "] not found.";
                    responseText = DataConverter.ResponseHandler(eStatus, eDescr, responseText, APImsg.APIget("FORMAT").toUpperCase());
                    NamedCommon.uStatus = eStatus;
                }
            }

            if (NamedCommon.debugging) uCommons.uSendMessage("finished processing message ");
            if (responseText.contains("<<FAIL>>")) uCommons.uSendMessage(responseText);
        }

        if (NamedCommon.ZERROR && !NamedCommon.isRest) {
            if (!responseText.contains("<<FAIL>>")) {
                if (!NamedCommon.Zmessage.contains("<<FAIL>>")) responseText = "<<FAIL>> ";
                responseText += NamedCommon.Zmessage;
                if (!NamedCommon.ZERROR) NamedCommon.ZERROR = true;
            } else {
                responseText += " " + NamedCommon.Zmessage;
            }
        }

        finishM = System.nanoTime();
        laps = (finishM - NamedCommon.startM) / div;

        lastMsg = NamedCommon.MessageID;

        if (inTask.equals("990")) {
            // String Encryption
            Hop.start(responseText, "", nextBrk, APImsg.APIget("replyto"), "", NamedCommon.CorrelationID);
            responseText = "";
        } else {
            if (!NamedCommon.BurstRestarted) {
                if (NamedCommon.isWhse && !NamedCommon.ZERROR && !NamedCommon.isNRT) Send2Finish();
            }
            if (NamedCommon.protocol.equals("real")) SourceDB.DisconnectSourceDB();
        }

        if (NamedCommon.uvReset && !NamedCommon.isNRT) {
            uCommons.uSendMessage(NamedCommon.block);
            uCommons.uSendMessage("SourceDB: Disconnecting (" + NamedCommon.databaseType + ")  " + NamedCommon.dbhost + " [" + NamedCommon.dbuser + "]");
            SourceDB.DisconnectSourceDB();
            uCommons.uSendMessage(NamedCommon.block);
        }

        APImsg.instantiate();
        if (NamedCommon.uRequests != null) NamedCommon.uRequests = u2Commons.uClose(NamedCommon.uRequests);
        if (NamedCommon.isRest) uCommons.uSendMessage("   >. MessageProtocol finished. Payload.length("+responseText.length()+")");
        return responseText;
    }

    public static String stringifyMessage(String theMessage) {
        StringBuilder answer = new StringBuilder();
        JSONObject obj = null;
        try {
            obj = new JSONObject(theMessage);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        if (obj == null) return "";
        if (obj.length() < 1) return "";
        String jHeader = "request";
        Iterator<String> jKeys = null;
        try {
            jKeys = obj.getJSONObject(jHeader).keys();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = "";
            try {
                zval = obj.getJSONObject(jHeader).get(zkey).toString();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            answer.append(zkey.toUpperCase() + "<is>" + zval + "<tm>");
        }
        return answer.toString();
    }

    public static String GetQueueName(String inval) {
        // inval will be a task such as 012 or 014, etc.
        String ans = "000-Queue-Error";
        for (int i=0 ; i < qNameArray.size() ; i++) {
            if (qNameArray.get(i).startsWith(inval)) {
                ans = qNameArray.get(i);
                break;
            }
        }
        return ans;
    }

    private static void PingTest() {
        uCommons.uSendMessage("---------------------------------------------------------------");
        if (!NamedCommon.inputQueue.equals("unknown_queue")) {
            uCommons.uSendMessage("rFuel PING-TEST on " + NamedCommon.messageBrokerUrl + " " + NamedCommon.inputQueue);
        } else {
            uCommons.uSendMessage("rFuel PING-TEST on " + NamedCommon.dbhost + " " + NamedCommon.dbpath);
        }
        uCommons.uSendMessage("---------------------------------------------------------------");
        String pingValue;

        pingValue = String.valueOf(NamedCommon.debugging).toUpperCase();
        uCommons.eMessage = "Debug mode is " + pingValue;
        responseText += "<debug>" + pingValue + "</debug>";
        if (NamedCommon.debugging) uCommons.uSendMessage(uCommons.eMessage);

        uCommons.eMessage = "Licence mode is ";
        if (NamedCommon.CPL && !NamedCommon.protocol.equals("real")) {
            responseText += "<LicenceMode>CPL</LicenceMode>";
            responseText += "<minPool>" + String.valueOf(NamedCommon.minPoolSize) + "</minPool>";
            responseText += "<maxPool>" + String.valueOf(NamedCommon.maxPoolSize) + "</maxPool>";
            uCommons.eMessage += "CPL  (";
            uCommons.eMessage += NamedCommon.minPoolSize + "/" + NamedCommon.maxPoolSize + ")";
            if (NamedCommon.debugging) uCommons.uSendMessage(uCommons.eMessage);
            uCommons.eMessage = "Secure mode is ";
            if (NamedCommon.uSecure) {
                uCommons.eMessage += "ON";
                responseText += "<dbSecure>ON</dbSecure>";
            } else {
                uCommons.eMessage += "OFF" + "";
                responseText += "<dbSecure>OFF</dbSecure>";
            }
            if (NamedCommon.debugging) uCommons.uSendMessage(uCommons.eMessage);
        } else {
            uCommons.eMessage += "SEAT";
            if (NamedCommon.debugging) uCommons.uSendMessage(uCommons.eMessage);
            responseText += "<LicenceMode>SEAT</LicenceMode>";
        }

        responseText += "<mqTimeout>" + uCommons.oconvM(String.valueOf(NamedCommon.mqWait), "MD0,") + " seconds" + "</mqTimeout>" +
                "<dbTimeout>" + uCommons.oconvM(String.valueOf(NamedCommon.dbWait), "MD0,") + " seconds" + "</dbTimeout>" +
                "<hbTimeout>" + uCommons.oconvM(String.valueOf(NamedCommon.mqHeartBeat), "MD0,") + " seconds" + "</hbTimeout>" +
                "<vtTimer>" + uCommons.oconvM(String.valueOf(NamedCommon.vtPing), "MD0,") + " seconds" + "</vtTimer>" +
                "<dbType>" + NamedCommon.databaseType + "</dbType>" +
                "<protocol>" + NamedCommon.protocol + "</protocol>";

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("rFuel idle timeout settings:-");
            uCommons.uSendMessage("   .) MQ idle timeout: " + uCommons.oconvM(String.valueOf(NamedCommon.mqWait), "MD0,") + " seconds");
            uCommons.uSendMessage("   .) DB idle timeout: " + uCommons.oconvM(String.valueOf(NamedCommon.dbWait), "MD0,") + " seconds");
            uCommons.uSendMessage("   .) <<heartbeat>>  : " + uCommons.oconvM(String.valueOf(NamedCommon.mqHeartBeat), "MD0,") + " seconds");
            uCommons.uSendMessage("   .) VT ping timer  : " + uCommons.oconvM(String.valueOf(NamedCommon.vtPing), "MD0,") + " seconds");
            uCommons.uSendMessage("   .) Host.Domain is : " + NamedCommon.hostname);
            uCommons.uSendMessage("   .) Database type  : " + NamedCommon.databaseType);
            uCommons.uSendMessage("   .) Using protocol : " + NamedCommon.protocol);
        }
        if (!NamedCommon.isNRT) u2Commons.CheckSource();
    }

    private static void MarkerMessage() {
        // the purpose of a marker message is to record in UPLCTL when a bunch of messages began
        // and when they finished.
        // message format is:
        // task = 930
        // marker = started   or   stopped
        UplCtl = new ArrayList<>();
        UplCtl.add(0, NamedCommon.BatchID);
        UplCtl.add(1, "MessageMarker");
        UplCtl.add(2, NamedCommon.serial);
        UplCtl.add(3, Genesis);                     // "" if not a genesis group
        UplCtl.add(4, ("("+inTask+") " + APImsg.APIget("marker")));
        UplCtl.add(5, "");
        UplCtl.add(6, "");
        UplCtl.add(7, "");
        UplCtl.add(8, "0");
        UplCtl.add(9, "0");
        UplCtl.add(10, "0");
        UplCtl.add(11, "0");
        UplCtl.add(12, "0");
        UplCtl.add(13, "0");
        UplCtl.add(14, "0");
        UplCtl.add(15, "0");
        UpdateUPLCTL();
    }

    private static void CreateViews() {
        if (NamedCommon.datOnly) return;
        // need a map property which prohibits createViews, so I can have multiple maps going into 1 table
        // without each one dropping and creating the same view
        // e.g.   if (APImap.APIget("noview").equals("true")) return;
        if (APImap.APIget("noview").equals("true")) return;
        uCommons.uSendMessage("   .) Create View (if not exists)");
        String check = "[" + NamedCommon.SqlDatabase + "].[" + NamedCommon.SqlSchema + "].[" + NamedCommon.vwPrefix+uCommons.APIGetter("sqlTable") + "]";

        uCommons.uSendMessage("      > Create View: " + check);

        if (NamedCommon.streamedFiles.indexOf(check) >= 0 && NamedCommon.isNRT) {
            uCommons.uSendMessage("   .) Done       : ");
            return;
        }

        UniDynArray LineArray;
        if (!flowThru) {
            LineArray = GetCSVcols();
            if (NamedCommon.ZERROR) return;
        } else {
            LineArray = BurstSplit.LineArray;
        }

        String createView = SqlCommands.CreateView(NamedCommon.SqlDatabase, NamedCommon.SqlSchema, NamedCommon.sqlTarget, LineArray);
        if (createView.length() > 0) {
            ArrayList<String> DDL = new ArrayList<>();
            // if (!NamedCommon.BulkLoad) {
            // CREATE VIEW must be the first command in DDL
            // execute all commands before CREATE VIEW
            if (!NamedCommon.sqlLite) {
                ArrayList<String> cmdArray = new ArrayList<>(Arrays.asList(createView.split("\\r?\\n")));
                for (int i = 0; i < cmdArray.size(); i++) {
                    if (cmdArray.get(i).equals("")) continue;
                    if (cmdArray.get(i).toUpperCase().startsWith("CREATE VIEW")) {
                        SqlCommands.ExecuteSQL(DDL);
                        DDL.clear();
                    }
                    DDL.add(cmdArray.get(i));
                }
                if (DDL.size() > 0) SqlCommands.ExecuteSQL(DDL);
                cmdArray.clear();
            } else {
                DDL.add(createView);
                SqlCommands.ExecuteSQL(DDL);
            }
            DDL = null;
            createView = null;
        }
        if (NamedCommon.Proceed && !flowThru) {
            NamedCommon.burstCnt = Send2NextProcess(nextBrk, NamedCommon.burstCnt, nextCor + NamedCommon.xMap);
        }

        uCommons.uSendMessage("      . done");
        if (NamedCommon.isNRT) NamedCommon.streamedFiles.add(check);
    }

    public static UniDynArray GetCSVcols() {
        UniDynArray LineArray;
        uCommons.uSendMessage("   .) Get columns from dsd");
        // -------------------- for each csv file in the map -------------------------
        uCommons.GetAPImap(maps + NamedCommon.xMap);
        int csvLen = NamedCommon.csvList.length;
        String csvName, content;
        ArrayList<String> AllcsvLines = new ArrayList<String>();
        for (int i = 0; i < csvLen; i += 1) {
            csvName = maps + NamedCommon.csvList[i];
            List<String> csvLine = null;
            try {
                content = new String(Files.readAllBytes(Paths.get(csvName)));
                if (content.startsWith("ENC(")) {
                    content = uCipher.Decrypt(content);
                } else {
                    if (NamedCommon.KeepSecrets) {
                        uCommons.WriteDiskRecord(csvName + "_text", content);
                        uCipher.isLic = false;
                        String encCsv = uCipher.Encrypt(content);
                        uCommons.WriteDiskRecord(csvName, encCsv);
                        uCipher.isLic = true;
                        encCsv = "";
                    }
                }
                csvLine = new ArrayList<String>(Arrays.asList(content.split("\\r?\\n")));
                AllcsvLines.addAll(csvLine);
            } catch (IOException e) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "<<FAIL>> Cannot find " + csvName;
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return null;
            }
        }
        return uCommons.PrepareCsvDetails(AllcsvLines);
    }

    private static void UpdateUPLCTL() {
        if (NamedCommon.datOnly) return;
        uCommons.uSendMessage("Updating UPLCTL ---------------------");
        String ctlLine = "", cma = "", tmp = "";
        ArrayList DDL = new ArrayList<String>();
        if (NamedCommon.SqlDBJar.equals("MSSQL")) {
            if (!NamedCommon.SqlDatabase.equals(NamedCommon.rawDB))  ctlLine += "USE " + NamedCommon.rawDB + "  ";
            ctlLine += "INSERT INTO [" + NamedCommon.rawDB + "].[upl].[UPLCTL]";
        } else if (NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE")) {
            ctlLine += "INSERT INTO upl.UPLCTL";
        } else {
            ctlLine += "INSERT INTO " + NamedCommon.rawDB + ".upl_UPLCTL";
        }
        ctlLine += " (BatchID,RunType,Serial,Map,Task,Source,SelCmd,Target,rowsIN,rowsCHECKED,rowsPROCESSED,"
                + "rowsEMPTY,rowsNULL,rows2BIG,rowsOUT,RunTime) VALUES (";
        long finishP = System.nanoTime();
        laps = (finishP - NamedCommon.startM) / div;
        UplCtl.set(15, df.format(laps));
        if ((NamedCommon.xMap).toLowerCase().contains(shell)) {
            UplCtl.set(3, NamedCommon.xMap+":"+uCommons.APIGetter("u2file"));
        }
        for (int u = 0; u <= 15; u++) {
            tmp = UplCtl.get(u);
            if (u >= 8 && u <= 15) {
                if (tmp.equals("")) tmp = "0";
                if (tmp.contains(".")) {
                    int iTmp1 = Integer.valueOf(uCommons.FieldOf(tmp, "\\.", 1));
                    int iTmp2 = Integer.valueOf(uCommons.FieldOf(tmp, "\\.", 2));
                    if (iTmp2 > 50) iTmp1++;
                    tmp = String.valueOf(iTmp1);
                }
                tmp = tmp.replaceAll("\\ ", "");
                ctlLine += cma + Integer.valueOf(tmp);
            } else {
                if (tmp.indexOf("'") > -1) tmp = tmp.replaceAll("\\'", "''");
                ctlLine += cma + "'" + tmp + "'";
            }
            cma = ",";
        }

        ctlLine += ")";
        DDL.add(ctlLine);
        if (NamedCommon.debugging) uCommons.uSendMessage(ctlLine);
        SqlCommands.ExecuteSQL(DDL);
        if (!NamedCommon.SqlReply.equals("null") && !NamedCommon.SqlReply.equals("")) {
            uCommons.uSendMessage(NamedCommon.SqlReply);
            uCommons.uSendMessage(ctlLine);
        }

        if (!NamedCommon.u2Source.equals("@DUMMY")) return;

        String useSCH="upl", wkfile = "workfile"+UplCtl.get(2);
        String dbObj = "["+NamedCommon.SqlDatabase+"].["+useSCH+"].["+wkfile+"]";

        String dropTable = SqlCommands.DropTable(NamedCommon.SqlDatabase, useSCH, wkfile);

        if (!dropTable.equals("")) {
            DDL.clear();
            uCommons.uSendMessage("      > Dropping table " + dbObj);
            DDL.add(dropTable);
            SqlCommands.ExecuteSQL(DDL);
            if (NamedCommon.ZERROR) return;
            DDL.clear();
        }
        uCommons.uSendMessage("Done.");
    }

    private static void CheckUPLCTL() {
        if (NamedCommon.datOnly) return;
        ArrayList<String> DDL = new ArrayList<String>();
        uCommons.uSendMessage("Prepare UPLCTL and the upl schema");
        String HoldSchema = NamedCommon.SqlSchema;
        String HoldDatabs = NamedCommon.SqlDatabase;
        NamedCommon.SqlSchema = "upl";
        if (HoldDatabs.startsWith("$")) NamedCommon.SqlDatabase = APImsg.APIget("sqldb");
        if (NamedCommon.rawDB.equals("$$$")) NamedCommon.rawDB = NamedCommon.SqlDatabase;
        // Prefixes for column names:-
        //   -    keytype
        //   *    maxtype
        //   @    inttype
        String[] tCols = {"BatchID", "RunType", "Serial", "Map", "Task", "Source", "SelCmd", "Target", "@rowsIN", "@rowsCHECKED", "@rowsPROCESSED", "@rowsEMPTY", "@rowsNULL", "@rows2BIG", "@rowsOUT", "@RunTime"};

        String ctlLine = SqlCommands.CreateTable(NamedCommon.rawDB, NamedCommon.SqlSchema, "UPLCTL", tCols);

        DDL.clear();
        if (ctlLine.length() > 0) {
            DDL.add(ctlLine);
            SqlCommands.ExecuteSQL(DDL);
        }
        NamedCommon.SqlSchema = HoldSchema;
        NamedCommon.SqlDatabase = HoldDatabs;
    }

    private static String SetCorrel(String theTask) {
        String ans = "";
        switch (theTask) {
            case "010":
                ans = "Start>>_";
                break;
            case "012":
                ans = "Fetch>>_";
                break;
            case "014":
                ans = "Burst>>_";
                break;
            case "015":
                ans = "Cview>>_";
                break;
            case "017":
                ans = "Flip>>>_";
                break;
            case "050":
                ans = "REST>>>_";
                break;
            case "055":
                ans = "MSVC>>>_";
                break;
            case "910":
                ans = "LoadBP>_";
                break;
            default:
                ans = "ERROR>>_";
                ans = APImsg.APIget("correlationid");
        }
        ans = theTask + "_" + ans;
        return ans;
    }

    private static void MapTrigger() {
        if (NamedCommon.ZERROR) return;
        if (qTask.equals("010")) return;
        boolean looping = false;
        if (qTask.equals("017")) {
            if (NamedCommon.mapLooping && APImap.APIget("loop").toLowerCase().equals("true")) {
                NamedCommon.mTrigger = NamedCommon.xMap;
                NamedCommon.mTrigQue = "010";
                NamedCommon.RunType = "INCR";      // you can come in REFRESH but looping should be INCR
                looping = true;
            }
        }

        if (NamedCommon.mTrigger.equals("")) return;
        if (NamedCommon.mTrigQue.equals("")) return;

        TrigMsg = true;
        String holdmap = NamedCommon.xMap;
        String holdtsk = NamedCommon.task;
        //
        // Enhanced to allow multiple triggers in same map
        // triggers are comma separated
        // if this task = trigger task, move to the next
        // if there is no next trigger - return with no action taken.
        //
        boolean mapsTodo = false;
        if (NamedCommon.mTrigger.contains(",")) {
            String[] arrTmaps = NamedCommon.mTrigger.split(",");    //  array of trigger maps
            String[] arrTques = NamedCommon.mTrigQue.split(",");    //  array of trigger queues
            int currTask = Integer.parseInt(NamedCommon.task), nextTask=0;
            int nbrTrigs = arrTmaps.length;
            for (int t=0 ; t< nbrTrigs ; t++ ) {
                nextTask = Integer.parseInt(arrTques[t]);
                if (nextTask > currTask) {
                    NamedCommon.xMap = arrTmaps[t];
                    NamedCommon.task = arrTques[t];
                    mapsTodo = true;
                    break;
                }
            }
        } else {
            NamedCommon.xMap = NamedCommon.mTrigger;
            NamedCommon.task = NamedCommon.mTrigQue;
            mapsTodo = true;
        }

        if (!mapsTodo) return;

        nextTask = NamedCommon.task;
        ChangeMessage("task=", nextTask);
        ChangeMessage("map=", NamedCommon.xMap);
        ChangeMessage("runtype", NamedCommon.RunType);
        if (looping) ChangeMessage("incr_loaddte", "999999999999999");
        int fnd = TasksArray.indexOf(NamedCommon.task);
        responseText = "<<PASS>> triggered " + NamedCommon.task;

        if (fnd >= 0) {
            String send2 = qNameArray.get(fnd);
            queMax = Integer.valueOf(Responders.get(fnd));
            String holdQue = nextQue;
            nextQue = send2;
            nextCtr = 1;
            uCommons.uSendMessage("***************************************************");
            uCommons.uSendMessage("Trigger (" + holdmap + ") >> "
                    + NamedCommon.xMap + " to Task queue (" + NamedCommon.task + ")");
            uCommons.uSendMessage("***************************************************");
            NamedCommon.burstCnt = Send2NextProcess(nextBrk, NamedCommon.burstCnt, nextCor + NamedCommon.xMap);
            nextQue = holdQue;
        }

        NamedCommon.xMap = holdmap;
        NamedCommon.task = holdtsk;
    }

    private static void LoadBP() {
        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        if (!NamedCommon.tConnected) SqlCommands.ConnectSQL();
        if (NamedCommon.ZERROR) return;
        uCipher.SetAES(false, "", "");
        uCommons.uSendMessage("********************");
        uCommons.uSendMessage("Loading UDE programs");
        uCommons.uSendMessage("********************");
        responseText = LoadUplBp.LoadPgms();
//        if (responseText.contains("<<PASS>>")) {
//            nextCtr = Send2NextProcess(nextBrk, -1, nextCor + NamedCommon.xMap);
            responseText = "Load of U2 Agents completed";
//        }
        uCommons.uSendMessage(responseText);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
    }

    private static void LoadCustomerBP() {
    }

    private static void SetupRun() {
        int mapSW = 0;
        flowThru = true;
        if (NamedCommon.xMap.equals("*")) mapSW = 1;
        if (NamedCommon.xMap.endsWith(".gog")) mapSW = 2;
        if (NamedCommon.xMap.endsWith(".grp")) mapSW = 3;
        if (NamedCommon.xMap.endsWith(".map")) mapSW = 4;
        if (NamedCommon.xMap.endsWith(".pre")) mapSW = 5;       // e.g. shell.pre : do pre-run uni and sql tasks
        if (inTask.equals("910")) mapSW = 91;
        switch (mapSW) {
            case 0:
                uCommons.uSendMessage("Unknown map [" + NamedCommon.xMap + "]");
                break;
            case 1:
                DoAllMaps();
                break;
            case 2:
                GrpOfGrps();
//                if (!NamedCommon.ZERROR) MapTrigger();
                break;
            case 3:
                GrpOfMaps();
//                if (!NamedCommon.ZERROR) MapTrigger();
                break;
            case 4:
                HandleMap();
                if (!NamedCommon.ZERROR) MapTrigger();
                break;
            case 5:
                PreExecCommands();
                if (!NamedCommon.ZERROR) MapTrigger();
                break;
            case 91:
                LoadBP();
                break;
        }
    }

    private static void DoAllMaps() {
        String[] MapNames = ExtractManager.GetAllMaps();
        int nbrMaps = MapNames.length;
        for (int m = 0; m < nbrMaps; m++) {
            NamedCommon.xMap = GetMapOnly(MapNames[m]);
            uCommons.GetMap(MapNames[m]);
            if (!NamedCommon.ZERROR) {
                uCommons.uSendMessage("======================================");
                uCommons.uSendMessage((m + 1) + " of " + nbrMaps + " ..>>.. " + NamedCommon.xMap);
                ChangeMessage("map=", NamedCommon.xMap);
                HandleMap();
                messageText = messageHold;
            }
            NamedCommon.ZERROR = false;
        }
    }

    private static void GrpOfGrps() {
        uCommons.uSendMessage("Processing: "+NamedCommon.xMap);
        String contains = "Containing: ";
        metaGrps = true;
        String HoldMap = NamedCommon.xMap;
        Properties mapProps;
        mapProps = uCommons.LoadProperties(maps + NamedCommon.xMap);

        // Is this marked as the Genesis message? If so, setup Genesis in conf/
        String genesis = APImap.APIget("genesis");
        if (!genesis.isEmpty()) SetupGenesis(genesis);

        if (APImsg.APIget("preuni").equals("")) APImsg.APIset("preuni", mapProps.get("preuni").toString());
        if (APImsg.APIget("presql").equals("")) APImsg.APIset("presql", mapProps.get("presql").toString());
        if (NamedCommon.StopNow.contains("<<FAIL>>")) NamedCommon.ZERROR = true;
        if (!NamedCommon.ZERROR) PreExecCommands();
        APImsg.APIset("preuni", "");
        APImsg.APIset("presql", "");
        uCommons.uSendMessage("*****************************************************************");
        uCommons.uSendMessage("****           Distributing group-of-group items             ****");
        uCommons.uSendMessage("*****************************************************************");
        if (!NamedCommon.ZERROR) {
            String[] grpList = uCommons.APIGetter("grps", "ERROR-IN-YOUR-grp-DEFINITION.map").split(",");
            int nbrMaps = grpList.length;
            // show first, then process -----------------------------------
            for (int mm=0; mm< nbrMaps; mm++) {
                grpList[mm] = grpList[mm].trim();
                uCommons.uSendMessage(contains+grpList[mm]);
                contains = "          : ";
            }
            uCommons.uSendMessage("*****************************************************************");
            for (int mm = 0; mm < nbrMaps; mm++) {
                NamedCommon.xMap = grpList[mm];
                GrpOfMaps();
            }
        }
        NamedCommon.xMap = HoldMap;
        uCommons.GetMap(maps + NamedCommon.xMap);
    }

    private static void GrpOfMaps() {
        metaGrps = true;
        String HoldMap = NamedCommon.xMap;
        uCommons.uSendMessage("Received :: " + NamedCommon.xMap);
        uCommons.SetAPIMap(maps + NamedCommon.xMap);
        Properties mapProps;
        mapProps = uCommons.LoadProperties(maps + NamedCommon.xMap);

        // Is this marked as the Genesis message? If so, setup Genesis in conf/
        String genesis = APImap.APIget("genesis");
        if (!genesis.equals("")) SetupGenesis(genesis);

        if (APImsg.APIget("preuni").equals("")) APImsg.APIset("preuni", mapProps.get("preuni").toString());
        if (APImsg.APIget("presql").equals("")) APImsg.APIset("presql", mapProps.get("presql").toString());
        if (NamedCommon.StopNow.contains("<<FAIL>>")) NamedCommon.ZERROR = true;
        if (!NamedCommon.ZERROR) PreExecCommands();

        APImsg.APIset("preuni", "");
        APImsg.APIset("presql", "");
        if (!NamedCommon.ZERROR) {
            uCommons.uSendMessage("**************************************");
            uCommons.uSendMessage("****   Distributing group items   ****");
            uCommons.uSendMessage("**************************************");
            String[] grpList = uCommons.APIGetter("maps", "ERROR-IN-YOUR-grp-DEFINITION.map").split(",");
            int nbrMaps = grpList.length;
            String holdTask = nextTask, fqn;
            String send2Task;
            if (inTask.equals("010")) {
                send2Task = nextTask;
            } else {
                nextTask = thisTask;
                send2Task = thisTask;
            }
            uCommons.uSendMessage("Sharing the group across " + queMax + " lot(s) of " + nextQue + " queues");
            nextCor = thisCor;
            for (int mm = 0; mm < nbrMaps; mm++) {
                NamedCommon.xMap = grpList[mm];
                uCommons.uSendMessage("======================================");
                if (inTask.equals("014") && NamedCommon.isPrt) {
                    ChangeMessage("RunType=", "PART");
                    ChangeMessage("proceed=", "false");
                    uCommons.uSendMessage(NamedCommon.xMap + " >>  ***prt*** - in task 014 :: RunType is now 'PART'");
                }
                uCommons.GetMap(maps + NamedCommon.xMap);
                if (!NamedCommon.ZERROR) {
                    ChangeMessage("task=", send2Task);
                    ChangeMessage("map=", NamedCommon.xMap);
                    if (!APImsg.APIget("presel").equals("")) {
                        String fName = NamedCommon.xMap;
                        fName = fName.replace("\\", "-");
                        fName = fName.replace("/", "-");
                        ChangeMessage("presel=", "PREsel(" + fName + ").txt");
                    }
                    uCommons.eMessage = (mm + 1) + " of " + nbrMaps + " Send >> " + NamedCommon.xMap
                            + " to queue(" + NamedCommon.burstCnt + ") "
                            + " on task(" + send2Task + ") ";
                    uCommons.uSendMessage(uCommons.eMessage);
                    HandleMap();            // Sends the message - that's it !!
                } else {
                    uCommons.uSendMessage(NamedCommon.xMap + "  >> Ignored.");
                }
                NamedCommon.ZERROR = false;
            }
            nextTask = holdTask;
            uCommons.uSendMessage("======================================");
        }
        NamedCommon.xMap = HoldMap;
    }

    private static void PreExecCommands() {
        String preSql = uCommons.APIGetter("presql", "");
        String preUni = uCommons.APIGetter("preuni", "");
        if (preUni.equals("") && preSql.equals("")) return;

        String[] grpList = uCommons.APIGetter("maps", "ERROR-IN-YOUR-grp-DEFINITION.map").split(",");
        uCommons.uSendMessage("*****************************************************************");
        uCommons.uSendMessage("****            Executing message commands                  *****");
        uCommons.uSendMessage("*****************************************************************");
        if (!preUni.equals("") && !NamedCommon.ZERROR) PreUni(preUni, grpList);
        if (!preSql.equals("") && !NamedCommon.ZERROR) PreSql(preSql);
    }

    public static void PreUni(String preUni, String[] grpList) {
        if (!NamedCommon.IsAvailableU2) {
            if (!SourceDB.ConnectSourceDB().contains("<<PASS>>")) {
                NamedCommon.ZERROR = true;
                uCommons.uSendMessage("<<FAIL>> SourceDB is unavailable");
                return;
            }
        }
        ArrayList<String> fNames = new ArrayList<>();
        List<String> uCmds = Arrays.asList(preUni.split("\\;"));
        uCommons.uSendMessage("Pre-execution " + NamedCommon.databaseType + " Statements::");
        int nbrcmds = uCmds.size(), nbrMaps = grpList.length;
        String uCmd = "", PAcmd = "PA"+NamedCommon.FMark, PAitem="PA-"+inTask+"."+NamedCommon.pid;
        boolean preSelect = false, success = false;

        for (int pu = 0; pu < nbrcmds; pu++) {
            uCmd = uCmds.get(pu);
            if (uCmd.startsWith("\\")) uCmd = uCmd.substring(1, uCmd.length());
            uCmd = Substitutions(uCmd);
            uCommons.uSendMessage(uCommons.RightHash(String.valueOf(pu + 1), 2) + " " + uCmd);
            PAcmd += uCmd + NamedCommon.FMark;
        }
        success = u2Commons.WriteAnItem("VOC", PAitem, "", "", "", PAcmd);
        if (success) u2Commons.uniExec(PAitem);

        if (!success) {
            uCommons.uSendMessage("<<FAIL>> U2 command failure in " + NamedCommon.xMap);
            NamedCommon.ZERROR = true;
            return;
        } else {
            success = u2Commons.DeleteAnItem("VOC", PAitem);
            if (!success) {
                uCommons.uSendMessage("WARNING *** Cannot delete VOC item: " + PAitem);
            }
        }
    }

    private static void PostUni() {
        if (!NamedCommon.postuni.equals("")) {
            List<String> uCmds = Arrays.asList(NamedCommon.postuni.split("\\;"));
            uCommons.uSendMessage("Post-run-execution of " + NamedCommon.databaseType + " Statements::");
            int nbrcmds = uCmds.size();
            boolean success = false;
            String uCmd = "";
            for (int pu = 0; pu < nbrcmds; pu++) {
                uCmd = uCmds.get(pu);
                if (uCmd.startsWith("\\")) uCmd = uCmd.substring(1, uCmd.length());
                uCmd = Substitutions(uCmd);
                uCommons.uSendMessage((pu + 1) + " " + uCmd);
                success = u2Commons.uniExec(uCmd);
                if (!success) {
                    responseText = "<<FAIL>> U2 post-run command (" + uCmd + ") in " + NamedCommon.xMap;
                    uCommons.uSendMessage(responseText);
                    NamedCommon.ZERROR = true;
                    return;
                }
            }
        }
    }

    private static void PreSql(String preSql) {
        if (SqlCommands.ConnectSQL()) {
            uCommons.uSendMessage("TargetDB: Connected");
            uCommons.uSendMessage("Pre-execution SQL Statements::");
            List<String> DDL = Arrays.asList(preSql.split("\\;"));
            String outLine="";
            for (int ps = 0; ps < DDL.size(); ps++) {
                if (DDL.get(ps).trim().isEmpty()) continue;
                if (DDL.get(ps).startsWith("\\")) DDL.set(ps, DDL.get(ps).substring(1, DDL.get(ps).length()));
                if (!DDL.get(ps).endsWith(";")) DDL.set(ps, DDL.get(ps)+";");
                if (DDL.get(ps).contains(lSplitter)) DDL.set(ps, DDL.get(ps).replace(lSplitter, "\n"));
                outLine = DDL.get(ps);
                uCommons.uSendMessage(uCommons.RightHash(String.valueOf(ps + 1), 2) + " " + outLine);
//                uCommons.uSendMessage(uCommons.RightHash(String.valueOf(ps + 1), 2) + " " + DDL.get(ps));
            }
            SqlCommands.ExecuteSQL(DDL);
            if (NamedCommon.ZERROR) {
                NamedCommon.ZERROR = false;
                NamedCommon.Zmessage = "";
            }
            if (NamedCommon.StopNow.contains("<<FAIL>>")) {
                uCommons.uSendMessage("<<FAIL>> SQL command in " + NamedCommon.xMap);
                if (NamedCommon.SqlReply.equals("null")) NamedCommon.SqlReply="";
                if (!NamedCommon.SqlReply.isEmpty()) {
                    String[] sqlErrs = NamedCommon.SqlReply.split("\\r?\\n");
                    int nbrErrs = sqlErrs.length;
                    for (int ee = 0; ee < nbrErrs; ee++) {
                        uCommons.uSendMessage(">> " + sqlErrs[ee]);
                    }
                }
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "SQL Errors reported.";
            }
        } else {
            uCommons.uSendMessage("<<FAIL>> TargetDB not available.");
            NamedCommon.ZERROR = true;
        }
    }

    private static void PostSQL() {
        String postsql= uCommons.APIGetter("postsql");
        if (postsql.endsWith(".sql")) {
            String junk = uCommons.ReadDiskRecord(maps+postsql);
            if (NamedCommon.ZERROR) {
                NamedCommon.Zmessage = "";
                NamedCommon.ZERROR   = false;
                return;
            }
            postsql = junk;
        }
        String[] commands = postsql.split("\\;");
        int nbrCmds = commands.length; //postsql.length() - postsql.replaceAll("\\;","").length();
        if (nbrCmds > 0) {
            if (!NamedCommon.tConnected) {
                if (!SqlCommands.ConnectSQL()) return;
            }
            uCommons.uSendMessage("TargetDB: Connected");
            uCommons.uSendMessage("Execute Post-Run SQL Statements::");
            ArrayList<String> DDL = BuildCMDArray(commands);
            String cmd="";
            for (int ps = 0; ps < DDL.size(); ps++) {
                cmd = DDL.get(ps);
                if (cmd.startsWith("\\")) cmd = cmd.substring(1, cmd.length());
                SqlCommands.ExecuteSQL(DDL);
            }
        }
    }

    public static void SetupGenesis(String genesis) {

        // "genesis" comes from the grp and/or gog E.g. genesis=Transaction-Block
        if (genesis.equals("")) return;

        uCommons.uSendMessage("Creating Genesis object -------------");
        String Holdmap = NamedCommon.xMap;

        // ----------------------------------------------------------------------------------------------
        // If a gog or grp has genesis=blah, create a process lineage registry for blah
        // ----------  Write the registry to conf/Genesis-{blah} ------------
        // The registry is a json object as follows:
        // {
        //  "map-name": "group-name",
        //      ...      :  ...
        //  "group-name": {
        //      "maps": ["map-name", "map-name", etc...],
        //       "gog": "gog-name or empty"
        //      },
        //  "gog-name": ["group-name", "group-name", etc...]
        // }
        //
        // Inject this process registry into the message as:
        //      "Genesis<is>blah<tm>"
        //      "Genereg<is>this-json-object<tm>
        // ----------------------------------------------------------------------------------------------

        if (!NamedCommon.tConnected) {
            SqlCommands.ConnectSQL();
            if (NamedCommon.ZERROR) SqlCommands.ReconnectService();
        }

        Genesis=genesis;
        uCommons.uSendMessage("[GENESIS] Marking "+genesis+" as started");
        APImsg.APIset("marker", genesis+"-STARTED");
        MarkerMessage();
        APImsg.APIdel("marker");

        JSONObject gene = new JSONObject();

        Properties tmpProps;
        String geneKey = "Genesis-" + genesis;
        String thisMap = NamedCommon.xMap;
        String grpGene="";
        int mCntr = 0;
        boolean geneDone = false, gogDone=false;
        String[] grps = new String[0];

        while (!geneDone) {

            // this needs to stay at xMap !!
            if (NamedCommon.xMap.endsWith("gog")) {
                if (!gogDone) {
                    grps = APImap.APIget("grps").split(",");
                    gene.put(thisMap, new JSONArray(Arrays.asList(grps)));

                    // Link each grp to its parent gog
                    for (String grp : grps) {
                        tmpProps = uCommons.LoadProperties(maps+grp);
                        grpGene = tmpProps.getProperty("genesis");
                        if (grpGene == null) grpGene = "";
                        if (!grpGene.isEmpty()) {
                            APImsg.APIset("marker", grpGene+"-STARTED");
                            MarkerMessage();
                            APImsg.APIdel("marker");
                        }
                        JSONObject grpObj = new JSONObject();
                        grpObj.put("maps", new JSONArray()); // placeholder, will be filled later
                        grpObj.put("gog", thisMap);
                        grpObj.put("genesis", grpGene);
                        gene.put(grp, grpObj);
                        grpObj = null;
                    }
                    gogDone = true;
                }

                if (mCntr >= grps.length) {
                    thisMap = "";
                } else {
                    thisMap = grps[mCntr];
                    mCntr++;
                }
            }

            if (thisMap.endsWith("grp")) {
                uCommons.GetMap(maps+thisMap);
                String[] gMaps = APImap.APIget("maps").split(",");
                JSONObject grpObj = gene.optJSONObject(thisMap);

                // Safe guard ! ---------------------------------
                if (grpObj == null) {
                    grpObj = new JSONObject();
                    grpObj.put("genesis", genesis);
                    grpObj.put("maps", "");
                    grpObj.put("gog", "");
                }

                grpObj.put("maps", new JSONArray(Arrays.asList(gMaps)));
                gene.put(thisMap, grpObj);
                grpObj = null;

                // Link each map to its parent grp
                for (String map : gMaps) {
                    if (map.equals("")) continue;
                    gene.put(map, thisMap);
                }
            }

            if (thisMap.equals("")) geneDone = true;
            if (thisMap.equals(NamedCommon.xMap)) geneDone = true;
        }

        ChangeMessage( "Genesis",  genesis);
        uCommons.WriteDiskRecord(conf+geneKey, gene.toString());
        if (!NamedCommon.xMap.equals(Holdmap)) {
            uCommons.uSendMessage("   >> reset map to " + Holdmap );
            NamedCommon.xMap = Holdmap;
        }

        uCommons.GetMap(maps+NamedCommon.xMap);
        gene = null;
        uCommons.uSendMessage("> SetupGenesis - Finished.");
    }

    public static void ManageGenesis() {

        // At this point, ONLY the map is known.
        // remove the map and check its grp,
        // get gog items, using the grp,
        // remove the grp from the gog
        // if the gog is empty, remove it - we're all done!

        String genesis = APImsg.APIget("genesis");      // returns the process registry (json)
        if (genesis.isEmpty()) return;
        String geneKey="Genesis-"+genesis;
        String geneRec = uCommons.ReadDiskRecord(conf+geneKey, true);
        if (NamedCommon.ZERROR) {
            // the file has already been handled.
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage="";
        }
        if (geneRec.isEmpty()) return;

        if (!NamedCommon.tConnected) {
            SqlCommands.ConnectSQL();
            if (NamedCommon.ZERROR) SqlCommands.ReconnectService();
        }

        Genesis=genesis;
        JSONObject genereg = new JSONObject(geneRec);
        String mGrp = genereg.getString(NamedCommon.xMap);

        // ----------------------------------------------------------------------------------------------
        // remove this map - IF it exists
        if (!mGrp.isEmpty()) {
            genereg.remove(NamedCommon.xMap);
            uCommons.uSendMessage("[GENESIS] "+NamedCommon.xMap+"  removed from register");
        } else {
            uCommons.uSendMessage("[GENESIS] could not find "+NamedCommon.xMap+" ----- skipping it.");
            return;
        }

        // ----------------------------------------------------------------------------------------------
        // Get the group that owns the map then remove the map from it
        JSONObject grpObj = genereg.getJSONObject(mGrp);        // returns the group element
        JSONArray grpMaps = grpObj.getJSONArray("maps");   // returns the maps in the group

        // Get these values BEFORE removing them - otherwise throws null exception
        String mGog = grpObj.getString("gog");             // returns the gog it belongs to
        JSONArray gogGrps = new JSONArray();
        if(!mGog.isEmpty()) {
            gogGrps = genereg.getJSONArray(mGog);
        }

        // check it is NOT empty before trying to remove things
        if (grpMaps != null && grpMaps.length() > 0) {
            for (int m=0 ; m<grpMaps.length() ; m++) {
                if (grpMaps.getString(m).equals(NamedCommon.xMap)) {
                    grpMaps.remove(m);
                    uCommons.uSendMessage("[GENESIS] "+NamedCommon.xMap+"  removed from "+mGrp);
                    break;
                }
            }

            // if the group has no more maps, remove it
            if (grpMaps.length() == 0) {
                uCommons.uSendMessage("[GENESIS] group has finished, removing it.");
                grpObj = new JSONObject();
                genereg.put(mGrp, grpObj);
                // it will be checked for being empty below
            } else {
                uCommons.uSendMessage("[GENESIS] group has more maps to process, updating it.");
                grpObj.put("maps", grpMaps);
                genereg.put(mGrp, grpObj);
            }
        }

        // ----------------------------------------------------------------------------------------------
        // if all maps in group are now finished :-
        // a) Get the gog that holds the group (if any)
        // b) remove the grp from the gog's list of groups

        if (grpMaps == null || grpMaps.length() == 0) {
            // remove the grp from the gog then remove the grp object
            if (genereg.has(mGrp) && genereg.getJSONObject(mGrp).length() == 0) {
                genereg.remove(mGrp);
            }
            uCommons.uSendMessage("[GENESIS] removed "+mGrp+ " from register.");
        }

        // IF the group is empty, remove it from the gog
        if (grpObj == null || grpObj.length() == 0) {
            if (gogGrps != null && gogGrps.length() > 0) {
                for (int g = 0; g < gogGrps.length(); g++) {
                    if (gogGrps.getString(g).equals(mGrp)) {
                        gogGrps.remove(g);
                        uCommons.uSendMessage("[GENESIS] removed "+mGrp+" from gog.");
                        break;
                    }
                }

                if (gogGrps == null || gogGrps.length() == 0) {
                    genereg.remove(mGog);
                    // the removal will be logged below.
                } else {
                    genereg.put(mGog, gogGrps);
                    uCommons.uSendMessage("[GENESIS] updated "+mGog);
                }
            }
        }

        // if all grps in the gog have finished, remove the gog (if exists) and send marker
        if (gogGrps == null || gogGrps.length() == 0) {
            if (!mGog.isEmpty()) {
                genereg.remove(mGog);
                uCommons.uSendMessage("[GENESIS] removed " + mGog);
            }

            // Send Marker
            uCommons.uSendMessage("[GENESIS] Marking " + genesis + " as finished");
            APImsg.APIset("marker", genesis + "-FINISHED");
            MarkerMessage();
            APImsg.APIdel("marker");
//            genereg = null;
//            uCommons.DeleteFile(conf + geneKey);
//            return;
        }

        if (genereg == null || genereg.keySet().isEmpty()) {
            uCommons.DeleteFile(conf + geneKey);
        } else {
            uCommons.WriteDiskRecord(conf+geneKey, genereg.toString());
        }
    }

    private static String Substitutions(String inStr) {
        while (inStr.contains("$task")) {
            inStr = inStr.replace("$task", inTask);
        }
        while (inStr.contains("$map")) {
            inStr = inStr.replace("$map", NamedCommon.xMap);
        }
        while (inStr.contains("$que")) {
            inStr = inStr.replace("$que", NamedCommon.inputQueue);
        }
        return inStr;
    }

    private static ArrayList<String> BuildCMDArray(String[] commands) {
        String cmd="", cmdFile="";
        ArrayList<String> DDL = new ArrayList<>();
        for (int ps = 0; ps < commands.length; ps++) {
            cmd = commands[ps];
            if (cmd.endsWith(".sql")) {
                cmdFile = maps + cmd;
                cmd = uCommons.ReadDiskRecord(cmdFile);
                if (NamedCommon.ZERROR) {
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    uCommons.uSendMessage("Skipped SQL command "+cmd+" ... not found.");
                    NamedCommon.ZERROR = false;
                    NamedCommon.Zmessage = "";
                } else {
                    String[] sqlCmds = cmd.split("\\r?\\n");
                    for (int pps=0; pps < sqlCmds.length; pps++) {
                        if (!sqlCmds[pps].trim().equals("")) DDL.add(sqlCmds[pps]);
                    }
                }
            } else {
                if (!cmd.trim().equals("")) DDL.add(cmd);
            }
        }
        return DDL;
    }

    private static void HandleMap() {
        NamedCommon.burstCnt = Send2NextProcess(nextBrk, NamedCommon.burstCnt, nextCor + NamedCommon.xMap);
        messageText = messageHold;
        responseText = "<<PASS>> ";
    }

    private static void ExecuteFetch() {
        if (NamedCommon.u2Source.equals("@DUMMY")) {
            uCommons.uSendMessage("Processing " + NamedCommon.xMap + "   >>  @DUMMY map");
            uCommons.uSendMessage("            this is no longer supported");
            return;
        }

        boolean proceed = false;
        if (!NamedCommon.sConnected) {
            if (SourceDB.ConnectSourceDB().contains("<<PASS>>")) {
                NamedCommon.IsAvailableU2 = true;
                proceed = (u2Commons.CheckU2Controls().contains("<<PASS>>"));
            }
        } else {
            proceed = true;
        }
        if (proceed) {
            responseText = "RESTART";
            while (responseText.toUpperCase().equals("RESTART")) {
                PreExecCommands();
                String rMsg = "        RunType: ***** " + NamedCommon.RunType + " *****";
                uCommons.uSendMessage((NamedCommon.block+NamedCommon.block).substring(0,rMsg.length()));
                uCommons.uSendMessage(rMsg);
                uCommons.uSendMessage((NamedCommon.block+NamedCommon.block).substring(0,rMsg.length()));
                if (SqlCommands.ConnectSQL()) {
                    if (!NamedCommon.datOnly) uCommons.uSendMessage("TargetDB: Connected");
                } else {
                    uCommons.uSendMessage("********************");
                    uCommons.uSendMessage("TargetDB unavailable");
                    uCommons.uSendMessage("********************");
                    responseText = "<<FAIL>> TargetDB unavailable";
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = responseText;
                    return;
                }
                uCommons.eMessage = "   .) Fetch data from " + NamedCommon.databaseType
                        + " file " + NamedCommon.u2Source + " in the "
                        + NamedCommon.datAct + " account";
                uCommons.uSendMessage(uCommons.eMessage);
                boolean showOutput = true;
                NamedCommon.SqlSchema = NamedCommon.rawSC;
                String takeFile = NamedCommon.u2Source;

                boolean fOpen = false;
                if (NamedCommon.debugging) uCommons.uSendMessage("Check-Point: Does "+takeFile+" exist in account: "+NamedCommon.datAct+" ???");
                switch (NamedCommon.protocol) {
                    case "u2cs":
                        fOpen = u2Commons.uOpenFile(takeFile, "1");     // adds the data account when creating q-file.
                        break;
                    case "real":
                        String qFile= "upl_"+takeFile+"_"+NamedCommon.datAct+"_"+NamedCommon.pid;
                        String junk = "{WRI}{file=MD}{item=" + qFile + "}{data=Q[[fm]]+" + NamedCommon.datAct + "[[fm]]" + takeFile + "}";
                        junk = u2Commons.MetaBasic(junk);
                        if (!NamedCommon.ZERROR) {
                            junk = "{RDI}{file="+ qFile + "}{item=DOESNOTEXIST}";
                            junk = u2Commons.MetaBasic(junk);
                            if (!junk.equals("")) {
                                NamedCommon.ZERROR = true;
                                NamedCommon.Zmessage = junk;
                                return;
                            }
                        }
                        fOpen = !NamedCommon.ZERROR;
                        break;
                    case "u2mnt":
                        fOpen = (!u2Commons.ReadAnItem("VOC", takeFile, "", "", "").equals(""));
                        break;
                    case "u2sockets":
                        fOpen = (!u2Commons.ReadAnItem("VOC", takeFile, "", "", "").equals(""));
                        break;
                    case "rmi.u2cs":
                        System.out.println("rmi:  is being developed - not ready yet");
                    default:
                        fOpen = u2Commons.uOpenFile(takeFile, "1");
                }

                if (!fOpen) {
                    responseText = "<<FAIL>> Cannot access " + takeFile;
                    uCommons.uSendMessage(responseText);
                    if (NamedCommon.Proceed) {
                        nextCtr = Send2NextProcess(nextBrk, 999, nextCor + NamedCommon.xMap);
                    }
                } else {
                    boolean dict = (NamedCommon.u2Source.startsWith("DICT"));
                    String pid = "_" + NamedCommon.pid;
                    String tmpDacct = uCommons.APIGetter("dacct");
                    if (tmpDacct.equals("")) tmpDacct = NamedCommon.datAct;
                    //
                    // This is for raw data so ALWAYS use u2File _ dacct ... WHY !!!
                    //
                    if (uCommons.APIGetter("sqltable").equals("")) {
                        NamedCommon.sqlTarget = u2Commons.GetExtractFile(NamedCommon.U2File.getFileName()); // takeFile;
                    } else {
                        NamedCommon.sqlTarget = uCommons.APIGetter("sqltable");
                        if (!tmpDacct.equals("")) NamedCommon.sqlTarget += "_" + tmpDacct;
                    }
                    NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\.", "_");
                    NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\,", "_");
                    NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\ ", "_");
                    NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll(pid, "");
                    if (dict && !NamedCommon.sqlTarget.startsWith("DICT_")) NamedCommon.sqlTarget = "DICT_" + NamedCommon.sqlTarget;
                    if (NamedCommon.debugging) uCommons.uSendMessage("Check-Point: PrepareSQLDumper for "+NamedCommon.sqlTarget);

                    if (!NamedCommon.datOnly) ExtractManager.PrepareSQLDumper();

                    switch (NamedCommon.protocol) {
                        case "real":
                            takeFile = "upl_"+takeFile+"_"+NamedCommon.datAct+"_"+NamedCommon.pid;
                            break;
                        default:
                            takeFile = NamedCommon.U2File.getFileName();    // reset for UV processes
                            break;
                    }

                    String sendFile = takeFile;
                    if (dict) sendFile = "DICT " + takeFile;
                    boolean first=true;

                    responseText = ExtractManager.FetchSourceData(sendFile);
                    System.out.println(" ");
                    if (responseText.toUpperCase().equals("RESTART")) {
                        uCommons.uSendMessage("*************************************************************************************************");
                        uCommons.uSendMessage("*****                               RESTARTING                                              *****");
                        uCommons.uSendMessage("*************************************************************************************************");
                        if (NamedCommon.ZERROR) {
                            NamedCommon.ZERROR = false;
                            NamedCommon.Zmessage = "";
                        }
                        if (NamedCommon.sConnected) {
                            uCommons.uSendMessage("Disconnect Source DB");
                            SourceDB.DisconnectSourceDB();
                        }
                        SourceDB.ConnectSourceDB();
                        if (NamedCommon.tConnected) {
                            uCommons.uSendMessage("Disconnect Target DB");
                            SqlCommands.DisconnectSQL();
                        }
                        SqlCommands.ConnectSQL();
                    }
                }

                /* ---------------------------------------------------------------- */
                if (!ExtractManager.correl.equals("")) {
                    uCommons.uSendMessage("Clean-up list items for " + ExtractManager.correl);
                    u2Commons.CleanupLists(ExtractManager.correl);
                }
            }

            if (NamedCommon.Proceed) {
                if (!NamedCommon.ZERROR) {
                    if (NamedCommon.RunType.equals("INCR") || (APImap.APIget("loop").toLowerCase().equals("true"))) {
                        ChangeMessage("INCR_LOADDTE", NamedCommon.BatchID);
                    }
                    if (NamedCommon.BulkLoad) {
                        uCommons.uSendMessage(NamedCommon.BurstWait + " Second pause for MoveRaw() loading");
                        uCommons.Sleep(NamedCommon.BurstWait);
                    }
                    nextCtr = NamedCommon.burstCnt;
                    NamedCommon.burstCnt = Send2NextProcess(nextBrk, NamedCommon.burstCnt, nextCor + NamedCommon.xMap);
                } else {
                    responseText = "<<FAIL>> " + NamedCommon.Zmessage;
                    nextCtr = Send2NextProcess(nextBrk, 999, nextCor + NamedCommon.xMap);
                    messageText = messageHold;
                }
            } else {
                uCommons.uSendMessage("proceed=false for this message.");
            }
            responseText = "<<PASS>>";
        } else {
            responseText = NamedCommon.Zmessage;
            uCommons.uSendMessage(responseText);
        }
    }

    private static void ExecuteBurst() {
        if (ConnectionPool.jdbcPool.size() == 0) SqlCommands.ConnectSQL();
        if (ConnectionPool.jdbcPool.size() == 0) {
            NamedCommon.ZERROR = true;
            uCommons.uSendMessage(NamedCommon.block);
            uCommons.uSendMessage("There is no TargetDB connection !!");
            uCommons.uSendMessage(NamedCommon.block);
            return;
        }

        boolean proceed = true;
        if (NamedCommon.SqlDatabase.equals("")) NamedCommon.SqlDatabase = NamedCommon.rawDB;

        if (proceed) {
            responseText = "";
            if (!NamedCommon.u2Source.equals("@DUMMY")) {

                // ## Gitlab 920    014 to have zero touch on source db
                String takeFile = uCommons.APIGetter("sqltable");
                if (takeFile.equals("")) takeFile = NamedCommon.u2Source;

                if (!NamedCommon.ZERROR) {

                    String tmpDacct = NamedCommon.datAct;
                    // ---------- source file exists -------------------------------------------------------------------
                    // ---------- now use the target file + dacct because that's where Fetch put the data !! -----------
                    // -------------------------------------------------------------------------------------------------

                    if (!APImsg.APIget("dacct").equals("")) tmpDacct = APImsg.APIget("dacct");
                    // ## Gitlab 920
                    // ------------------------------------------------------------------------------------
                    // takeFile                 : which table to select raw data from
                    // NamedCommon.sqlTarget    : which table to burst it into
                    // ------------------------------------------------------------------------------------
                    if (uCommons.APIGetter("sqltable").equals("")) {
                        NamedCommon.sqlTarget = u2Commons.GetExtractFile(NamedCommon.U2File.getFileName()); // takeFile;
                    } else {
                        NamedCommon.sqlTarget = uCommons.APIGetter("sqltable");
                    }
                    NamedCommon.sqlTarget = NamedCommon.sqlTarget.replace(".", "_");

                    if (!takeFile.endsWith(tmpDacct)) takeFile += "_" + tmpDacct;
                    String tmpSource = NamedCommon.u2Source;
                    tmpSource = tmpSource.replaceAll("\\.", "_");
                    tmpSource = tmpSource.replaceAll("\\,", "_");
                    tmpSource = tmpSource.replaceAll("\\ ", "_");
                    if (!tmpSource.equals(NamedCommon.sqlTarget)) {
                        uCommons.uSendMessage("   .) Fetch took data from " + NamedCommon.u2Source + " to create raw." + NamedCommon.sqlTarget);
                    }

                    takeFile = takeFile.replaceAll("\\.", "_");
                    takeFile = takeFile.replaceAll("\\,", "_");
                    takeFile = takeFile.replaceAll("\\ ", "_");

                    String psch = NamedCommon.SqlSchema;
                    if (NamedCommon.uniBase) psch = "uni";
                    // ## Gitlab 921
                    if (!NamedCommon.datOnly) {
                        uCommons.eMessage = "   .) Bursting data from " +
                                "[" + NamedCommon.rawDB + "].[raw].[" + takeFile + "] into " +
                                "[" + NamedCommon.SqlDatabase + "].[" + psch + "]." +
                                "[" + NamedCommon.sqlTarget + "]";
                        APImsg.APIset("sqlTable", NamedCommon.sqlTarget);

                        uCommons.uSendMessage(uCommons.eMessage);
                    }
                    NamedCommon.BurstRestarted = false;     // when BurstProcess sends it back to 014_Burst_000
                    responseText = BurstSplit.ProcessMap(messageHold);
                    while (responseText.toUpperCase().equals("RESTART")) {
                        if (NamedCommon.ZERROR) { NamedCommon.ZERROR = false; NamedCommon.Zmessage = ""; }
                        uCommons.Sleep(NamedCommon.BurstWait);  // SQL could be very busy
                        responseText = BurstSplit.ProcessMap(messageHold);
                    }
                } else {
                    responseText = "<<FAIL>> "+NamedCommon.Zmessage;
                }

                if (!responseText.contains("<<FAIL>>") && !NamedCommon.BurstRestarted) {
                    if (!NamedCommon.datOnly && NamedCommon.uniBase) CreateViews();
                    if (!uCommons.APIGetter("postsql").equals("")) PostSQL();
                }

            } else {
                uCommons.uSendMessage("@DUMMY map");
                responseText = "<<PASS>>";
                String junk = NamedCommon.mTrigger;
                // maps link to grps but dummy map is the last link in a grps to maps
            }

        } else {
            responseText = "<<FAIL>> rFuel STOP switch is set on";
        }
        if (responseText.contains("<<PASS>>") && !NamedCommon.BurstRestarted) {
            if (NamedCommon.Proceed) {
                NamedCommon.burstCnt = Send2NextProcess(nextBrk, NamedCommon.burstCnt, nextCor + NamedCommon.xMap);
            }
        }
    }

    private static void FlipLoaded() {
        String fqn = "";
        nextTask = "010";
        if (!NamedCommon.ZERROR) {
            if (SourceDB.ConnectSourceDB().contains("<<PASS>>")) {
                UplCtl = new ArrayList<>();
                for (int u = 0; u <= 15; u++) {UplCtl.add("");}
                UplCtl.set(0, NamedCommon.BatchID);
                UplCtl.set(1, NamedCommon.RunType);
                UplCtl.set(2, NamedCommon.serial);
                UplCtl.set(3, NamedCommon.xMap);
                UplCtl.set(4, "017-Flip");
                UplCtl.set(5, "");
                UplCtl.set(6, "");
                UplCtl.set(7, "");
                UplCtl.set(8, "0");
                UplCtl.set(9, "0");
                UplCtl.set(10, "0");
                UplCtl.set(11, "0");
                UplCtl.set(12, "0");
                UplCtl.set(13, "0");
                UplCtl.set(14, "0");
                UpdateUPLCTL();
                u2Commons.uFlipRaw();
                responseText = "<<PASS>> " + thisCor + NamedCommon.xMap;
            } else {
                uCommons.uSendMessage("Source DB unavailable");
                responseText = "<<FAIL>> " + thisCor + NamedCommon.xMap
                        + "  " + NamedCommon.Zmessage;
            }
            if (!NamedCommon.postuni.equals("")) PostUni();
        } else {
            responseText = "<<FAIL>> see RunERRORS queue";
        }
    }

    private static void Send2Finish() {
        if (APImsg.APIget("TASK").startsWith("99")) return;
        if (NamedCommon.StopNow.contains("<<FAIL>>")) {
            responseText = "<<FAIL>> " + NamedCommon.Zmessage + " ... see logs for "
                    + NamedCommon.xMap + " in " + thisQue + " on queue " + queNbr;
//            responseText = messageText;
            nextCor = "FAIL>>_" + nextCor;
            nextCtr = 0;
        } else {
            nextCtr = -1;
            if (!thisTask.equals(nextTask)) {
                long finishP = System.nanoTime();
                laps = (finishP - NamedCommon.startM) / div;
                responseText = "<<PASS>> " + thisCor + NamedCommon.xMap + " processed  in "
                        + df.format(laps) + " seconds";

//                if (thisCor.contains(NamedCommon.xMap)) {
                    thisCor = "Finished>>_" + thisCor;
//                } else {
//                    thisCor = "Finished>>_" + thisCor + NamedCommon.xMap;
//                }
            }
        }
        nextCtr = Send2NextProcess(nextBrk, nextCtr, thisCor + NamedCommon.xMap);
        messageText = messageHold;
    }

    public static int Send2NextProcess(String brk, int nextCnt, String cor) {
        String send2Que, tmp1;
        long holdExpiry = NamedCommon.Expiry;

        if (nextCnt == 999) {
            send2Que = "RunERRORS";
            NamedCommon.Expiry = 0;         // NO expiry time on RunError  messages !!!
            Hop.start(messageText, "", nextBrk, send2Que, "", cor);
            NamedCommon.Expiry = holdExpiry;
            return nextCnt;
        }

        if (cor.contains("Finished>>")) {
            if (messageText.equals("") || messageText.equals(messageHold)) responseText = "<<PASS>> " + NamedCommon.xMap;
            send2Que = NamedCommon.reply2Q;
            Hop.start(messageText, "", nextBrk, send2Que, "", cor);
            return nextCnt;
        }

        NamedCommon.Expiry = 0;             // NO expiry time on active messages !!!
        ChangeMessage("task=", nextTask.trim());
        String processIdentifier = NamedCommon.xMap;
        if (processIdentifier.startsWith("shell")) processIdentifier = NamedCommon.u2Source;
        cor = SetCorrel(nextTask) + processIdentifier;

        if (NamedCommon.isKafka) {
            // Messages arrive too fast for qBalancer - go direct !!
            if (nextCnt > nextMax) nextCnt = 1;
            String qNbr = uCommons.RightHash("000" + nextCnt, 3);
            send2Que = nextQue + "_" + qNbr;
            qNbr="";
        } else {
            if (nextQue.startsWith("010")) {
                send2Que = nextQue + "_001";
            } else {
                send2Que = nextQue + "_000";
            }
        }
        Hop.start(messageText, "", nextBrk, send2Que, "", cor);
        NamedCommon.Expiry = holdExpiry;
        holdExpiry = 0;
        nextCnt++;
        if (nextCnt > queMax) nextCnt = 1;
        return nextCnt;
    }

    private static boolean CheckMsgTask() {
        String  mPart = APImsg.APIget("task");
        if (!mPart.equals(inTask) && !mPart.startsWith("9")) {
            uCommons.uSendMessage("Task issue- inTask is [" + inTask + "] but this task is [" + mPart + "]");
            uCommons.uSendMessage(messageText);
            return false;
        }
        mPart = APImsg.APIget("replyto");
        if (mPart.equals("") && NamedCommon.replyReqd) {
            uCommons.uSendMessage("ReplyTo parameter not provided.");
            uCommons.uSendMessage(messageText);
            return false;
        }
        mPart = APImsg.APIget("correlationid");
        if (mPart.equals("")) {
            uCommons.uSendMessage("CorrelationID parameter not provided.");
            uCommons.uSendMessage(messageText);
            return false;
        }
        return true;
    }

    public static void ChangeMessage(String tag, String value) {
        tag = tag.replaceAll("\\=", "");
        APImsg.APIset(tag, value);
        messageText = APImsg.BuildMessage();
    }

    private static void Send2Kafka() {
        // Input:   json payload with a raw UV record within it
        // Process: Either process as a;
        //          a) 014 Build an array of SQL-like rows, or
        //          b) 050 Build a "normalised" payload of the record (* recommended)
        // Output:  send the rows / json to a topic in an event stream
        //
        // Purpose: Create a JSON representation of the record
        // -----------------------------------------------------------------------------

        String holdTask = nextTask;
        uCommons.uSendMessage("   Kafka properties from: " + NamedCommon.kafkaBase);
        // Get details from APImsg
        // -----------------------------------------------------------------------------
        String kafTopic = "uni-", kafKey = APImsg.APIget("item");
        if (NamedCommon.multihost) {
            kafTopic = NamedCommon.uplSite + "-";
            kafTopic+= NamedCommon.dbhost + "-";
            kafTopic+= APImsg.APIget("dacct") + "-";
        }
        // -----------------------------------------------------------------------------
        // push responseText to a topic in a KStream (event stream)
        switch (NamedCommon.kafkaAction) {
            case "014":
                break;
            case "050":
                // 1.   Create a json payload of the record as it is mapped.
                // 2.   Send to topic {site}-{host}-{dacct}-{file}-{version}-{item}
                //              topic {site}-{host}-{dacct}-{file}-{version}
                //              key=item, value=json
                //      e.g. ACCOUNT.v1
                //      NB: {site} , {host} and {dacct} should be optional - multiHost
                NamedCommon.isRest = true;
                // uRestLoader creates responseText
                uRestLoader();              //  does it really need to go through DataConverter?
                NamedCommon.isRest = false;
                if (NamedCommon.uniBase) {
                    NamedCommon.SqlSchema = "uni";
                } else {
                    NamedCommon.SqlSchema = APImsg.APIget("schema");
                }
                kafTopic+= NamedCommon.u2Source + "-" + NamedCommon.topicExtn;
                uCommons.uSendMessage(" .> Send  [" + kafKey + "]  to topic [" + kafTopic + "]  in k-batch.");
                //  --------------------------------------------------------------------------------------------------
                //  IF responseText is an array, should rFuel send each item in the array independantly?
                //  --------------------------------------------------------------------------------------------------
                boolean okay = kProducer.kBatchCollector(kafTopic, kafKey, responseText);
                if (!okay) NamedCommon.ZERROR = true;

                //  # Andy
                //  Can create the ksql table OR can send to topic and use kafka to stream to a table (preferred)
                // String createTable = ksqlDB.CreateTable("", NamedCommon.SqlSchema, NamedCommon.u2Source, NamedCommon.tblCols.split(","));
                break;
        }

        if (NamedCommon.Proceed) {
            if (nextCtr <=0) nextCtr=1;
            nextTask = holdTask;
            nextCtr = Send2NextProcess(nextBrk, nextCtr, nextCor + NamedCommon.xMap);
        }
    }

    private static void uRestLoader() {
        BurstSplit.SetDumpFlag(false);
        nextTask = "050";
        NamedCommon.fmvArrayIsSet = false;
        NamedCommon.fmvArray = null;
        String fqn = maps + NamedCommon.xMap;
        // ------------------------------------------------
        uCommons.GetMap(fqn);       //  Reads the map and builds the mapOrder list
        if (NamedCommon.ZERROR) {
            responseText = NamedCommon.Zmessage;
            if (!responseText.contains("<<FAIL>>")) responseText = "<<FAIL>> "+responseText;
            return;
        }
        // ------------------------------------------------
        uCommons.uSendMessage("       item: "+uCommons.APIGetter("item"));
        uCommons.uSendMessage("        map: "+uCommons.APIGetter("map"));
        uCommons.uSendMessage("   dsd list: "+uCommons.APIGetter("list"));
        uCommons.uSendMessage("   template: "+uCommons.APIGetter("template"));
        uCommons.uSendMessage("      class: "+uCommons.APIGetter("class"));
        uCommons.uSendMessage("     domain: "+uCommons.APIGetter("domain"));
        uCommons.uSendMessage("           : --------------------");

        NamedCommon.DataList.clear();
        NamedCommon.DataLineage.clear();
        NamedCommon.SubsList.clear();
        NamedCommon.TmplList.clear();
        NamedCommon.Templates.clear();
        NamedCommon.AsocList.clear();

        responseText = "";

        // special handling for CDR Open Banking

        if (uCommons.APIGetter("class").toLowerCase().equals("cdrob")) {
            String type = uCommons.APIGetter("domain").toLowerCase();
            switch (type) {
                case "customerid":
                    temp = OBMethods.GetCustomerID();
                    break;
                case "customer":
                    temp = OBMethods.GetCustomer();
                    break;
                case "account":
                    temp = OBMethods.GetAccounts();
                    break;
                case "transactions":
                    temp = OBMethods.GetTransactions();
                    break;
                case "transactionsv2":
                    temp = OBMethods.GetTransactionsV2();
                    break;
                case "payees":
                    temp = OBMethods.GetPayees();
                    break;
                case "payments":
                    temp = OBMethods.GetPayments();
                    break;
                case "jointaccounts":
                    temp = OBMethods.GetJointAccounts();
                    break;
                case "":
                    break;
            }
            if (!temp.contains(fail) && !NamedCommon.ZERROR) {
                responseText = NamedCommon.DataList.get(0);
                if (!APImsg.APIget("format").equals("")) {
                    responseText = uConnector.Format(responseText, APImsg.APIget("format"));
                }
            } else {
                String err = "422";
                String descr = NamedCommon.ReturnCodes.get(Integer.valueOf(err));
                if (descr.equals("")) descr = "ReturnCode ["+err+"] not found.";
                descr += ". Refer to logs.";

                responseText = DataConverter.ResponseHandler("422", descr, responseText, APImsg.APIget("format"));
                NamedCommon.uStatus = "422";
            }
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage="";
            uCommons.uSendMessage("--------------------");
            return;
        }

        // Data Blender: Blend on order of appearance in xMap -------------------------------------

        for (int ord = 0 ; ord < NamedCommon.mapOrder.size(); ord++) {
            if (NamedCommon.mapOrder.get(ord).toLowerCase().equals("pick")) { uRestLoader_Pick(fqn)     ; continue; }
            if (NamedCommon.mapOrder.get(ord).toLowerCase().equals("sql"))  { uRestLoader_Jdbc()        ; continue; }
            if (NamedCommon.mapOrder.get(ord).toLowerCase().equals("api"))  { uRestLoader_WebService()  ; }
        }

        // -----------------------------------------------------------------------------------------------------------------------------

        if (!NamedCommon.ZERROR) {
            if (NamedCommon.Template.trim().equals("")) {
                String csvVals = "";
                int nbrVals = NamedCommon.DataList.size();
                for (int xx = 0; xx < nbrVals; xx++) {
                    csvVals = csvVals + NamedCommon.DataList.get(xx) + "; ";
                }
                uCommons.uSendMessage("*** No template has been provided in the map or csv.");
                uCommons.uSendMessage("*** Will use basic xml.");
                responseText = DataConverter.HandleCSVtoXML(csvVals);
            } else {
                if (!responseText.startsWith("<<FAIL>>")) {
                    responseText = DataConverter.Replacements(NamedCommon.Template.trim());
                }
            }
        } else {
            responseText = NamedCommon.Zmessage;
        }

        // -----------------------------------------------------------------------------------------------------------------------------

        if (NamedCommon.debugging) uCommons.uSendMessage("Default wrapping on task(s) " + NamedCommon.StructuredResponse);
        String WrapTaskHolder = NamedCommon.StructuredResponse;

        if (APImsg.APIget("wraptask").toLowerCase().equals("true")) {
            if (NamedCommon.debugging) uCommons.uSendMessage("This message turned wrapping ON");
            if (!NamedCommon.StructuredResponse.contains(inTask)) NamedCommon.StructuredResponse += ","+inTask+",";
        }

        if (APImsg.APIget("wraptask").toLowerCase().equals("false")) {
            int tPosx = NamedCommon.StructuredResponse.indexOf(inTask);
            if (tPosx > -1) {
                String oldStruct = NamedCommon.StructuredResponse;
                String b4Task    = "";
                String afterTask = "";
                if (tPosx > 0) b4Task = oldStruct.substring(0, tPosx - 1);
                afterTask = oldStruct.substring(tPosx + inTask.length(), oldStruct.length());
                uCommons.uSendMessage("*** Stripped "+inTask+" from "+NamedCommon.StructuredResponse);
                NamedCommon.StructuredResponse = b4Task+afterTask;
                uCommons.uSendMessage("Default wrapping on task(s) "+NamedCommon.StructuredResponse);
            }
        }

        // -----------------------------------------------------------------------------------------------------------------------------

        String mStatus="", descr="";
        if (responseText.contains("<<FAIL>>") || NamedCommon.ZERROR) {
            if (uCommons.APIGetter("class").toLowerCase().equals("cdrob")) responseText = "Bad Parameter";
            mStatus = "422";
            NamedCommon.uStatus = mStatus;
            responseText = responseText.replaceAll("\\<", "");
            responseText = responseText.replaceAll("\\>", "");
            descr = NamedCommon.ReturnCodes.get(Integer.valueOf(mStatus));
            if (descr.equals("")) descr = "ReturnCode [" + mStatus + "] not found.";
        } else {
            mStatus = "200";
            NamedCommon.uStatus = mStatus;
            descr = NamedCommon.ReturnCodes.get(Integer.valueOf(mStatus));
            if (descr.equals("")) descr = "ReturnCode [" + mStatus + "] not found.";
            boolean intervene = (NamedCommon.StructuredResponse.contains(inTask));
            intervene = (intervene && ( !responseText.startsWith("<") || !responseText.startsWith("{")));
//            intervene = (intervene && !responseText.startsWith("{"));
            if (intervene) {
                descr = NamedCommon.ReturnCodes.get(Integer.valueOf(mStatus));
                if (descr.equals("")) descr = "ReturnCode [" + mStatus + "] not found.";
            } else {
                if (responseText.startsWith("<?xml")) {
                    String mBody = responseText.substring(responseText.indexOf("><") + 1, responseText.length());
                    responseText = NamedCommon.xmlProlog + mBody;
                }
            }
        }

        if (!descr.equals("")) {
            responseText = DataConverter.ResponseHandler(mStatus, descr, responseText, APImsg.APIget("FORMAT").toUpperCase());
        }

        if (responseText.startsWith("<?xml")) {
            String prolog = responseText.substring(0,responseText.indexOf("><")+1);
            String mBody  = responseText.substring(responseText.indexOf("><")+1, responseText.length());
            if (!prolog.equals(NamedCommon.xmlProlog)) {
                uCommons.uSendMessage("   .) WARNING - the xml prolog in the template is wrong.");
                uCommons.uSendMessage("          old - "+prolog);
                uCommons.uSendMessage("          new - "+NamedCommon.xmlProlog);
                responseText = NamedCommon.xmlProlog + mBody;
            }
        }

        BurstSplit.SetDumpFlag(true);
        NamedCommon.StructuredResponse = WrapTaskHolder;
    }

    public static void uRestLoader_Pick(String fqn) {
        if (!NamedCommon.isNRT) {
            uCommons.uSendMessage("**");
            uCommons.uSendMessage(uCommons.LeftHash("***** Acquiring " + NamedCommon.databaseType + " data " + NamedCommon.block, 50));
            uCommons.uSendMessage("**");
        } else {
            if (NamedCommon.isNRT) uCommons.uSendMessage("***** Near-Real-Time actions");
            if (NamedCommon.isKafka) uCommons.uSendMessage("***** Send to Kafka topic");
        }

        temp = ExtractManager.GetSourceData(fqn);

        if (!NamedCommon.ZERROR) {
            if (temp.contains("<<FAIL>>") || temp.contains("<status>422")) {
                uCommons.uSendMessage(temp);
                responseText = temp;
            } else {
                uCommons.uSendMessage(" ");
                if (NamedCommon.DataList.size() > 0) responseText = NamedCommon.DataList.get(0);
            }
        } else {
            uCommons.uSendMessage(temp);
            responseText = temp;
        }
    }

    public static void uRestLoader_Jdbc() {
        uCommons.uSendMessage("**");
        uCommons.uSendMessage(uCommons.LeftHash("***** Acquiring SQL data " + NamedCommon.block, 50));
        uCommons.uSendMessage("**");
        ExtractManager.rExtract_Sql();
    }

    public static void uRestLoader_WebService() {
        uCommons.uSendMessage("**");
        uCommons.uSendMessage(uCommons.LeftHash("***** Acquiring WebService data " + NamedCommon.block, 50));
        uCommons.uSendMessage("**");
        ExtractManager.rExtract_Api();
    }

    public static String GetMapOnly(String thisMap) {
        int nbrSlash = 0;
        thisMap = thisMap.replace("\\", "/");
        nbrSlash = thisMap.length() - thisMap.replace("/", "").length();
        thisMap = uCommons.FieldOf(thisMap, "/", (nbrSlash + 1));
        return thisMap;
    }

    public static void uStreamsInitialise(List<String> tmpList) {
        // tmpList [0] = account [1] = file  [2] = iid   [3] = record
        NamedCommon.datAct   = tmpList.get(0);
        NamedCommon.u2Source = tmpList.get(1);

        APImsg.APIset("dacct", tmpList.get(0));
        APImsg.APIset("file",  tmpList.get(1));
        APImsg.APIset("item",  tmpList.get(2));

        String uvRec = tmpList.get(3).replaceAll("[^a-zA-Z0-9!@#$%^&*()<>-_+=\\]\\[{}|\"',<.>/? `~]","");

        // ALL Decryption is done in com.unilibre.kafka.commons.GetJSONvalue
        // So now the message can fly through here with minimal effort
        //
//        if (NamedCommon.encRaw) {
//            String tmpStr = APImsg.APIget("passport");
//            tmpList = null;
//            tmpList = new ArrayList<>(Arrays.asList(tmpStr.split("\\~")));
//            // tmpList is reused !  [0] = future cipher key    [1] = encSeed
//            if (tmpList.size() > 1) {
//                uvRec = uCipher.v2UnScramble(uCipher.keyBoard25, uvRec, tmpList.get(1));
//            }
//        }

        APImsg.APIset("record",  uvRec);

        if (!NamedCommon.tConnected) SqlCommands.ConnectSQL();
        if (!NamedCommon.tConnected) { NamedCommon.ZERROR=true; NamedCommon.Zmessage="No SQL database definition!"; }

        DeltaTS = APImsg.APIget("loadts");
        if (!DeltaTS.equals("")) NamedCommon.BatchID = DeltaTS;
        tmpList.clear();
    }

    public static void uStreamsFetch () {

        String uvID = APImsg.APIget("item");
        uCommons.uSendMessage("uStreams: " + NamedCommon.datAct + " > " + NamedCommon.u2Source + " > " + uvID);
        uCommons.uSendMessage("   .) Load  raw Record");

        if (SqlCommands.ConnectSQL()) {
            if (NamedCommon.debugging) {
                uCommons.uSendMessage("TargetDB: Connected");
                uCommons.uSendMessage("***");
            }
        } else {
            uCommons.uSendMessage("********************");
            uCommons.uSendMessage("TargetDB unavailable");
            uCommons.uSendMessage("********************");
            responseText = "<<FAIL>> TargetDB unavailable";
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = responseText;
            return;
        }

        String holdCols = NamedCommon.rawCols;

        uCommons.GetMap(maps + NamedCommon.xMap);
        if (NamedCommon.ZERROR) return;

        if (NamedCommon.debugging) {
            uCommons.eMessage = "Stream record from file " + NamedCommon.u2Source + " in the "
                    + NamedCommon.datAct + " account";
            uCommons.uSendMessage(uCommons.eMessage);
            uCommons.uSendMessage("1. Update raw table with this Delta.");
        }

        if (APImsg.APIget("sqldb").equals("$DB$")) APImsg.APIset("sqldb", NamedCommon.rawDB);
        if (APImsg.APIget("schema").equals("$SC$")) APImsg.APIset("schema", NamedCommon.rawSC);

        String takeFile = NamedCommon.u2Source + "_" + NamedCommon.datAct;
        NamedCommon.sqlTarget = takeFile;
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\.", "_");
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\,", "_");
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\ ", "_");

        String holdDB = NamedCommon.SqlDatabase;
        String holdSC = NamedCommon.SqlSchema;
        NamedCommon.SqlDatabase = NamedCommon.rawDB;
        NamedCommon.SqlSchema   = NamedCommon.rawSC;
        ExtractManager.PrepareSQLDumper();

        NamedCommon.rawCols = NamedCommon.rawCols.replaceAll("\\*", "");
        NamedCommon.rawCols = NamedCommon.rawCols.replaceAll("\\-", "");
        NamedCommon.rawCols = NamedCommon.rawCols.replaceAll("\\.", "");
        NamedCommon.rawCols = NamedCommon.rawCols.replaceAll("\\@", "");

        NamedCommon.Proceed = true;
        ExtractManager.Ctr = 0;
        ExtractManager.DDL = new ArrayList<>();
        ExtractManager.dbFocus = "[" + NamedCommon.rawDB + "].[raw].[" + NamedCommon.sqlTarget + "]";
        ExtractManager.quote = "'";
        ExtractManager.Komma = ",";

        // Dealerships have control chars in EVERY field !!
        String uvRec = APImsg.APIget("record");
        if (NamedCommon.CleanseData) uvRec = u2Commons.Cleanse(uvRec);
        ExtractManager.LoadRow(uvID, uvRec);

        NamedCommon.rawCols = holdCols;
        NamedCommon.SqlDatabase = holdDB;
        NamedCommon.SqlSchema   = holdSC;
        uvRec = "";
    }

    public static void uStreamsBurst () {
        uCommons.uSendMessage("   .) Load  CDC events");
        if (NamedCommon.ZERROR) return;
        if (!NamedCommon.tConnected) {
            if (NamedCommon.debugging) uCommons.uSendMessage("<<FAIL>> TargetDB is not connected");
            return;
        }
        uCommons.GetMap(maps + NamedCommon.xMap);
        responseText = "";
        String psch = NamedCommon.SqlSchema;
        if (NamedCommon.uniBase) psch = "uni";
        NamedCommon.SqlSchema = psch;

        if (NamedCommon.debugging) {
            String takeFile = NamedCommon.u2Source + "_" + NamedCommon.datAct;
            if (NamedCommon.u2Source.startsWith("DICT") && !takeFile.startsWith("DICT")) takeFile = "DICT_"+takeFile;
            uCommons.eMessage = "2. Burst the raw Delta";
            uCommons.eMessage += " from [" + NamedCommon.rawDB + "].[raw].[" + takeFile + "]";
            uCommons.uSendMessage(uCommons.eMessage);
            uCommons.eMessage += "                           into [" + NamedCommon.SqlDatabase + "].[" + psch + "].";
            uCommons.eMessage += "[" + NamedCommon.sqlTarget + "]";
            uCommons.uSendMessage(uCommons.eMessage);
        }

        BurstSplit.quote = "'";
        BurstSplit.Komma = ",";
        NamedCommon.uID = new UniString(APImsg.APIget("item"));

        boolean okay = BurstSplit.ManageTask(NamedCommon.xMap);

        NamedCommon.MQgarbo.gc();
        psch = "";
    }
}
