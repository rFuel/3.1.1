package com.unilibre.core;

/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.Connection;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class uResponse {

    private static boolean transacted = false;
    private static boolean debug = true;
    private static String rQue = "";
    private static String nextBrk = "";
    private static String startline = "-- [ Start ] --------------------------------------------------";
    private static String underline = "---------------------------------------------------------------";
    private static String countdown = "<<heartbeat>> reconnect SourceDB in $$ seconds";
    private static ActiveMQConnectionFactory connectionFactory;
    private static ArrayList<String> badRecs = new ArrayList<>();
    private static String cmd = "SELECT uRESPONSES";
    private static String raw = "RAW";
    private static TextMessage inMessage;
    private static Connection connection;
    private static long lastAction = 0;
    private static long rightNow = 0;
    private static double laps = 0;
    private static long mStart = 0;
    private static long mFinish = 0;
    private static long freeMem = 0;
    private static long usedMem = 0;
    private static final long MEGABYTE = 1024L * 1024L;
    private static double mTime = 0;
    private static int maxWait = 45;
    private static double div = 1000000000.00;

    public static void main(String[] args) {
        debug = (System.getProperty("debug", "false").toLowerCase().equals("true"));
        NamedCommon.Broker = System.getProperty("bkr", "");
        Initialise();
        nextBrk = NamedCommon.Broker.substring(0, (NamedCommon.Broker.length() - 4));
        lastAction = System.nanoTime();
        uCommons.uSendMessage("Starting: uResponse("+NamedCommon.pid+") -------------------------------------");
        switch (NamedCommon.protocol) {
            case "u2cs":
                DoUxcs(args);
                break;
            case "real":
                DoReal(args);
                break;
        }
    }

    private static void Initialise() {
        NamedCommon.Reset();
        NamedCommon.isWhse = false;
        NamedCommon.isRest = true;
        NamedCommon.debugging = debug;
        String slash = "";
        if (NamedCommon.upl.equals("")) {
            NamedCommon.upl = System.getProperty("user.dir");
            NamedCommon.BaseCamp = NamedCommon.upl;
            if (NamedCommon.upl.contains("/")) slash = "/";
            if (NamedCommon.upl.contains("\\")) slash = "\\";
            NamedCommon.slash = slash;
        }
        slash = NamedCommon.slash;

        if (NamedCommon.Broker.equals("")) {
            // ------- [DEV / PROD housekeeping -------------------------------------------
            if (NamedCommon.upl.contains("/home/andy")) {
                String old = NamedCommon.BaseCamp;
                NamedCommon.BaseCamp = slash + slash + "rfuel22" + slash + "all" + slash + "upl";
                String knw = NamedCommon.BaseCamp;
                uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
                NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
            }
            // -----------------------------------------------------------------------------
        }

        if (NamedCommon.Broker.equals("")) {
            responder.uTask = "055";
            responder.GetBroker();
        }

        if (NamedCommon.Broker.equals("")) {
            responder.uTask = "050";
            responder.GetBroker();
        }

        if (NamedCommon.Broker.equals("")) {
            uCommons.uSendMessage("No uRest broker identified. Cannot proceed.");
            System.exit(0);
        }
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt); // MUST clear AES vars first.
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
    }

    private static void DoUxcs(String[] args) {
        // ----------------------------------------
        // If being restarted: ignore everything in uRESPONSES
        // ----------------------------------------
        if (NamedCommon.dbWait != maxWait) maxWait = NamedCommon.dbWait;
        int rHash = String.valueOf(maxWait).length() + 1;

        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        if (!NamedCommon.sConnected) System.exit(0);

        if (args.length > 0) rQue = args[0];

//        NamedCommon.hostname = "uRestResponder";
        if (NamedCommon.hostname.contains(" ")) NamedCommon.hostname = NamedCommon.hostname.replaceAll(" ", "-");
        uCommons.uSendMessage("Hostname is " + NamedCommon.hostname);
        NamedCommon.inputQueue = NamedCommon.dbhost;

        boolean stopme = false;
        String sID = "", reply2Q = "";
        String ping = "PING";
        badRecs.clear();
        UniString rID = null, rRec = null;
        UniDynArray dRec = null;

        if (!rQue.equals("")) cmd += " LIKE ...[" + rQue + "]";

        UniFile uResponses, BPUPL;
        UniCommand ucmd = null;
        try {
            ucmd = NamedCommon.uSession.command();
        } catch (UniSessionException e) {
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            uCommons.uSendMessage("Cannot set uCmd " + e.getMessage());
            System.exit(0);
        }

        ucmd.setCommand(cmd);
        BPUPL = u2Commons.uOpen("BP.UPL");
        uResponses = u2Commons.uOpen("uRESPONSES");
        try {
            uResponses.setLockStrategy(1);
        } catch (UniFileException e) {
            uCommons.uSendMessage("WARNING ------------------------------------------------------");
            uCommons.uSendMessage("Cannot setLockStrategy(1) for uRESPONSES : " + e.getMessage());
        }
        if (NamedCommon.ZERROR) System.exit(0);
        int rCnt = 0, hBeat = 1, nodata=-1;
        Hop.setShow(true);
        uCommons.uSendMessage("Looking for responses ...");
        String starting = startline, stopRec="";
        UniString uStop = new UniString("STOP");
        NamedCommon.MQgarbo.gc();
        GarbageCollector.setStart(System.nanoTime());
        freeMem = NamedCommon.MQgarbo.freeMemory() / MEGABYTE;
        try {
            while (!stopme) {
                if (rCnt < 2) {
                    if (nodata > 100) {
                        nodata = -1;
                        CheckStopNow(BPUPL, uStop);
                    }
                    try {
                        Thread.sleep(150);
                        nodata++;
                        NamedCommon.Reset();
                        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt); // MUST clear AES vars first.
                        Properties runProps = uCommons.LoadProperties("rFuel.properties");
                        uCommons.SetCommons(runProps);
                        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
                    } catch (InterruptedException e) {
                        uCommons.uSendMessage("Thread Sleep ERROR: " + e.getMessage());
                        System.exit(0);
                    }
                }

                usedMem = NamedCommon.MQgarbo.freeMemory() / MEGABYTE;
                if (freeMem - usedMem > GarbageCollector.getGCsize()) {
                    NamedCommon.MQgarbo.gc();
                    freeMem = NamedCommon.MQgarbo.freeMemory() / MEGABYTE;
                }

                rightNow = System.nanoTime();
                laps = (rightNow - lastAction) / div;
                if (laps >= maxWait) {
                    uCommons.uSendMessage("<<heartbeat>> Reconnect SourceDB - idle timeout exceeded.");
                    CheckStopNow(BPUPL, uStop);
                    SourceDB.DisconnectSourceDB();
                    if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
                    if (!NamedCommon.sConnected) System.exit(0);
                    ucmd = null;
                    ucmd = NamedCommon.uSession.command();
                    ucmd.setCommand(cmd);
                    uResponses = u2Commons.uOpen("uRESPONSES");
                    uCommons.uSendMessage("Looking for responses ....");
                    starting = startline;
                    lastAction = rightNow;
                } else {
                    if (laps > (NamedCommon.mqHeartBeat * hBeat)) {
                        int timeout = ((int) laps);
                        String timeLeft = uCommons.RightHash(String.valueOf(maxWait - timeout), rHash);
                        String msg = countdown.replace("$$", timeLeft);
                        uCommons.uSendMessage(msg);
                        hBeat++;
                    }
                }

                try {
                    ucmd.setCommand(cmd);
                    ucmd.exec();
                    rCnt = 0;
                    boolean eof = false;
                    while (!eof) {
                        try {
                            rID = NamedCommon.uSession.selectList(0).next();
                            sID = String.valueOf(rID);
                            if (sID.toUpperCase().equals(ping)) {
                                DeleteRecord(uResponses, rID, sID);
                                continue;
                            }
                            if (NamedCommon.uSession.selectList(0).isLastRecordRead() || sID.isEmpty()) {
                                rID = null;
                                sID = "";
                                eof = true;
                            } else {
                                mStart = System.nanoTime();
                                uCommons.uSendMessage(starting);
                                starting = "";
                                rCnt++;
                                if (badRecs.indexOf(sID) > -1) continue;
                                while(true) {
                                    try {
                                        if (debug) uCommons.uSendMessage("--- " + rID.toString());
                                        if (debug) uCommons.uSendMessage("1/4  lockRecord()");
                                        uResponses.lockRecord(rID, 1);
                                        break;
                                    } catch (UniFileException e) {
                                        uCommons.uSendMessage(e.getMessage());
                                        uCommons.Sleep(1);
                                    }
                                }
                                try {
                                    if (debug) uCommons.uSendMessage("2/4  read()");
                                    rRec = uResponses.read(rID);
                                    if (debug) uCommons.uSendMessage("3/4  unlockRecord()");
                                    uResponses.unlockRecord(rID);
                                    if (debug) uCommons.uSendMessage("4/4  deleteRecord()");
                                    uResponses.deleteRecord(rID);
                                    if (rRec.equals("")) {
                                        uCommons.uSendMessage("      " + rID.toString() + " is an empty record");
                                        continue;
                                    }

                                    dRec = new UniDynArray(rRec);
                                    Respond(dRec);
                                    dRec = null;
                                    hBeat = 1;

                                } catch (UniFileException e) {
                                    uCommons.Sleep(0);
                                    continue;
                                }

                                if (NamedCommon.ZERROR) {
                                    uCommons.uSendMessage("*");
                                    uCommons.uSendMessage("*******************************************");
                                    uCommons.uSendMessage("*");
                                    uCommons.uSendMessage("* Cannot create a Producer on <<" + reply2Q + ">>");
                                    uCommons.uSendMessage("* Reason: " + NamedCommon.Zmessage);
                                    uCommons.uSendMessage("* Stopping now.");
                                    uCommons.uSendMessage("*");
                                    uCommons.uSendMessage("*******************************************");
                                    System.exit(0);
                                }

                                try {
                                    if (uResponses.isRecordLocked(rID)) {
                                        while (uResponses.isRecordLocked(rID)) {
                                            uCommons.uSendMessage(rID.toString() + " is STILL locked !!");
                                            uResponses.unlockRecord(rID);
                                            uCommons.Sleep(1);
                                        }
                                    }
                                } catch (UniFileException e) {
                                    uCommons.uSendMessage("UniFileException #02: " + e.getMessage());
                                }

                                if (stopme) eof=true;
                                reply2Q = "";
                                sID = "";
                                rID = null;
                                rRec = null;
                            }
                        } catch (UniSelectListException e) {
                            uCommons.uSendMessage("UniSelectListException " + e.getMessage());
                            eof = true;
                            stopme = true;
                        }
                    }
                    // ----------- readnext id ----------------------------------------------------------
                } catch (UniCommandException e) {
                    uCommons.uSendMessage("UniCommandException() ERROR : " + e.getMessage());
                    uCommons.uSendMessage("Attempting: " + cmd);
                    SourceDB.DisconnectSourceDB();
                    NamedCommon.Zmessage = e.getMessage();
                    NamedCommon.ZERROR = true;
                    System.exit(0);
                }
                try {
                    NamedCommon.uSession.selectList(0).clearList();
                } catch (UniSelectListException e) {
                    // ignore this error ??
                    uCommons.uSendMessage("**********************************************************");
                    uCommons.uSendMessage("UniSelectListException() ERROR " + e.getMessage());
                    uCommons.uSendMessage("**********************************************************");
                }
                GarbageCollector.CleanUp();
                stopme = coreCommons.StopNow();
                u2Commons.uniExec("RELEASE uRESPONSES");
            }
        } catch (UniSessionException e) {
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            uCommons.uSendMessage("UniSessionException() ERROR : " + e.getMessage());
            NamedCommon.Zmessage = e.getMessage();
            NamedCommon.ZERROR = true;
            System.exit(0);
        }
    }

    private static void DoReal(String[] args) {

        int rCnt = 0, hBeat = 1, nodata=-1;
        if (NamedCommon.dbWait != maxWait) maxWait = NamedCommon.dbWait;
        int rHash = String.valueOf(maxWait).length() + 1;

        Hop.setShow(true);
        uCommons.uSendMessage("Looking for responses ....");
        String starting = startline, stopRec="";
        NamedCommon.MQgarbo.gc();
        GarbageCollector.setStart(System.nanoTime());
        freeMem = NamedCommon.MQgarbo.freeMemory() / MEGABYTE;

        String chkBP= "{RDI}{file=BP.UPL}{ITEM=STOP}";
        String rdi  = "{RDI}{file=uRESPONSES}";
        String del  = "{DEL}{file=uRESPONSES}";
        String srtn = "{SAR}";
        String sCmd = "{cmd=SELECT uRESPONSES}";
        String file = "{file=uRESPONSES}";
        String item = "{ITEM=}";
        String atr  = "{atr=-1}";
        String hush = "{hush=true}";
        String cSel = srtn + sCmd + file + item + atr + hush;
        String line, recId, response, uStop="STOP";
        boolean stopme = false;
        while (!stopme) {
            line = u2Commons.MetaBasic(cSel);
            if (NamedCommon.ZERROR) return;

            // pause to wait for some data - do not chew up cpu !!

            if (line.equals("")) {
                if (rCnt < 2) {
                    if (nodata > 100) {
                        nodata = -1;
                        response = u2Commons.MetaBasic(chkBP+hush);
                        if (response.toUpperCase().contains(uStop)) {
                            uCommons.uSendMessage("MASTER STOP switch set on. Stopping now.");
                            return;
                        }
                    }
                    try {
                        Thread.sleep(150);
                        nodata++;
                        NamedCommon.Reset();
                        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt); // MUST clear AES vars first.
                        Properties runProps = uCommons.LoadProperties("rFuel.properties");
                        uCommons.SetCommons(runProps);
                        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
                    } catch (InterruptedException e) {
                        uCommons.uSendMessage("Thread Sleep ERROR: " + e.getMessage());
                        System.exit(0);
                    }
                }
                usedMem = NamedCommon.MQgarbo.freeMemory() / MEGABYTE;
                if (freeMem - usedMem > GarbageCollector.getGCsize()) {
                    NamedCommon.MQgarbo.gc();
                    freeMem = NamedCommon.MQgarbo.freeMemory() / MEGABYTE;
                }
                rightNow = System.nanoTime();
                laps = (rightNow - lastAction) / div;
                if (laps >= maxWait) {
                    response = u2Commons.MetaBasic(chkBP+hush);
                    if (response.toUpperCase().contains(uStop)) {
                        uCommons.uSendMessage("MASTER STOP switch set on. Stopping now.");
                        return;
                    }
                    uCommons.uSendMessage("<<heartbeat>> Reconnect SourceDB - idle timeout exceeded.");
                    SourceDB.DisconnectSourceDB();
                    if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
                    if (!NamedCommon.sConnected) System.exit(0);
                    lastAction = rightNow;
                } else {
                    if (laps > (NamedCommon.mqHeartBeat * hBeat)) {
                        int timeout = ((int) laps);
                        String timeLeft = uCommons.RightHash(String.valueOf(maxWait - timeout), rHash);
                        String msg = countdown.replace("$$", timeLeft);
                        uCommons.uSendMessage(msg);
                        hBeat++;
                    }
                }
                continue;
            }

            ArrayList<String> items = new ArrayList<>(Arrays.asList(line.split("<im>")));
            for (int i=0; i < items.size(); i++) {
                recId = items.get(i).split("<km>")[0];
                if (recId.equals("")) continue;
                response = u2Commons.MetaBasic(rdi + "{item="+recId+"}");
                if (NamedCommon.ZERROR) {
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    return;
                }
                UniDynArray dRec = new UniDynArray(uCommons.SQL2UVRec(response));
                Respond(dRec);
                dRec = null;
                if (!NamedCommon.ZERROR) {
                    response = u2Commons.MetaBasic(del + "{item="+recId+"}");
                    if (NamedCommon.ZERROR) {
                        uCommons.uSendMessage(NamedCommon.Zmessage);
                        return;
                    }
                }
                rCnt++;
                lastAction = System.nanoTime();
            }
        }
    }

    private static void Respond (UniDynArray dRec) {
        String response = String.valueOf(dRec.extract(1));
        String reply2Q = String.valueOf(dRec.extract(2));
        String corrID = String.valueOf(dRec.extract(3));
        String fmt = String.valueOf(dRec.extract(4));
        dRec = null;

        if (debug) uCommons.uSendMessage("Extracted the response from the record");
        if (!response.startsWith(NamedCommon.xmlProlog) && !fmt.equals(raw)) {
            String mBody = response.substring(response.indexOf("><") + 1, response.length());
            response = NamedCommon.xmlProlog + mBody;
            mBody = "";
        }

        String tstStr = reply2Q;
        while (tstStr.contains(" ")) { tstStr = tstStr.replace(" ", ""); }

        if (tstStr.isEmpty()) {
            reply2Q = "RunERRORS";
            response = "No ReplyTo provided for CorrelationID [" + corrID + "].\n" + response;
        }
        tstStr = corrID;
        while (tstStr.contains(" ")) { tstStr = tstStr.replace(" ", ""); }
        if (tstStr.isEmpty()) corrID = "No_CorrelationID";
        tstStr = "";
        if (!(fmt == raw)) response = uConnector.Format(response, fmt);
        if (debug) uCommons.uSendMessage("Formatted the response");

        if (debug) uCommons.uSendMessage("Sending the response to \"" + reply2Q + "\"");

        NamedCommon.CorrelationID = corrID;
        NamedCommon.reply2Q = reply2Q;

        if (reply2Q.startsWith("temp")) {
            uCommons.uSendMessage("Temp-Queue handler --------------------");
            Hop.start(response, "", nextBrk, reply2Q, "ACK", corrID);
        } else {
            Hop.start(response, "", nextBrk, reply2Q, "uRestResponder", corrID);
        }
        lastAction = System.nanoTime();
    }

    private static boolean DeleteRecord(UniFile uResponses, UniString rID, String sID) {
        boolean stopme=false;
        try {
            uResponses.deleteRecord(rID);
            if (!sID.toUpperCase().equals("PING")) {
                if (debug) uCommons.uSendMessage("Deleted response from " + NamedCommon.databaseType);
                mFinish = System.nanoTime();
                mTime = (mFinish - mStart) / div;
                uCommons.uSendMessage("   Message ID [" + sID + "] processed in " + mTime + " second.");
                uCommons.uSendMessage(underline);
                lastAction = System.nanoTime();
            }
            uCommons.uSendMessage("   Message ID [" + rID.toString() + "] unlocked and deleted.");
            while (uResponses.isRecordLocked(rID)) {
                uResponses.unlockRecord(rID);
                uCommons.Sleep(0);
            }
        } catch (UniFileException e) {
            uCommons.uSendMessage("uniFile delete failed on uRESPONSES " + sID);
            badRecs.add(sID);
            if (badRecs.size() > 10) {
                uCommons.uSendMessage("too many bad records that cannot be deleted.");
                stopme = true;
            }
        }
        return stopme;
    }

    private static void CheckStopNow(UniFile BPUPL, UniString uStop) {
        String stopRec = "";
        try {
            BPUPL.setRecordID(uStop);
            stopRec = BPUPL.read().toString();
        } catch (UniFileException e) {
            stopRec = "";
        }
        if (!stopRec.equals("")) {
            System.out.println(" ");
            while (true) {
                uCommons.uSendMessage("<<heartbeat>> Waiting for the pid to die.");
                uCommons.Sleep(3);
                System.exit(0);
            }
        }
    }

}
