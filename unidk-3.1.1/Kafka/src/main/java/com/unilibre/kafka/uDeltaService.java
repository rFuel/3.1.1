package com.unilibre.kafka;
/**
 * Copyright UniLibre on 2015. ALL RIGHTS RESERVED
 */

import asjava.uniobjects.*;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.kafkaCommons;
import com.unilibre.commons.uCommons;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public class uDeltaService {

    private static final DecimalFormat decfmt = new DecimalFormat("#,###.####");
    private static final String SAVEDLISTS = "&SAVEDLISTS&", LOGFILE = "uDELTA.LOG", TMARK="<tm>";
    private static final String HEARTBEAT = "<<heartbeat>>", SUBR = "SR.GETDELTAKEYS";
    private static final String STOP_FILE = "./conf/STOP", crlf = "\n";
    static Thread[] threads = null;
    static Properties props;
    static String[] tArgs = null;
    static FutureTask[] futures;
    static ArrayList<String> eventList = new ArrayList<>();;
    static String runID, slID, extN, cmd, cfg ,retries, acks, main, keySerdes, valSerdes, compression, coreApp;
    static int numThreads = 4, maxpool, kafkaBatchSize, hbCnt = 250, pause = 100, minSize=131082;
    static int fetchBatchSize = 1, maxblockms = 10000, proccnt = 1000, epoch=0, acceptable=30;
    static boolean nothingToDo = false, verbose = false, sourceConnected = false, reconnect = false;
    static boolean hush=false, isThreadCaller = true, cpl = false, batching=false;
    static UniJava uj;
    static UniSession us;
    static UniFile uf;
    static UniFile SL;
    static UniSelectList sl;
    static UniSubroutine getkeys;
    static double uvlaps, klaps, laps, div = 1000000000.00;
    static long start, finish, kstart, uvstart, uvfinish;

    public static void main(String[] args) throws InterruptedException, ExecutionException, UniSessionException {

        String sProcs = uCommons.ReadDiskRecord("/proc/1/cgroup", true);
        if (!NamedCommon.isDocker) NamedCommon.isDocker = (sProcs.contains("/docker/"));
        if (!NamedCommon.isDocker) NamedCommon.isDocker = (sProcs.contains("/kubepods"));
        //
        main = " MAIN   ";
        System.out.println(new Date() + main + "uDeltaService starting.");
        runID = UUID.randomUUID().toString();                   // make it unique
        runID = runID.replaceAll("\\r?\\n", "");
        Setup();
        ConnectDB();
        if (SL == null) return;
        Reclaim();
        hush = true;
        slID = runID;
        if (extN.startsWith(".") && !slID.startsWith(".")) slID = "." + slID;
        System.out.println("");
        String rightnow = uCommons.GetLocaltimeStamp(); // new Date();
        System.out.println(rightnow + main + "MainLine(" + runID + ")");
        MainLine();
        DisConnectDB();
        System.out.println(new Date() + main + "uDeltaService is finished.");
    }

    private static void MainLine() throws ExecutionException, InterruptedException, UniSessionException {
        // ---------------------------------------------------------------------
        // current mod: hangCnt > 100 - stop the container
        // ---------------------------------------------------------------------
        boolean keepRunning = CheckStopSW();
        int noData = 0;
        boolean firstSW = true;
//        Date rightnow;
        ArrayList<String> returnList = new ArrayList<>();
        ArrayList<Integer> finishedT = new ArrayList<>();       // finished Threads

        int hangCnt = 0, originalKblock = kafkaBatchSize;
        while (keepRunning) {
            start = System.nanoTime();
            eventList.clear();
            for (int i = 0; i < numThreads; i++) { eventList.add(""); }
            GetDeltaKeys();
            if (SL == null) return;

            // fire off the worker threads -----------------------------

            hangCnt = 0;
            if (!nothingToDo) {
                noData = 0;
                if (numThreads > 0) {
                    threads = new Thread[numThreads];
                    uDelta2kafka[] d2k = new uDelta2kafka[numThreads];
                    kstart = System.nanoTime();                 // when kafka processes started
                    if (isThreadCaller) {
                        CallThreads(d2k);
                    } else {
                        RunThreads(d2k);
                    }
                    System.out.println(new Date() + main + "... wait for threads. ------------------------------------");

                    boolean areAlive=true;
                    Object obj;
                    finishedT.add(9999);
                    while (areAlive) {
                        if (hangCnt > 100) {
                            System.out.println(new Date() + main + "some threads are not responding - stopping this process");
                            keepRunning = false;
                            break;
                        }
                        for (int x = 0; x < numThreads; x++) {
                            Thread.sleep(100);
                            if (isThreadCaller) {
                                if (finishedT.indexOf(x) > 0) continue;
                                if (futures[x].isDone()) {
                                    System.out.println(new Date() + main + "thread # " + x + " has finished. ************");
                                    obj = futures[x].get();
                                    if (obj instanceof ArrayList) returnList.addAll((Collection<? extends String>) obj);
                                    obj = null;
                                    finishedT.add(x);
                                } else {
                                    Thread.sleep(500);
                                }
                            } else {
                                if (threads[x].isAlive()) areAlive = true;
                            }
                        }
                        if (finishedT.size() > numThreads) areAlive = false;
                        hangCnt++;
                    }

                    if (hangCnt > 100) break;

                    finish = System.nanoTime();
                    klaps  = (finish - kstart) / div;
                    laps = (finish - start) / div;
                    String rightnow = uCommons.GetLocaltimeStamp(); // new Date();

                    if (isThreadCaller) {
                        System.out.println(new Date() + main + "PostProcessHandler");
                        PostProcessHandler(returnList);
                        returnList.clear();
                    }

                    System.out.println(rightnow + " [loop summary]--------------------------------------------------------------");
                    System.out.println(rightnow + main + "               UV hook : " + decfmt.format(uvlaps));
                    System.out.println(rightnow + main + "         Kafka threads : " + decfmt.format(klaps));
                    System.out.println(rightnow + main + "All threads finished in: " + decfmt.format(laps) + " seconds ");
                    System.out.println(rightnow + main + "Events per seconds " + decfmt.format(fetchBatchSize / laps));
                    System.out.println(rightnow + " ----------------------------------------------------------------------------");
                    System.out.println(rightnow);

                    laps = (finish - kstart) / div;
                    if (laps > acceptable) {
                        if (kafkaBatchSize <= minSize) {
                            // it's already at the lowest value - try upping it again.
                            kafkaBatchSize = originalKblock;
                        } else {
                            kafkaBatchSize = kafkaBatchSize / 2;
                            if (kafkaBatchSize < minSize) kafkaBatchSize = minSize;
                        }
                        System.out.println(new Date() + main + "---------------------------------");
                        System.out.println(new Date() + main + "reset block size: " + kafkaBatchSize);
                        System.out.println(new Date() + main + "---------------------------------");
                    } else {
                        kafkaBatchSize = originalKblock;
                    }

                    if (isThreadCaller) futures = null;
                    threads = null;
                    d2k = null;
                    finishedT.clear();
                    firstSW = true;
                } else {
                    uDelta2kafka d2k = null;
                    for (int i = 0; i < eventList.size(); i++) {
                        tArgs[10] = eventList.get(i);
                        if (tArgs[10] == null) continue;
                        d2k = new uDelta2kafka(tArgs, 1);
                        d2k.containerRunner();
                        PostProcessHandler(d2k.GetDeleteables());
                        d2k = null;
                    }
                }
            } else {
                if (firstSW) {
                    System.out.println("waiting on delta events");
                    firstSW = false;
                }
                Reclaim();
                kafkaCommons.Sleep(pause);
                noData++;
                if (noData >= hbCnt) {
                    String rightnow = uCommons.GetLocaltimeStamp(); // new Date();
                    System.out.println(rightnow + " " + HEARTBEAT);
                    noData = 0;
                }
            }

            if (hangCnt < 100) keepRunning = CheckStopSW();
//            rightnow = null;
        }
    }

    private static void Reclaim() throws InterruptedException {
        if (!ReclaimOrphans()) {
            System.out.println(new Date() + main + "stopping.");
            DisConnectDB();
            System.exit(1);
        }
    }

    private static void CallThreads(uDelta2kafka[] d2k) {
        futures = new FutureTask[numThreads];
        for (int i = 0; i < numThreads; i++) {
            tArgs[10] = eventList.get(i);
            if (tArgs[10] == null) break;
            Callable callme = SetThread(i);
            futures[i] = new FutureTask(callme);
            Thread t = new Thread(futures[i]);
            t.start();
            eventList.set(i, "");
            t = null;
        }
    }

    private static void RunThreads(uDelta2kafka[] d2k) {
        // Runnable threads CANNOT return values to the invoking process.
        for (int i = 0; i < numThreads; i++) {
            tArgs[10] = eventList.get(i);
            if (tArgs[10] == null) break;
            d2k[i] = SetThread(i);
            Thread t = new Thread((Runnable) d2k[i]);
            threads[i] = t;
            threads[i].start();
            eventList.set(i, "");
            t = null;
        }
    }

    private static void PostProcessHandler(ArrayList<String> list) {
        int eop = list.size();
        System.out.println(new Date() + main + "handle " + eop + " events - this includes tags.");
        String subrName="", line="", lastAction="";
        ArrayList<String> actionItems = new ArrayList<>();

        for (int i = 0; i < eop; i++) {
            line = list.get(i);
            switch (line) {
                case "DELETE":
                    if (actionItems.size() > 0) {
                        System.out.println(new Date() + main + "" + lastAction + "  " + actionItems.size() + " events");
                        subrName = "SR." + lastAction + "KEYS";
                        ProcessItems(subrName, actionItems);
                        subrName = "";
                    }
                    actionItems.clear();
                    lastAction = line;
                    break;
                case "RECLAIM":
                    if (actionItems.size() > 0) {
                        System.out.println(new Date() + main + "" + lastAction + "  " + actionItems.size() + " events");
                        subrName = "SR." + lastAction + "KEYS";
                        ProcessItems(subrName, actionItems);
                        subrName = "";
                    }
                    actionItems.clear();
                    lastAction = line;
                    break;
                default:
                    actionItems.add(line);
                    break;
            }
        }
        if (actionItems.size() > 0) {
            subrName = "SR." + lastAction + "KEYS";
            switch (lastAction) {
                case "DELETE":
                    System.out.println(new Date() + main + "deleting " + actionItems.size() + " events (final)");
                    ProcessItems(subrName, actionItems);
                    break;
                case "RECLAIM":
                    System.out.println(new Date() + main + "reclaiming " + actionItems.size() + " events (final)");
                    ProcessItems(subrName, actionItems);
                    break;
                default:
                    System.out.println(new Date() + main + "UNHANDLED action: " + line + "  " + actionItems.size() + " events (final)");
                    break;
            }
        }
        actionItems.clear();
    }

    private static void ProcessItems(String subrName, ArrayList<String> list) {
        if (subrName.equals("")) return;
        String sbList = String.join(TMARK, list);
        try {
            UniSubroutine ppSubr = us.subroutine(subrName, 3);
            ppSubr.setArg(0, "");
            ppSubr.setArg(1, LOGFILE);
            ppSubr.setArg(2, sbList.toString());
            ppSubr.call();
            String ans = ppSubr.getArg(0);
            if (!ans.equals("")) System.out.println(new Date() + main + "" + ans);
            ppSubr = null;
        } catch (UniSubroutineException e) {
            System.out.println(new Date() + main + "      >>  UniSubroutineException " + e.getMessage());
        } catch (UniSessionException e) {
            System.out.println(new Date() + main + "      >>  UniSessionException:  " + e.getMessage());
            return;
        }
        sbList = null;
    }

    private static uDelta2kafka SetThread(int i) {
        uDelta2kafka d2k = new uDelta2kafka(tArgs, i);
        d2k.SetSASL(false, "", "", "");
        d2k.SetBroker(tArgs[14]);
        d2k.SetTopic(tArgs[13]);
        d2k.SetGroup(tArgs[11]);
        d2k.SetClientID("uDS-"+runID);
        d2k.SetTransID(epoch);
        d2k.SetBatchSize(kafkaBatchSize);
        d2k.SetBlockingMS(maxblockms);
        d2k.SetSerdes(keySerdes, valSerdes);
        d2k.SetProcCnt(proccnt);
        d2k.SetAcks(acks);
        d2k.SetRetries(retries);
        d2k.SetBatching(batching);
        d2k.SetCompression(compression);
        d2k.SetCoreApp(coreApp);
        epoch++;
        if (epoch > 10000) epoch = 0;
        return d2k;
    }

    private static void Setup() {
        String rightnow = uCommons.GetLocaltimeStamp(); // new Date();
        System.out.println(rightnow + main + "starting Setup()");
//        rightnow = null;
        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);

        if (NamedCommon.upl.equals("")) {
            NamedCommon.upl = System.getProperty("user.dir");
            NamedCommon.BaseCamp = NamedCommon.upl;
        }
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            uCommons.SetMemory("domain", "rfuel22");
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            NamedCommon.upl = NamedCommon.BaseCamp;
            NamedCommon.slash = "/";
        }

        cfg = System.getProperty("conf", "NO-CONF-FILE");
        rightnow = uCommons.GetLocaltimeStamp(); // new Date();
        System.out.println(rightnow + main + "using the properties of " + cfg);
//        rightnow = null;
        props = uCommons.LoadProperties(cfg);

        int lx = String.valueOf(props.getProperty("threads", "0")).length() + 1;
        main = " MAIN";
        for (int l = 0 ; l < lx ; l++) { main += " "; }

        String runtype = props.getProperty("runtype", "uplift");
        batching= (props.getProperty("batching", "false").toLowerCase().equals("true"));
        acks    = props.getProperty("acks", "all");
        extN    = props.getProperty("extension", "");
        verbose = props.getProperty("verbose", "false").toLowerCase().equals("true");
        cpl     = props.getProperty("cpl", "false").toLowerCase().equals("true");

        tArgs = new String[25];
        for (int i = 0; i < 25; i++) {tArgs[i] = "";}

        retries = "";
        try {
            String sChk = props.getProperty("retries", "");
            int iChk = Integer.valueOf(sChk);
            retries = sChk;
        } catch (NumberFormatException nfe) {
            retries = Integer.toString(Integer.MAX_VALUE);
        }

        fetchBatchSize = 1;
        try {
            fetchBatchSize = Integer.valueOf(props.getProperty("fbatch", "1"));
        } catch (NumberFormatException nfe) {
            System.out.println(new Date() + main + "fbatch size must be an integer!!");
        }


        kafkaBatchSize = 1048576;
        try {
            kafkaBatchSize = Integer.valueOf(props.getProperty("kbatch", "1048576"));
        } catch (NumberFormatException nfe) {
            System.out.println(new Date() + main + "kbatch size must be an integer!!");
        }


        maxblockms = 30000;
        try {
            maxblockms = Integer.valueOf(props.getProperty("maxblockms", "30000"));
        } catch (NumberFormatException nfe) {
            System.out.println(new Date() + main + "maxblockms size must be an integer!!");
        }

        proccnt = 100;
        try {
            proccnt = Integer.valueOf(props.getProperty("proccnt", "100"));
        } catch (NumberFormatException nfe) {
            System.out.println(new Date() + main + "proccnt size must be an integer!!");
        }

        GetSerDes();

        tArgs[0] = props.getProperty("u2host", "");
        tArgs[1] = props.getProperty("u2path", "");
        tArgs[2] = props.getProperty("u2user", "");
        tArgs[3] = props.getProperty("u2pass", "");
        tArgs[4] = props.getProperty("dbtype", "");
        tArgs[5] = props.getProperty("u2type", "");
        tArgs[6] = props.getProperty("protocol", "");
        tArgs[7] = props.getProperty("secure", "");
        tArgs[8] = props.getProperty("maxpool", "");
        tArgs[9] = props.getProperty("threads", "");
        tArgs[10] = props.getProperty("DO-NOT-USE", "");
        tArgs[11] = props.getProperty("group", "");                         // e.g. "uStream"
        tArgs[12] = props.getProperty("debug", "");
        tArgs[13] = props.getProperty(runtype, "NoTopicGiven");
        tArgs[14] = props.getProperty("brokers", "");                       // unused
        tArgs[15] = String.valueOf(fetchBatchSize);
        tArgs[16] = String.valueOf(verbose);

        compression = props.getProperty("compression", "");
        coreApp     = props.getProperty("core.application", "");            // E.g. ultracs / era

        try {
            numThreads = Integer.parseInt(tArgs[9]);
            maxpool = Integer.parseInt(tArgs[8]);
            hbCnt = Integer.valueOf(props.getProperty("heartbeat", "100"));
            pause = Integer.valueOf(props.getProperty("pause", "250"));
        } catch (NumberFormatException nfe) {
            numThreads = 4;
            maxpool = 4;
            hbCnt = 100;
            pause = 250;
        }

        cmd = " ";
        rightnow = uCommons.GetLocaltimeStamp(); // new Date();
        System.out.println(rightnow + main + "[kafka config] ------------------------------------");
        System.out.println(rightnow + main + "    brokers: " + tArgs[14]);
        System.out.println(rightnow + main + "      topic: " + tArgs[13]);
        System.out.println(rightnow + main + "      group: " + tArgs[11]);
        System.out.println(rightnow + main + "    threads: " + numThreads);
        System.out.println(rightnow + main + "  k-BatchSZ: " + kafkaBatchSize);
        System.out.println(rightnow + main + "  f-BatchSZ: " + fetchBatchSize);
        System.out.println(rightnow + main + " MaxBlockMS: " + maxblockms);
        System.out.println(rightnow + main + "  keySerdes: " + keySerdes);
        System.out.println(rightnow + main + "  valSerdes: " + valSerdes);
        System.out.println(rightnow + main + "---------------------------------------------------");

        // Check Kafka is available
        rightnow = uCommons.GetLocaltimeStamp(); // new Date();
        System.out.println(rightnow + main + "Checking if Kafka is ready.");
//        rightnow = null;

        uDelta2kafka d2k = null;
        d2k = new uDelta2kafka(tArgs, 0);
        d2k.SetSASL(false, "", "", "");
        d2k.SetBroker(tArgs[14]);
        d2k.SetTopic(tArgs[13]);
        d2k.SetGroup(tArgs[11]);
        d2k.SetClientID("uDS-"+runID);
        d2k.SetTransID(0);
        d2k.SetBatchSize(kafkaBatchSize);
        d2k.SetBlockingMS(maxblockms);
        d2k.SetSerdes(keySerdes, valSerdes);
        d2k.SetProcCnt(proccnt);
        d2k.SetAcks(acks);
        d2k.SetRetries(retries);
        d2k.SetBatching(batching);
        d2k.SetCompression(compression);
        d2k.SetVerbose(true);

        rightnow = uCommons.GetLocaltimeStamp(); // new Date();
        if (!d2k.kafkaIsReady()) {
            System.out.println(rightnow + main + "Kafka is unreachable.");
            System.exit(0);     // exit code 0 so it does NOT restart automatically
        } else {
            d2k.Close();
            System.out.println(rightnow + main + "Kafka is ready");
        }
//        rightnow = null;
        d2k = null;
    }

    private static void GetSerDes() {
        // NB:      This is a producer - it will only SERIALISE data
        // -------------------------------------------------------------------------
        // serdes and serialiser do the "same"
        // they act as the default for both key and value
        String serdes   = props.getProperty("serdes", "");
        String serial   = props.getProperty("serialiser", "");
        //
        String kserial  = props.getProperty("kserialiser", "");
        String vserial  = props.getProperty("vserialiser", "");
        //
        // set as default
        //
        keySerdes = "STRING";
        valSerdes = keySerdes;
        //
        if (serdes.toUpperCase().equals("BYTE")) {
            keySerdes = serdes.toUpperCase();
            valSerdes = keySerdes;
        }
        if (serial.toUpperCase().equals("BYTE")) {
            keySerdes = serial.toUpperCase();
            valSerdes = keySerdes;
        }
        if (kserial.toUpperCase().equals("BYTE")) {
            keySerdes = kserial.toUpperCase();
        }
        if (vserial.toUpperCase().equals("BYTE")) {
            valSerdes = vserial.toUpperCase();
        }
    }

    private static boolean ReclaimOrphans() throws InterruptedException {
        if (!hush) System.out.println(new Date() + main + "Reclaim orphan events.");
        boolean okay = true;
        try {
            UniSubroutine ppSubr = us.subroutine("SR.ORPHANKEYS", 3);
            ppSubr.setArg(0, "");
            ppSubr.setArg(1, LOGFILE);
            ppSubr.setArg(2, "");
            ppSubr.call();
            String ans = ppSubr.getArg(0);
            if (!ans.equals("")) if (!hush) System.out.println(new Date() + main + "   >> " + ans);
            ppSubr = null;
            if (!hush) System.out.println(new Date() + main + "done.");
        } catch (UniSubroutineException e) {
            System.out.println(new Date() + main + "      >>  UniSubroutineException " + e.getMessage());
            okay = false;
        } catch (UniSessionException e) {
            System.out.println(new Date() + main + "      >>  UniSessionException:  " + e.getMessage());
            okay = false;
        }
        return okay;
    }

    private static void GetDeltaKeys() throws UniSessionException, InterruptedException {
        if (us == null) ConnectDB();
        if (us == null) return;
        if (SL == null) return;
        long uvstart = System.nanoTime();
        String sep = "|";
        int bucket = 0, nbrItems = 0;
        String ans="", msg="", startMsg="";
        //
        // Get fetchBatchSize number of deltas from uDELTA.LOG
        //      Modes of delta gets:
        //      1. singular call to subroutine
        //      2. threaded buffered reader of uDELTA.LOG items
        startMsg = new Date() + main + "GetKeys(" + fetchBatchSize + ")";
        int gulps = (int) (fetchBatchSize / 100);
        int rem   = fetchBatchSize - (gulps * 100);
        //
        StringBuilder sb = new StringBuilder();
        for (int i=0 ; i < gulps; i++) {
            sb.append(GetDeltaGroup(100));
        }
        if (rem > 0) sb.append(GetDeltaGroup(rem));
        ans = sb.toString();

//        try {
//            getkeys.setArg(0, "");
//            getkeys.setArg(1, uf.getFileName());
//            getkeys.setArg(2, fetchBatchSize);
//            getkeys.setArg(3, "");
//            getkeys.setArg(4, extN);
//            getkeys.setArg(5, slID);
//            getkeys.call();
//            ans = getkeys.getArg(0);
//            if (verbose) {
//                msg = getkeys.getArg(3);
//                if (!msg.equals("")) {
//                    String[] messages = msg.split("\\r?\\n");
//                    for (int m = 0; m < messages.length; m++) {
//                        if (!messages[m].equals("")) {
//                            System.out.println(new Date() + "      " + messages[m]);
//                        }
//                    }
//                    messages = null;
//                }
//            }
//        } catch (UniSubroutineException e) {
//            System.out.println(e.getMessage());
//        }

        //
        // Now spread fetchBatchSize deltas into datasets for threading
        //
        String[] strArray = splitString(ans, crlf);
        ans = "";
        nbrItems = strArray.length;
        if (nbrItems > 0) {
            System.out.println(startMsg);
            System.out.println(new Date() + main + "found  (" + nbrItems + ")");
            int[] eventArray = new int[numThreads];

            StringBuilder[] sbIDs = new StringBuilder[numThreads];
            for (int i = 0; i < numThreads; i++) { sbIDs[i] = new StringBuilder("");}

            String item;
            bucket = 0;

            // ---------------------------------------------------------------
            // ans is string  with linefeeds separating each record.
            // spread each record in ans across numthreads lists
            // each list is a string with "|" deliminating records within it
            // ---------------------------------------------------------------

            for (int i = 0; i < nbrItems; i++) {
                item = strArray[i];
                if (item.equals("")) continue;        // exclude empty elements
                sbIDs[bucket].append(item + sep);
                eventArray[bucket]++;
                bucket++;
                if (bucket >= numThreads) bucket = 0;
            }

            for (int j = 0; j < numThreads; j++) { eventList.set(j, sbIDs[j].toString()); }

            if (eventArray[0] == 0) nothingToDo = true;
            else nothingToDo = false;
            if (nothingToDo) nbrItems = 0;               // excludes empty elements
            strArray = null;
            sl = null;
        } else {
            nothingToDo = true;
        }

        long uvfinish = System.nanoTime();
        uvlaps = (uvfinish - uvstart) / div;
    }

    private static String GetDeltaGroup(int recs) {
        try {
            getkeys.setArg(0, "");
            getkeys.setArg(1, uf.getFileName());
            getkeys.setArg(2, recs);
            getkeys.setArg(3, "");
            getkeys.setArg(4, extN);
            getkeys.setArg(5, slID);
            getkeys.call();
            return getkeys.getArg(0);
        } catch (UniSubroutineException e) {
            System.out.println(e.getMessage());
            return "";
        }
    }

    private static String[] splitString(String line, String delimiter) {
        CharSequence[] chrSeq = new CharSequence[(line.length() / 2) + 1];
        int recCount = 0;
        int i = 0;
        int j = line.indexOf(delimiter, 0); // first substring

        while (j >= 0) {
            chrSeq[recCount++] = line.substring(i, j);
            i = j + 1;
            j = line.indexOf(delimiter, i); // rest of substrings
        }

        if (!line.substring(i).equals("")) chrSeq[recCount++] = line.substring(i); // last substring

        String[] result = new String[recCount];
        System.arraycopy(chrSeq, 0, result, 0, recCount);
        line = "";
        chrSeq = null;
        return result;
    }

    private static void ConnectDB() {
//        Date rightnow;
        String protocol = "";
        boolean secure = tArgs[7].toLowerCase().equals("true");
        if (tArgs[6].equals("u2cs")) {
            protocol = "uvcs";
            if (tArgs[4].equals("UNIDATA")) protocol = "udcs";
        }
        try {
            UniJava.setIdleRemoveThreshold(60000);    // max session idle time  = 60 seconds
            UniJava.setIdleRemoveExecInterval(15000); // look for idle sessions = 15 seconds
            UniJava.setOpenSessionTimeOut(3000);      // wait 3 seconds for a session to open
            UniJava.setUOPooling(cpl);
            if (cpl) {
                UniJava.setMinPoolSize(1);
                UniJava.setMaxPoolSize(maxpool);
            }
            uj = new UniJava();
            uj.openSession(UniObjectsTokens.SECURE_SESSION);
            us = uj.openSession();
            if (secure) {
                System.setProperty("javax.net.sslTrustStore", "/usr/lib/jvm/java-11-amazon-corretto/lib/security/cacerts");
                System.setProperty("javax.net.trustStorePassword", "changeit");
                us.setSSLDescriptor(null);
            }
            us.setHostName(tArgs[0]);
            us.setHostPort(31438);
            us.setAccountPath(tArgs[1]);
            us.setUserName(tArgs[2]);
            us.setPassword(tArgs[3]);
            us.setConnectionString(protocol);
            String rightnow = uCommons.GetLocaltimeStamp(); // new Date();
            if (!reconnect) {
                System.out.println(rightnow + main + "ConnectDB()");
                System.out.println(rightnow + "        Host    : " + us.getHostName());
                System.out.println(rightnow + "        Port    : " + us.getHostPort());
                System.out.println(rightnow + "        Account : " + us.getAccountPath());
                System.out.println(rightnow + "        User    : " + us.getUserName());
                System.out.println(rightnow + "        Type    : " + us.getConnectionString());
                System.out.println(rightnow + "        SECURE  : " + secure);
//                rightnow = new Date();
                rightnow = uCommons.GetLocaltimeStamp(); // new Date();
                System.out.println(rightnow + main + "Connected to " + tArgs[4] + " host at " + tArgs[0] + " via " + protocol);
//                rightnow = null;
                reconnect = true;
            }
//            rightnow = null;
            System.out.println(new Date() + main + "connecting to source host");
            us.connect();
            uf = us.open(LOGFILE);
            SL = us.open(SAVEDLISTS);
            getkeys = us.subroutine(SUBR, 6);
            sourceConnected = true;
            System.out.println(new Date() + main + "connection established.");
        } catch (UniSessionException e) {
            System.out.println("********************************************************************");
            System.out.println("UniSessionException: [" + e.getErrorCode()+"]   " + e.getMessage());
            System.out.println("********************************************************************");
            sourceConnected = false;
        }
    }

    private static void DisConnectDB() {
        if (sourceConnected) {
            try {
                SL.close();
                uf.close();
                uj.closeSession(us);
                uj = null;
                us = null;
                uf = null;
                SL = null;
                sourceConnected = false;
            } catch (UniSessionException e) {
                System.out.println(e.getMessage());
            } catch (UniFileException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static boolean CheckStopSW() {
        StringBuilder rec = new StringBuilder();
        try {
            FileReader fr = new FileReader(STOP_FILE);
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            line = br.readLine();
            while ((line) != null) {
                rec.append(line + "\n");
                line = br.readLine();
            }
            line = "";
            br.close();
            br = null;
            fr.close();
            fr = null;
        } catch (IOException e) {
            rec = null;
            return true;        // i.e. keepRunning = true
        }
        boolean ans = rec.toString().toLowerCase().contains("stop");
        rec = null;
        return (!ans);
    }

}
