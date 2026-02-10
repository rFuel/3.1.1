package com.unilibre.kafka.dev;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

// ----------------------[AUDIT_LOG_TYPE 1 : dynamic files]---------------------------------------------------
//  uvhashStreams connects with a U2 host with "uvadm" permissions. It will maintain
//  UV audlog files before they grow too large.
//
//  MUST have connection to the UV account (i.e. uvHome)
//  MUST know the AUDIT_LOG_MAX value -- which MUST be > 1 or auditing will not work
//      $ which uv  ==>  /usr/uv/bin/uv
//      uvhome = usr/uv
//      cat {uvhome}/uvconfig | grep AUDIT_LOG_MAX
//      # AUDIT_LOG_MAX - Specifies the maximum number of audit log files. The value
//      AUDIT_LOG_MAX   4
//  Will loop through each &AUDLOGn& and find events the customer wants to stream
//
//  if kDirect :--
//      Will send each event to MQ virtual-topic then processed by rFuel in 022 queues.
//  if not kDirect  :--
//      Will place each event in a batch of events - use for slow networks or big volumes
//
//  Will "-clearlog n" once it has processed all the items in the file.
//  Will move to the next &AUDLOGn& and repeat the process.
// ---------------------------------------------------------------------------------

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.License;
import com.unilibre.commons.kafkaCommons;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SourceDB;
import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kProducer;
import org.json.JSONObject;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class uvhashStreams {

    private static String namedFile, encSeed="", passport="";
    private static String rfHome, uvHome, protocol, kBrokers;
    private static String nof = "Not..Configured!";
    private static String ClientID;
    private static String dCnv="D4-", tCnv="MTS";
    private static String SUSPEND, CLEANUP, RESUME, register, defaults;
    private static String purpose = "Take deltas from UniVerse audit logs and write them to Kafka topic ";
    private static boolean stopSW=false, ENCR=true, verbose=false, scramble=false, uvSecure=false, cpl=false, kDirect=true, stats=true;
    private static ArrayList<String> EventRegister, statsDesc;
    private static ArrayList<Integer> statsTime, statsCntr;
    private static int AUDIT_LOG_MAX=2, pcnt=100, wait=0, idxOF=0;
    private static UniCommand ucmd;
    public static ArrayList<UniFile> uFileArray;
    public static ArrayList<String>  uFileNames;
    public static ArrayList<String> missingFiles = new ArrayList<>();
    private static boolean sasl = false;
    private static String saslUser="", saslPass="",saslKey="";
    private static String batch_Path_File="", batchEXT=".ulog", batchTMP=".utmp", batchFile="";
    private static BufferedWriter bWriter=null;
    private static int logSIZE=0, logMAX=10000000, dbgCnt=0;

    private static long tFrom = 0, tTo = 0;
    private static long div = 100000;
    private static int laps = 0;


    public static boolean SetValues(String cfile) {

        Initialise(cfile);
        if (stopSW) return false;

        defaults  = "pcnt=" + pcnt + "\nwait=" + wait;

        if (protocol.equals(nof)) {
            kafkaCommons.uSendMessage("Database \"protocol\" parameter is missing or invalid.");
            System.exit(0);
        }

        if ((kafkaCommons.dbhost+kafkaCommons.dbpath+kafkaCommons.dbuser+kafkaCommons.dbpwd+kafkaCommons.dbSecure+kafkaCommons.minPool+kafkaCommons.maxPool+uvHome+rfHome).contains(nof)) {
            kafkaCommons.uSendMessage("U2 database connection parameters are incomplete.");
            if (!kafkaCommons.dbpwd.equals(nof)) kafkaCommons.dbpwd = "*********";
            kafkaCommons.uSendMessage("   dbhost : " + kafkaCommons.dbhost);
            kafkaCommons.uSendMessage("   dbpath : " + kafkaCommons.dbpath);
            kafkaCommons.uSendMessage("   dbacct : " + kafkaCommons.dbacct);
            kafkaCommons.uSendMessage("   dbuser : " + kafkaCommons.dbuser);
            kafkaCommons.uSendMessage("    dbpwd : " + kafkaCommons.dbpwd);
            kafkaCommons.uSendMessage(" dbSecure : " + kafkaCommons.dbSecure);
            kafkaCommons.uSendMessage("  minPool : " + kafkaCommons.minPool);
            kafkaCommons.uSendMessage("  maxPool : " + kafkaCommons.maxPool);
            kafkaCommons.uSendMessage("   uvHome : " + uvHome);
            kafkaCommons.uSendMessage("   rfHome : " + rfHome);
            System.exit(0);
        }

        if (!NamedCommon.sConnected) {
            NamedCommon.dbhost = kafkaCommons.dbhost;
            NamedCommon.dbpath = kafkaCommons.dbpath;
            NamedCommon.dbuser = kafkaCommons.dbuser;
            NamedCommon.passwd = kafkaCommons.dbpwd;
            NamedCommon.datAct = kafkaCommons.dbacct;
            NamedCommon.protocol=protocol;
            NamedCommon.CPL = cpl;
            NamedCommon.minPoolSize = kafkaCommons.minPool;
            NamedCommon.maxPoolSize = kafkaCommons.maxPool;
            NamedCommon.uSecure = uvSecure;

            SourceDB.SetDBtimeout(true);
            SourceDB.ConnectSourceDB();

            if (NamedCommon.sConnected) {
                kafkaCommons.uSendMessage("***");
                kafkaCommons.uSendMessage("*****************************************************************************************");
                kafkaCommons.uSendMessage("This process is connected as "+kafkaCommons.dbuser+" and will assume it has UV Admin privilleges");
                kafkaCommons.uSendMessage("*****************************************************************************************");
                kafkaCommons.uSendMessage("***");
            } else {
                return false;
            }
            try {
                ucmd = NamedCommon.uSession.command();
            } catch (UniSessionException e) {
                if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                uCommons.uSendMessage("ERROR: Cannot set UniCommand. "+e.getMessage());
                return false;
            }

//            EventRegister.clear();
            EventRegister = new ArrayList<>(Arrays.asList(register.split("\\,")));

            NamedCommon.MQgarbo.gc();

            if (!NamedCommon.ZERROR) {
                if (register.equals("")) {
                    kafkaCommons.uSendMessage("ERROR: No files are registered in conf/"+cfile);
                    System.exit(0);
                }
            }
        }

        register = "";
        defaults = "";
        return true;
    }

    private static void Initialise(String cfile) {

        Properties props = kafkaCommons.LoadProperties(cfile);
        if (kafkaCommons.KERROR) System.exit(0);

        stopSW    = GetProperty(props,"stop", "false").toLowerCase().equals("true");
        if (stopSW) return;

        register  = GetProperty(props, "register", "");
        kafkaCommons.dbhost    = GetProperty(props,"u2host", nof);
        kafkaCommons.dbpath    = GetProperty(props,"u2path", nof);
        kafkaCommons.dbacct    = GetProperty(props,"u2acct", nof);
        kafkaCommons.dbuser    = GetProperty(props,"u2user", nof);
        kafkaCommons.dbpwd     = GetProperty(props,"u2pass", nof);
        kafkaCommons.topic     = GetProperty(props, "topic", "uvhashStreams");
        uvHome    = GetProperty(props, "uvhome", nof);
        rfHome    = GetProperty(props, "rfhome", nof);
        kafkaCommons.dbSecure  = GetProperty(props,"secure", nof);
        kafkaCommons.minPool   = GetProperty(props,"minpool", nof);
        kafkaCommons.maxPool   = GetProperty(props,"maxpool", nof);
        protocol  = GetProperty(props,"protocol", nof);
        ENCR      = GetProperty(props,"encrypt", "false").toLowerCase().equals("true");
        verbose   = GetProperty(props,"verbose", "false").toLowerCase().equals("true");
        stats     = GetProperty(props,"showstats", "false").toLowerCase().equals("true");
        cpl       = GetProperty(props,"cpl", nof).toLowerCase().equals("true");
        uvSecure  = GetProperty(props,"secure", nof).toLowerCase().equals("true");
        kBrokers  = GetProperty(props, "brokers", nof);
        ClientID  = GetProperty(props, "clientid", "uvhashStreams")+NamedCommon.pid;

        try {
            AUDIT_LOG_MAX = Integer.valueOf(GetProperty(props,"maxlogs", "2"));
        } catch (NumberFormatException e) {
            kafkaCommons.uSendMessage("Properties: "+cfile+" is not set up correctly (maxlogs).");
            System.exit(0);
        }
        try {
            pcnt = Integer.valueOf(props.getProperty("proccnt", "1500"));
        } catch (NumberFormatException nfe) {
            pcnt = 1500;
        }
        try {
            wait = Integer.valueOf(props.getProperty("waitcnt", "15"));
        } catch (NumberFormatException nfe) {
            wait = 1500;
        }
        try {
            kafkaCommons.heartbeat = Integer.valueOf(props.getProperty("heartbeat", "100"));
        } catch (NumberFormatException nfe) {
            kafkaCommons.uSendMessage("HeartBeat value must be an integer > 100.");
            System.exit(0);
        }
        if (ENCR)  encSeed = uCipher.GetCipherKey();
        SUSPEND="sh -c\""+uvHome+"bin/audman -suspendlog @\"";
        CLEANUP="sh -c\""+uvHome+"bin/audman -clearlog @\"";
        RESUME="sh -c\""+uvHome+"bin/audman -resumelog @\"";

        // DANGER  - IF all are in suspendlog mode at the same time!!!
        // ANSWER  - Daudlog=file1,file2   -->   ring-fence two files
        namedFile = System.getProperty("audlog", "");
        if (namedFile.equals("")) namedFile = GetProperty(props, "audlog", "");

        // Should we build batches of items before sending to kafka?
        // tries to reduce impact of network latency (e.g. Kiwibank and AWS in Sydney)
        // batch_Path_File is the ENTIRE path and file, e.g.: /upl/data/ins/
        batch_Path_File = System.getProperty("batchfile", "");
        String bpfOride = GetProperty(props, "batchfile", nof);
        if (!bpfOride.equals(nof)) batch_Path_File = bpfOride;

        if (!batch_Path_File.equals("")) kDirect = false;

    }

    private static void CheckProperties(String cfile) {

        Properties props = kafkaCommons.LoadProperties(cfile);
        if (kafkaCommons.KERROR) System.exit(0);

        stopSW    = GetProperty(props,"stop", "false").toLowerCase().equals("true");
        if (stopSW) return;

        batch_Path_File = System.getProperty("batchfile", "");
        String bpfOride = GetProperty(props, "batchfile", nof);
        if (!bpfOride.equals(nof)) batch_Path_File = bpfOride;
        if (!batch_Path_File.equals("")) kDirect  = false;

        props = null;
        bpfOride = null;
    }

    private static String GetProperty(Properties props, String key, String defolt) {
        String ans = props.getProperty(key, defolt);
        if (ans.startsWith("ENC(")) {
            ans = ans.substring(4, ans.length());
            ans = ans.substring(0, ans.length() - 1);
            ans = uCipher.Decrypt(ans);
        }
        return ans;
    }

    private static void Process(String cFile) throws UniFileException {
        //
        // IF you want 1 producer per Audlog - use the namedFile
        // otherwise it will automatically step through each logfile (lf)
        //
        UniString rid, uvStr;
        UniFile datFile;

        String evt, act, fil, iid="", evtLoc, adminCmd, qfl="", sRec, lFile, lfNbr;
        ArrayList<String> audLogs = new ArrayList<>();
        statsDesc = new ArrayList<>();
        statsTime = new ArrayList<>();
        statsCntr = new ArrayList<>();
        uFileArray = new ArrayList<>();
        uFileNames = new ArrayList<>();

        if (namedFile.equals("")) {
            for (int i=1 ; i<= AUDIT_LOG_MAX ; i++) { audLogs.add("&AUDLOG" + i + "&"); }
        } else {
            audLogs  = new ArrayList<String>(Arrays.asList(namedFile.split("\\,")));
        }

        // loop through AUDLOGS --------------------------------
        long startLoop = 0;
        long endLoop = 0;
        int eol = audLogs.size();
        for (int lf=0; lf < eol ; lf++) {
            lFile = audLogs.get(lf);
            lfNbr = lFile.replaceAll("[^\\d]", "");

            if (missingFiles.indexOf(lFile) >= 0) continue;

            System.out.println(" ");
            System.out.println(" ");
            uCommons.uSendMessage("Processing: "+lFile + " ------------------------------------------------");
            if (verbose) System.out.println(" ");

            if (NamedCommon.U2File != null) {
                if (NamedCommon.U2File.isOpen()) NamedCommon.U2File.close();
                NamedCommon.U2File = null;
            }

            NamedCommon.U2File = OpenLogFile(lFile);

            if (NamedCommon.ZERROR) {
                missingFiles.add(lFile);
                NamedCommon.ZERROR = false;
                continue;
            }
            if (NamedCommon.U2File == null) {
                uCommons.uSendMessage("  "+lFile+" does not exist.");
                missingFiles.add(lFile);
                continue;
            }
            uCommons.uSendMessage("      OPEN >>  " + NamedCommon.U2File.getFileName());

            adminCmd = SUSPEND.replaceAll("\\@", lfNbr);
            uCommons.uSendMessage("   SUSPEND >>  " + adminCmd);
            try {
                uvExec(adminCmd);
            } catch (UniCommandException e) {
                uCommons.uSendMessage("ERROR: " + e.getMessage());
                return;
            }

            adminCmd = "SELECT "+NamedCommon.U2File.getFileName();
            uCommons.uSendMessage("    SELECT >>  " + adminCmd);

            try {
                NamedCommon.uSelect = NamedCommon.uSession.selectList(0);
                NamedCommon.uSelect.select(NamedCommon.U2File);
            } catch (UniSessionException e) {
                uCommons.uSendMessage("UniSession ERROR: " + e.getMessage());
                return;
            } catch (UniSelectListException e) {
                uCommons.uSendMessage("UniSelect ERROR: " + e.getMessage());
                return;
            }

            uCommons.uSendMessage("           >>  ***************************************************************************");

            boolean eof=false, proceed=false;
            int rCnt=0, pCnt=0, lCnt=0, fPos=0;

            // MAIN delta collection loop -------------------------------
            startLoop = System.currentTimeMillis();
            tFrom = System.currentTimeMillis();
            while (!eof) {
                dbgCnt = 0;
                try {
                    rid = NamedCommon.uSelect.next();

                    DoCounter("uSelect.next()");

                    if (NamedCommon.uSession.selectList(0).isLastRecordRead()) { eof=true; continue; }
                    if (rid.toString().isEmpty()) { eof=true; continue; }

                    try {
                        NamedCommon.U2File.setRecordID(rid);
                        uvStr = NamedCommon.U2File.read();
                    } catch (UniFileException e) {
                        continue;
                    }
                    if (uvStr == null) continue;
                    // ---------------------------------------------------------------

                    DoCounter("AudLog.read()");

                    NamedCommon.dynRec = null;
                    NamedCommon.dynRec = new UniDynArray(uvStr);
                    uvStr = null;

                    rCnt++;
                    pCnt++;
                    if (pCnt >= pcnt) {
                        uCommons.uSendMessage("     ... runStat: " + rCnt + " events checked. " + lCnt + " events logged.");
                        pCnt=0;
                    }

                    evt = String.valueOf(NamedCommon.dynRec.extract(1));
                    if (evt.equals("")) { continue; }

                    act = String.valueOf(NamedCommon.dynRec.extract(4));
                    fil = String.valueOf(NamedCommon.dynRec.extract(7));
                    iid = String.valueOf(NamedCommon.dynRec.extract(8));

                    String[] tmp;
                    if (act.contains(NamedCommon.slash)) {
                        tmp = act.split(NamedCommon.slash);
                        if (tmp.length < 1) continue;
                        act = tmp[tmp.length - 1];
                        tmp = null;
                    }

                    if (fil.contains(NamedCommon.slash)) {
                        tmp = fil.split(NamedCommon.slash);
                        fil = tmp[tmp.length - 1];
                        if (tmp.length > 2) act = tmp[tmp.length - 2];
                        tmp = null;
                    }

                    NamedCommon.dynRec = null;
                    evtLoc = act + " " + fil;
                    if (EventRegister.indexOf(evtLoc) < 0) {
                        evtLoc = act+" *";
                        if (EventRegister.indexOf(evtLoc) < 0) { continue; }
                    }

                    qfl = "uplqf_"+act+"_"+fil;
                    fPos= uFileNames.indexOf(qfl);
                    if (fPos >= 0) {
                        datFile = uFileArray.get(fPos);
                    } else {
                        NamedCommon.dynRec = null;
                        NamedCommon.dynRec = new UniDynArray();
                        NamedCommon.dynRec.insert(1, 0, 0, "Q");
                        NamedCommon.dynRec.insert(2, 0, 0, act);
                        NamedCommon.dynRec.insert(3, 0, 0, fil);
                        try {
                            NamedCommon.VOC.setRecordID(qfl);
                            NamedCommon.VOC.setRecord(NamedCommon.dynRec);
                            NamedCommon.VOC.write();
                            NamedCommon.dynRec = null;
                        } catch (UniFileException e) {
                            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                            uCommons.uSendMessage("VOC QFile ERROR: " + e.getMessage());
                            NamedCommon.ZERROR = true;
                            NamedCommon.dynRec = null;
                            return;
                        }
                        datFile = NamedCommon.uSession.openFile(qfl);
                        uFileNames.add(qfl);
                        uFileArray.add(datFile);
                    }

                    DoCounter("DataFile.open(qfl)");

                    uvStr = datFile.read(iid);
                    NamedCommon.dynRec = null;
                    NamedCommon.dynRec = new UniDynArray(uvStr);
                    uvStr = null;
                    sRec = uCommons.UV2SQLRec(null, NamedCommon.dynRec);
                    if (ENCR) sRec = uCipher.v2Scramble(uCipher.keyBoard, sRec, encSeed);

                    DoCounter("DataFile.read(iid)");

                    LogThisEvent(rid.toString(), act, fil, iid, sRec);
                    lCnt++;

                    DoCounter("LogTheEvent");

                } catch (UniSelectListException e) {
                    uCommons.uSendMessage("ERROR: Select List exception: " + e.getMessage());
                } catch (UniSessionException e) {
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                } catch (UniFileException e) {
                    uCommons.uSendMessage("ERROR: UniSession exception: ["+qfl+"] "+iid.toString()+"  : " + e.getMessage());
                }

                if (NamedCommon.ConnectionError) eof = true;
                NamedCommon.dynRec = null;
                datFile=null;
                if (NamedCommon.ZERROR) break;
            }

            uCommons.uSendMessage("     ... runStat: " + rCnt + " events checked. " + lCnt + " events logged");
            uCommons.uSendMessage("     Done. -------------------------------------------------------------------------------");

            endLoop = System.currentTimeMillis();

            if (stats) {
                int totTime, totInst, totMS=0;
                double average;
                System.out.println(" ");
                uCommons.uSendMessage("           Statistics:-");
                uCommons.uSendMessage("           " +
                        uCommons.RightHash("Action", 20) + "  " +
                        uCommons.RightHash("totMS", 10) + "  " +
                        uCommons.RightHash("tot#", 10) + "  " +
                        uCommons.RightHash("avMS", 10));
                for (int i = 0; i < statsDesc.size(); i++) {
                    totTime = statsTime.get(i);
                    totInst = statsCntr.get(i);
                    average = totTime / totInst;
                    totMS = totMS + totTime;
                    uCommons.uSendMessage("           " +
                            uCommons.RightHash(statsDesc.get(i), 20) + "  " +
                            uCommons.RightHash(String.valueOf(totTime), 10) + "  " +
                            uCommons.RightHash(String.valueOf(totInst), 10) + "  " +
                            uCommons.RightHash(String.valueOf(average), 10));
                }
                uCommons.uSendMessage(uCommons.RightHash("Total Milliseconds : ", 33) +
                        uCommons.RightHash(String.valueOf(totMS), 10));
                System.out.println(" ");
                statsDesc.clear();
                statsTime.clear();
                statsCntr.clear();
            }

            double thruTime = endLoop - startLoop;
            thruTime = thruTime / 1000;
            double thruPut = lCnt / thruTime;
            uCommons.uSendMessage("     Through-put : " + lCnt + " deltas in " + thruTime + " seconds : " + thruPut + " / second.");

            if (NamedCommon.ZERROR) {
                uCommons.uSendMessage("   .) Fatal error condition. Must fix ASAP.");
            } else {
                // -------------------------
                // Clean up the audit logs
                // -------------------------
                adminCmd = "";
                try {
                    adminCmd = CLEANUP.replaceAll("\\@", lfNbr);
                    uCommons.uSendMessage("   CLEANUP >>  " + adminCmd);
                    uvExec(adminCmd);
                    uCommons.Sleep(3);
                    // --------------------------------------------------------------------
                    adminCmd = RESUME.replaceAll("\\@", lfNbr);
                    uCommons.uSendMessage("   RESUME  >>  " + adminCmd);
                    uvExec(adminCmd);
                    uCommons.Sleep(3);
                    if (verbose)
                        uCommons.uSendMessage("   .) ******************************************************************");
                } catch (UniCommandException e) {
                    uCommons.uSendMessage("ERROR: UniCommand exception: " + e.getMessage());
                    return;
                }
            }
            CheckProperties(cFile);
            if(stopSW) break;
        }

        if (NamedCommon.sConnected) { SourceDB.DisconnectSourceDB(); }
        for (int ln=0; ln < 5; ln ++) { System.out.println(" "); }
        System.out.println("Automated reset -----------------------------------------------------------------------");
        System.out.println(purpose + "[" + kafkaCommons.topic + "]");
        System.out.println("---------------------------------------------------------------------------------------");
        System.out.println(" ");
        if (bWriter != null) {
            CloseLog();
            if (NamedCommon.ZERROR) return;
        }
    }

    private static void DoCounter(String msg) {
        tTo = System.currentTimeMillis();
        laps = (int) (tTo - tFrom);
        if (!stats) {
            if (verbose) uCommons.uSendMessage(dbgCnt + "  " + laps + " " + msg);
        } else {
            idxOF = statsDesc.indexOf(msg);
            if (idxOF < 0) {
                statsDesc.add(msg);
                statsTime.add(laps);
                statsCntr.add(1);
            } else {
                statsTime.set(idxOF, statsTime.get(idxOF)+laps);
                statsCntr.set(idxOF, statsCntr.get(idxOF)+1);
            }
        }
        dbgCnt++;
        tFrom = tTo;
    }

    private static void uvExec(String adminCmd) throws UniCommandException {
        if (verbose) uCommons.uSendMessage("   .) ******************************************************************");
        if (verbose) uCommons.uSendMessage("   .) command: " + adminCmd);
        ucmd.setCommand(adminCmd);
        ucmd.exec();
        while (ucmd.status() == UniObjectsTokens.UVS_REPLY) { ucmd.reply("Y"); }
        if (verbose) {
            String[] output = ucmd.response().split("\\r?\\n");
            for (int i = 0; i < output.length; i++) {
                if (output[i].length() < 1) continue;
                uCommons.uSendMessage("   .>> output: " + output[i]);
            }
        }
    }

    private static void LogThisEvent(String dts, String act, String fil, String iid, String rec) {

        String src = NamedCommon.dbhost, log = "", lid="hashed";
        String tdFile = act+" "+fil+" "+iid;
        String lfl=NamedCommon.U2File.getFileName();
        String kafKey = "[" + act + "].[" + fil + "].[" + iid + "]";
        boolean kPass;

        String dat = uCommons.FieldOf(dts, "\\.", 1);
        dat = uCommons.oconvD(dat, "D4-");
        String dd = uCommons.FieldOf(dat, "-", 1);
        String mm = uCommons.FieldOf(dat, "-", 2);
        String yy = uCommons.FieldOf(dat, "-", 3);
        String dte = yy+mm+dd;

        String tym = uCommons.FieldOf(dts, "\\.", 2);
        tym = uCommons.oconvM(tym, "MTS");
        String tme = tym.replace(":", "");

        if (verbose) {
            uCommons.uSendMessage(dat + " " + tym + " " + src + " " + act + " " + fil + " " + iid );
        }

        String iss = ClientID;
        if (ENCR) {
            if (scramble) {
                //
                // rediculously secure !! Scrambled AND encrypted
                //
                String encSeed = uCipher.GetCipherKey();
                fil = uCipher.v2Scramble(uCipher.keyBoard, fil, encSeed);
                iid = uCipher.v2Scramble(uCipher.keyBoard, iid, encSeed);
                rec = uCipher.v2Scramble(uCipher.keyBoard, rec, encSeed);
                act = uCipher.v2Scramble(uCipher.keyBoard, act, encSeed);
                src = uCipher.v2Scramble(uCipher.keyBoard, src, encSeed);
                dte = uCipher.v2Scramble(uCipher.keyBoard, uCommons.oconvD(dte, dCnv), encSeed);
                tme = uCipher.v2Scramble(uCipher.keyBoard, uCommons.oconvM(tme, tCnv), encSeed);
                iss = uCipher.v2Scramble(uCipher.keyBoard, iss, encSeed);
                lid = uCipher.v2Scramble(uCipher.keyBoard, lid, encSeed);
                lfl = uCipher.v2Scramble(uCipher.keyBoard, lfl, encSeed);
                passport = encSeed;
            } else {
                passport = uCipher.GetEncKey();
            }
            // jEncrypt uses libraries from the Java 11 jvm.
            // Most rFuel customers are on Java 8
            // ----------------------------------------------
        }

        JSONObject jObj = new JSONObject();
        jObj.put("passport", passport+"~"+encSeed);
        jObj.put("sourceinstance", src);
        jObj.put("sourceaccount", act);
        jObj.put("date", dte);
        jObj.put("time", tme);
        jObj.put("file", fil);
        jObj.put("item", iid);
        jObj.put("record", rec);

        log = jObj.toString();
//        if (ENCR)  log = uCipher.jEncrypt(log);   // Need JRE 11+
        jObj= null;

        if (kDirect) {
            if (kProducer.GetBrokers().equals("")) {
                if (sasl) kProducer.SetSASL(true, saslUser, saslPass, saslKey);
                kProducer.SetClientID(ClientID);
                kProducer.SetTopic(kafkaCommons.topic);
                kProducer.SetBroker(kBrokers);
            }
            kPass = kProducer.kSend(kafkaCommons.topic, kafKey, log);
            if (!kPass) kafkaCommons.uSendMessage("FAIlED to log " + tdFile);
        } else {
            AppendBatch(log);
        }

        src=""; log=""; tdFile=""; dte=""; tme=""; rec=""; act=""; src="";
    }

    private static UniFile OpenLogFile(String lfile) {
        String qfile = lfile.replaceAll("\\&", "");
        qfile = "uplqf_"+qfile;
        UniString uvstg;

        // Does the Q-Pointer exist ?
        try {
            uvstg = NamedCommon.VOC.read(qfile);
        } catch (UniFileException ufe) {
            uvstg = null;
        }
        boolean reset=false;
        if (uvstg == null) {
            reset=true;
        } else {
            UniDynArray uvRec = new UniDynArray(uvstg);
            if (!uvRec.extract(2).toString().equals("UV")) reset = true;
            if (!uvRec.extract(3).toString().equals(lfile)) reset = true;
            uvRec = null;
        }
        if (reset) {
            UniDynArray uvRec = new UniDynArray();
            // Create the Q-Pointer
            uvRec.insert(1, 0, 0, "Q");
            uvRec.insert(2, 0, 0, "UV");
            uvRec.insert(3, 0, 0, lfile);
            try {
                NamedCommon.VOC.setRecordID(qfile);
                NamedCommon.VOC.setRecord(uvRec);
                NamedCommon.VOC.write();
                uvRec = null;
            } catch (UniFileException e) {
                uCommons.uSendMessage("VOC QFile ERROR: " + e.getMessage());
                NamedCommon.ZERROR = true;
                uvRec = null;
                return null;
            }
        }
        // Open the Q-Pointer
        UniFile uf;
        try {
            uf = NamedCommon.uSession.open(qfile);
        } catch (UniSessionException e) {
            uf = null;
        }
        // Return the Q-Pointer
        uvstg = null;
        return uf;
    }

    private static void AppendBatch(String log) {
        String fname;
        File bFile;
        while (bWriter == null) {
            fname = GetBatchFileName(batch_Path_File, batchTMP);
            batchFile = fname + batchTMP;
            bFile = new File(batchFile);
            try {
                bWriter = new BufferedWriter(new FileWriter(bFile, true)); // append mode
            } catch (IOException e) {
                bWriter = null;
            }
        }
        try {
            bWriter.write(log);
            bWriter.newLine();
            bWriter.flush();
            logSIZE = logSIZE + log.length();
            if (logSIZE > logMAX) CloseLog();
        } catch (IOException e) {
            uCommons.uSendMessage(e.getMessage());
            uCommons.uSendMessage("CANNOT continue.");
            NamedCommon.ZERROR = true;
        }
    }

    private static void CloseLog() {
        try {
            bWriter.close();
            bWriter = null;
            String newFile = GetBatchFileName(batch_Path_File, batchEXT);
            uCommons.RenameFile(batchFile, newFile + batchEXT);
            logSIZE = 0;
        } catch (IOException e) {
            uCommons.uSendMessage(e.getMessage());
            NamedCommon.ZERROR = true;
        }
    }

    private static String GetBatchFileName(String file, String extn) {
        String infile = file;
        File fr = null;
        int ctr = 0;
        while (true) {
            ctr++;
            infile = file + "_" + ctr;
            fr = null;
            fr = new File(infile + extn);
            if (fr.exists()) {
                fr = null;
                continue;
            } else {
                break;
            }
        }
        fr = null;
        return infile;
    }

    public static void main (String[] args) throws UniFileException {

        if (!kafkaCommons.isLicenced()) System.exit(1);

        passport = (License.domain+NamedCommon.loAplha).substring(0,16);
        uCipher.domain = License.domain;

        String cFile="";
        if (args.length > 0) {
            cFile = args[0];
        } else {
            cFile = System.getProperty("conf", "");
        }
        if (cFile.equals("")) {
            uCommons.uSendMessage("No configuration property file provided. Stopping now.");
            System.exit(0);
        }

        uCommons.uSendMessage("Using: " + cFile);
        if (!SetValues(cFile)) NamedCommon.ZERROR=true;

        System.out.println(" ");
        System.out.println(" ");
        uCommons.uSendMessage("uvhashStreams()  starting...");
        System.out.println(" ");
        uCommons.uSendMessage("-------------------------------------------------------------");
        uCommons.uSendMessage(purpose + "[" + kafkaCommons.topic + "]");
        uCommons.uSendMessage("-------------------------------------------------------------");
        System.out.println(" ");
        uCommons.uSendMessage("<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>");

        while (!NamedCommon.ZERROR) {
            if (NamedCommon.sConnected) Process(cFile);
            if (!SetValues(cFile)) break;
        }
        if (bWriter != null) CloseLog();
        uCommons.uSendMessage("Completed processing. Stopping now.");
        System.exit(0);
    }

}
