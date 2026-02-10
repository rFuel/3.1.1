package com.unilibre.kafka.dev;


// ----------------------[AUDIT_LOG_TYPE 2 : sequential file]-------------------------------------------------
// -----------------------------------------------------------------------------------------------------------

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniFileException;
import asjava.uniobjects.UniSequentialFile;
import asjava.uniobjects.UniSessionException;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.u2Commons;
import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kProducer;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class uvaudStreams {

    private static String audMAST = "/usr/uv/uvaudd.log";
    private static ArrayList<String> done = new ArrayList<>();
    private static boolean stopSW=false, ENCR=false, AVRO=false, rsyncSw=false, uplDEV=false, shown=false, seqlogs=false;
    private static String cfile, logdir, matchStr, rsync, ClientID="", uvHome;
    private static String register, brokers, topic, dbhost, dbpath, dbuser, dbpwd, dbSecure, minPool, maxPool, u2type, u2acct;
    private static String realip, realdb, realus, realac, dbtype, protocol;
    private static String dCnv="D4-", tCnv="MTS";
    private static String logfile="", logid="", lastFile="", ctlFile="uStreams.Control";

    private static int lno=0, logCnt=0;
    private static UniString uvstr;
    private static UniFile uplAAUDLOG1, uplAUDLOG2;
    private static String dts, evt, act, fil, iid, evtSrc, evtHome, key, uvRecString;
    private static String qfile = "upl_qfSTREAM_" + NamedCommon.pid;
    private static String audLogger = "uvAUDLOGGER", audFile = "uvHOME", uvAudLog = "uvaudd.log";
    private static String logID="", cmd = "SSELECT VOC LIKE \"&AUDLOG...&\"";

    private static final String WRITE_EVENTS = "DAT.BASIC.WRITE";
    private static int heartbeat=0, pause=0, lastLine=99;
    private static ArrayList<String> DoneFiles = new ArrayList<>();
    private static ArrayList<String> IgnoredFiles = new ArrayList<>();
    private static ArrayList<String> EVT_REGISTER = new ArrayList<>();
    private static ArrayList<Integer> FileSizes = new ArrayList<Integer>();
    private static ArrayList<String>  matchFiles = new ArrayList<>();;

    public static void main(String[] args) throws IOException {

        if (NamedCommon.BaseCamp.contains("/home/andy")) uplDEV = true;
        cfile    = System.getProperty("conf", "kFetch.properties");
        NamedCommon.logLevel = "5";
        Initialise();
        ctlFile = NamedCommon.BaseCamp+NamedCommon.slash+"conf"+NamedCommon.slash+ctlFile;
        GetControls();

        while (!u2Commons.CheckU2Controls().equals("<<FAIL>>")) {
            if (rsyncSw) Rsync();
            if (seqlogs) {
                GetAuditlogs();
            } else {
                uCommons.uSendMessage(".");
                uCommons.uSendMessage("====================================================================");
                uCommons.uSendMessage("Streaming data events from a U2 database reflects U2 Audit Logging; ");
                uCommons.uSendMessage("   a) events from Sequential files, OR");
                uCommons.uSendMessage("   b) events from Hashed files");
                uCommons.uSendMessage("This process needs logging type (a)");
                uCommons.uSendMessage("Your Audit logging is type (b).");
                uCommons.uSendMessage("Use uvhashStreams instead of uvaudStreams to stream your data events.");
                uCommons.uSendMessage("=====================================================================");
                uCommons.uSendMessage(".");
                System.exit(0);
            }
            ProcessLogs();
            RemoveProcessedLogs();
            Initialise();
            uCommons.Sleep(3);
            UpdateControls();
        }
        System.exit(0);
    }

    private static void UpdateControls() {
        String ctlRec="";
        for (int i=0 ; i < DoneFiles.size() ; i++) {
            ctlRec += DoneFiles.get(i)+"\t"+FileSizes.get(i)+"\n";
        }
        uCommons.WriteDiskRecord(ctlFile, ctlRec);
    }

    private static void GetControls() {
        DoneFiles.clear();
        FileSizes.clear();
        String ctl = uCommons.ReadDiskRecord(ctlFile);
        if (ctl.equals("")) {
            NamedCommon.ZERROR=false;
            NamedCommon.Zmessage="";
            uCommons.uSendMessage("Setting up for first-time run. Ignore the message above.");
            return;
        }
        String[] parts = ctl.split("\\r?\\n");
        String[] lpart = new String[5];
        for (int i=0 ; i<parts.length ; i++) {
            lpart = parts[i].split("\t");
            DoneFiles.add(lpart[0]);
            if (lpart.length > 0) {
                FileSizes.add(Integer.valueOf(lpart[1]));
            } else {
                FileSizes.add(0);
            }
            UpdateControls();
        }
        parts = null;
        lpart = null;
    }

    private static void Initialise() {
        if (uplDEV) NamedCommon.BaseCamp = NamedCommon.DevCentre;
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);

        runProps = uCommons.LoadProperties(cfile);
        if (NamedCommon.ZERROR) System.exit(0);

        SetUp(runProps);
        NamedCommon.slash = "/";

        logdir   = NamedCommon.BaseCamp + System.getProperty("logdir", "/dbaudlogs");

        switch (NamedCommon.protocol) {
            case "u2cs":
                matchStr = System.getProperty("matchstr", "uvaud");
                break;
            case "real":
                matchStr = System.getProperty("matchstr", "CLOG");
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
                return;
            default:
                uCommons.uSendMessage("FATAL: protocol not defined in "+cfile);
                System.exit(1);
        }

        if (!GoodToGo()) System.exit(1);
    }

    private static void ProcessLogs() {

        int fPos, lastLn;
        if (matchFiles.size() < 0) return;
        for (int mf=0 ; mf < matchFiles.size() ; mf++) {
            if (matchFiles.get(mf) == null) continue;
            logfile = matchFiles.get(mf);
            if (logfile.equals("")) continue;

            fPos = DoneFiles.indexOf(logfile);

            if (fPos < 0) {
                lastLn = 0;
                DoneFiles.add(logfile);
                FileSizes.add(lastLn);
                fPos = DoneFiles.indexOf(logfile);
            } else {
                lastLn = FileSizes.get(fPos);
            }

            lastLn = ProcessU2Log(logfile, lastLn);
            FileSizes.set(fPos, lastLn);
        }
    }

    private static int ProcessU2Log(String fqfname, int fromPos) {
        String[] fParts = fqfname.split(NamedCommon.slash);
        logfile = fParts[fParts.length-1];
        fParts= null;

        FileInputStream inputStream = null;
        Scanner sc = null;
        int lno=0;
        String line, rtnString, thisSrc;

        dts=""; evt=""; act=""; fil=""; iid=""; key=""; uvRecString=""; logID="";

        try {
            UniSequentialFile LogFile = NamedCommon.uSession.openSeq(evtSrc, logfile, false);
        } catch (UniSessionException e) {
            uCommons.uSendMessage("OpenSeq ("+evtSrc+logfile+") ERROR: " + e.getMessage());
            NamedCommon.ZERROR = true;
            return 0;
        }

        logfile = NamedCommon.BaseCamp + NamedCommon.slash +"dbaudlogs/"+logfile;
        if (!logfile.equals(lastFile)) {
            uCommons.uSendMessage("============================================================");
            uCommons.uSendMessage("Inspecting: " + logfile);
            lastFile = logfile;
            shown=false;
        }

        if (fromPos != lastLine) {
            uCommons.uSendMessage("  restarting from line " + fromPos);
            uCommons.uSendMessage("------------------------------------------------------------");
            lastLine = fromPos;
            shown=false;
        } else if (!shown) {
            uCommons.uSendMessage("  waiting for data events after line " + fromPos);
            shown=true;
        }
        System.exit(0);
        try {
            inputStream = new FileInputStream(logfile);
            sc = new Scanner(inputStream, "UTF-8");
            while (sc.hasNextLine()) {
                lno++;
                line = sc.nextLine();
                if (lno <= fromPos) continue;

                if (!line.contains(WRITE_EVENTS)) continue;
                UniDynArray logRec = BuildUvRec(line);

                dts = String.valueOf(logRec.extract(1));
                evt = String.valueOf(logRec.extract(2));
                act = String.valueOf(logRec.extract(7));
                fil = String.valueOf(logRec.extract(10));
                iid = String.valueOf(logRec.extract(11));
                logRec = null;

                if (evt.equals("")) continue;
                if (act.equals("")) continue;
                if (fil.equals("")) continue;
                if (iid.equals("")) continue;
                key = dts;

                if (!WRITE_EVENTS.contains(evt) && !evt.contains(WRITE_EVENTS)) continue;
                if (fil.contains(NamedCommon.slash)) {
                    fParts = fil.split(NamedCommon.slash);
                    fil = fParts[fParts.length-1];
                    act = fParts[fParts.length-2];
                    fParts = null;
                }
                if (fil.trim().equals("")) continue;

                if (act.contains(NamedCommon.slash)) {
                    fParts = act.split(NamedCommon.slash);
                    act = fParts[fParts.length-1];
                    fParts = null;
                }
                if (act.trim().equals("")) continue;

                thisSrc = act + " " + fil;                               // e.g. LIVE CUSTOMER
                if (EVT_REGISTER.indexOf(thisSrc) < 0) {
                    thisSrc = act + " *";                                // e.g. LIVE *
                    if (EVT_REGISTER.indexOf(thisSrc) < 0) continue;
                }

                if (fil.contains(act)) {
                    fil = fil.replace(act, "");
                    if (fil.startsWith(NamedCommon.slash)) fil = fil.substring(1, fil.length());
                }

                uCommons.uSendMessage("Line("+lno+") Logging: "+act+" "+fil+" "+iid);

                key = dts;
                UniDynArray uvRec = new UniDynArray();
                uvRec.insert(1,0,0, "Q");
                uvRec.insert(2,0,0, act);
                uvRec.insert(3,0,0, fil);
                try {
                    NamedCommon.VOC.setRecordID(qfile);
                    NamedCommon.VOC.setRecord(uvRec);
                    NamedCommon.VOC.write();
                    uvRec=null;
                } catch (UniFileException e) {
                    uvRec=null;
                    uCommons.uSendMessage("VOC QFile ERROR: " + e.getMessage());
                    NamedCommon.ZERROR = true;
                    return lno;
                }

                logid = String.valueOf(lno);
                rtnString = u2Commons.ReadAnItem(qfile, iid, "", "", "");
                LogThisEvent(key, act, fil, iid, rtnString);
                rtnString="";
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                uCommons.uSendMessage("Scanner ERRORL: " + sc.ioException().getMessage());
                return lno;
            }
        } catch (FileNotFoundException e) {
            uCommons.uSendMessage("FileInputStream ERROR: " + e.getMessage());
            return lno;
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    uCommons.uSendMessage("FileInputStream close ERROR: " + e.getMessage());
                }
            }
            if (sc != null) sc.close();
        }
        uCommons.uSendMessage("...... End-of-logfile at line "+lno);
        return lno;
    }

    private static void LogThisEvent(String dts, String act, String fil, String iid, String rec) {

        if (uplDEV) {

            logCnt++;
        }
        String passport = "", src = NamedCommon.dbhost, log = "";
        boolean kPass;
        String tdFile = act+" "+fil+" "+iid;

        String dte = uCommons.FieldOf(dts, "\\.", 1);
        String tme = uCommons.FieldOf(dts, "\\.", 2);
        if (ENCR) {
            String encSeed = uCipher.GetCipherKey();
            fil = uCipher.v2Scramble(uCipher.keyBoard, fil, encSeed);
            iid = uCipher.v2Scramble(uCipher.keyBoard, iid, encSeed);
            rec = uCipher.v2Scramble(uCipher.keyBoard, rec, encSeed);
            act = uCipher.v2Scramble(uCipher.keyBoard, act, encSeed);
            src = uCipher.v2Scramble(uCipher.keyBoard, src, encSeed);
            passport = encSeed;
        }

        JSONObject jObj = new JSONObject();
        jObj.put("logfile", logfile);
        jObj.put("logid", logid);
        jObj.put("date", uCommons.oconvD(dte, dCnv));
        jObj.put("time", uCommons.oconvM(tme, tCnv));
        jObj.put("passport", passport);
        jObj.put("issuer", ClientID);
        jObj.put("sourcehost", src);
        jObj.put("sourceacct", act);
        jObj.put("file", fil);
        jObj.put("item", iid);
        jObj.put("record", rec);

        log = jObj.toString();
        jObj= null;

        kPass = kProducer.kSend(topic, "key", log);

        if (!kPass) {
            uCommons.uSendMessage("FAIlED to log " + tdFile);
        }
        passport=""; src=""; log=""; tdFile=""; dte=""; tme="";
    }

    private static void SetUp(Properties props) {
        String nof = "NotInConfigFile";
        register = props.getProperty("register", "");
        brokers  = props.getProperty("brokers", "");
        topic    = props.getProperty("topic", nof);
        dbhost   = props.getProperty("u2host", nof);
        dbpath   = props.getProperty("u2path", nof);
        dbuser   = props.getProperty("u2user", nof);
        dbpwd    = props.getProperty("u2pass", nof);
        u2type   = props.getProperty("u2type", nof);
        u2acct   = props.getProperty("u2acct", nof);
        dbSecure = props.getProperty("secure", nof);
        minPool  = props.getProperty("minpoolsize", nof);
        maxPool  = props.getProperty("maxpoolsize", nof);
        realip   = props.getProperty("realhost", nof);
        realdb   = props.getProperty("realdb", nof);
        realus   = props.getProperty("realuser", nof);
        realac   = props.getProperty("realac", nof);
        dbtype   = props.getProperty("dbtype", nof);
        protocol = props.getProperty("protocol", nof);
        evtSrc   = props.getProperty("source", nof);
        evtHome  = props.getProperty("home", nof);
        //---------------------------------------------------------
        uvHome   = props.getProperty("uvhome", nof);
        seqlogs  = props.getProperty("seqlogs", nof).toLowerCase().equals("true");
        //---------------------------------------------------------
        ENCR     = props.getProperty("encrypt", "false").toLowerCase().equals("true");
        ClientID = props.getProperty("clientid", "uvaudStreams");
        ClientID+= "_" + NamedCommon.pid;

        if (brokers.equals("")) {
            uCommons.uSendMessage("No Kafka broker(s) found.");
            System.exit(0);
        }
        if (topic.equals("")) {
            uCommons.uSendMessage("No Kafka topic found.");
            System.exit(0);
        }
        NamedCommon.protocol = protocol;

        if (protocol.toLowerCase().equals("u2cs")) {
            if ((dbhost+dbpath+dbuser+dbpwd+dbSecure+minPool+maxPool+evtSrc+evtHome).contains(nof)) {
                uCommons.uSendMessage("UV database connection parameters are invalid.");
                System.exit(0);
            }
            NamedCommon.dbhost = dbhost;
            NamedCommon.dbuser = dbuser;
            NamedCommon.passwd = dbpwd;
            NamedCommon.dbpath = dbpath;
            NamedCommon.datAct = u2acct;

            NamedCommon.realhost = "";
            NamedCommon.realdb   = "";
            NamedCommon.realuser = "";
            NamedCommon.realac   = "";

            NamedCommon.databaseType = u2type;
        } else if (protocol.toLowerCase().equals("real")) {
            if ((realip + realdb + realus + realac + dbtype).contains(nof)) {
                uCommons.uSendMessage("RT database connection parameters are invalid.");
                System.exit(0);
            }
            NamedCommon.dbhost = "";
            NamedCommon.dbuser = "";
            NamedCommon.passwd = "";
            NamedCommon.dbpath = "";
            NamedCommon.datAct = "";

            NamedCommon.realhost = realip;
            NamedCommon.realdb   = realdb;
            NamedCommon.realuser = realus;
            NamedCommon.realac   = realac;

            NamedCommon.databaseType = dbtype;
        } else {
            uCommons.uSendMessage("No database protocol defined.");
            System.exit(0);
        }

        try {
            pause = Integer.valueOf(props.getProperty("pause", "250"));
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("Pause value must be an integer > 100.");
            System.exit(0);
        }

        try {
            heartbeat = Integer.valueOf(props.getProperty("heartbeat", "100"));
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("HeartBeat value must be an integer > 100.");
            System.exit(0);
        }

        stopSW = (props.getProperty("stop", "").toLowerCase().equals("true"));
        EVT_REGISTER = new ArrayList<>(Arrays.asList(register.split("\\,")));

//        String fileRegister = ReadAnItem("BP.UPL", "register", "", "", "");
//        UniDynArray uReg = uCommons.SQL2UVRec(fileRegister);
//        EVT_REGISTER = new ArrayList<String>();
//        int eoi = uReg.dcount();
//        for (int a=1 ; a <= eoi ; a++) { EVT_REGISTER.add(String.valueOf(uReg.extract(a))); }
//        uReg = null;
//        fileRegister = "";

        props.clear();
        kProducer.SetBroker(brokers);
        kProducer.SetTopic(topic);
        kProducer.SetClientID(ClientID);
        System.gc();
    }

    private static void RemoveProcessedLogs() {

    }

    private static boolean GoodToGo() {
        boolean proceed = true;
        if (cfile.equals("")) {
            uCommons.uSendMessage("FATAL: shell script missing parameter: 'conf'");
            proceed = false;
        }
        if (logdir.equals("")) {
            uCommons.uSendMessage("FATAL: shell script missing parameter: 'logdir'");
            proceed = false;
        }
        if (matchStr.equals("")) {
            uCommons.uSendMessage("FATAL: shell script missing parameter: 'matchstr'");
            proceed = false;
        }
        return proceed;
    }

    private static void Rsync() {
        String cmd      = "rsync -u -z";
        String rsProto  = "-e ssh";
        String rsHost   = NamedCommon.dbuser + "@" + NamedCommon.dbhost;
        String rsSource = "/usr/uv/audit/seqlogs/*";
        String rsTarget = "/upl/dbaudlogs/";
        // ----------------------------------------------------------------------
        //  The result of executing the rsync command is that log files from
        //  rsHost are copied from rsSource to rsTarget - which is a local dir
        //  The FIRST rsync may take a few minutes, subsequent rsync's are FAST
        // ----------------------------------------------------------------------
        rsync = cmd + " " + rsProto + " " + rsHost + rsSource + " " + rsTarget;
        uCommons.nixExecute(rsync, false);
    }

    private static void GetAuditlogs() {
        matchFiles.clear();
        int idx=0;
        boolean viaOS = false;
        if (viaOS) {
            File[] mFiles = null;
            File dir = new File(logdir);
            mFiles = dir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.contains(matchStr);
                }
            });
            int eof = mFiles.length;
            String theFile, dosSlash="\\\\";
            for (int mf=0; mf<eof; mf++) {
                theFile = mFiles[mf].toString();
                if (theFile.contains("\\")) theFile = theFile.replaceAll(dosSlash, NamedCommon.slash);
                matchFiles.add(theFile);
            }
            return;
        }

        UniDynArray uvRec = new UniDynArray();
        uvRec.insert(1, 0, 0, "F");
        uvRec.insert(2, 0, 0, evtHome);
        uvRec.insert(3, 0, 0, "D_VOC");
        try {
            NamedCommon.VOC.setRecordID(audFile);
            NamedCommon.VOC.setRecord(uvRec);
            NamedCommon.VOC.write();
            if (NamedCommon.U2File != null) NamedCommon.U2File.close();
            NamedCommon.U2File = NamedCommon.uSession.openFile(audFile);
            uvstr = NamedCommon.U2File.read(uvAudLog);
            uvRec = new UniDynArray(uvstr);
            int eoi=uvRec.dcount();
            String line="";
            for (int a=0 ; a<eoi; a++) {
                line = uvRec.extract(a).toString();
                if (line.startsWith("buffer[")) {
                    if (line.contains(" sequential log ")) {
                        line = line.substring(line.indexOf("/"), line.length()).replace("...", "");
                        if ( matchFiles.indexOf(line) <0 ) matchFiles.add(line);
                    }
                }
            }
            uvRec = null;
        } catch (UniFileException | UniSessionException e) {
            uCommons.uSendMessage("VOC "+audFile+" ERROR: " + e.getMessage());
            NamedCommon.ZERROR = true;
        }
    }

    private static UniDynArray BuildUvRec(String line) {
        UniDynArray uvar = new UniDynArray();
        int lx = line.length();
        int asc;
        StringBuilder sb = new StringBuilder();;
        for (int c=0 ; c < lx ; c++) {
            char C = line.charAt(c);
            asc = (int) C;
            if (asc < 150) {
                sb.append(String.valueOf(C));
            } else {
                sb.append("<fm>");
            }
        }
        uvar = uCommons.SQL2UVRec(sb.toString());
        return uvar;
    }

}
