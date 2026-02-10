package com.unilibre.kafka.redundant;
/* * Copyright UniLibre on 2015. ALL RIGHTS RESERVED  **/

// -----------------------------------------------------
//           Superceeded by uvhashStreams
// -----------------------------------------------------

import asjava.uniobjects.*;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.kafka.kProducer;
import org.json.JSONObject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static com.unilibre.commons.kafkaCommons.*;

public class uDataLogger {

    private static Properties props;
    private static String brokers = "";
    private static String topic = "";
    private static String SourceDAT = "";
    private static String loggedItem = "";
    private static String cdcFile = "uDELTA.LOG";
    private static String passport = "";
    private static int pause;
    private static Double version;
    private static int heartbeat;
    private static int page=0,  pSize=100, itemsLogged=0, itemCnt=0;
    private static File SourceDir = null;
    private static String matchStr = ".ulog", ClientID="";
    private static String cmd = "SELECT uDELTA.LOG SAMPLE 100";
    private static String[] matchingFiles;
    private static String[] deleteItems;
    private static ArrayList<String> selList = new ArrayList<>();
    private static ArrayList<String> arrIIDs = new ArrayList<>();
    private static ArrayList<String> arrRecs = new ArrayList<>();
    private static UniCommand ucmd = null;
    private static boolean stopSW = false;
    private static boolean debug = false;
    private static boolean AVRO = false;
    private static boolean ENCR = false;
    private static boolean haveRecs = false;
    private static Stream<Path> walk = null;

    private static String GetDateTimeStamp() {
        String dts="";
        String chk, mTime, mDate, MSec;
        int ThisMS;
        mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()).replace(":", "");
        mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime()).replace("-","");
        dts = mDate+mTime;
        return dts;
    }

    private static String BuildRecord(String fqfn, String timedate) {
        String rec;
        deleteItems = new String[1];
        if (!SourceDAT.equals("")) {
            rec = ReadDiskRecord(fqfn);
            if (!rec.equals("")) rec = timedate + NamedCommon.IMark + rec;
            // DTS : atIM : atIM : ACCT : atIM : FILE : atIM : ITEM : atIM : RECORD
            File fTmp = new File(fqfn);
            if (fTmp.exists() && ! debug) {
                fTmp.delete();
                if (fTmp.exists()) {
                   uCommons.uSendMessage("Cannot delete "+fqfn);
                    System.exit(0);
                }
            }
            fTmp = null;
        } else {
            //
            // haveRecs is for Reality only.
            //
            if (!haveRecs) {
                NamedCommon.uID = null;
                // audlog ID <im> ACCT <im> FILE <im> ITEM <im> RECORD
                rec = uCommons.UV2SQLRec(NamedCommon.uID, u2Commons.uRead(NamedCommon.U2File, fqfn));
                try {
                    NamedCommon.U2File.setRecordID(fqfn);
                    NamedCommon.U2File.deleteRecord(fqfn);
                } catch (UniFileException e) {
                    // no problem - it was probably deleted by another process
                }
            } else {
                rec = fqfn;
            }
        }

        String audlog  = uCommons.FieldOf(rec, NamedCommon.IMark, 1);
        // date stamp
        String dstmp  = uCommons.FieldOf(audlog, "\\.", 1);
        dstmp = uCommons.oconvD(dstmp, "D4-");
        // time stamp
        String tstmp = uCommons.FieldOf(audlog, "\\.", 2);
        tstmp = uCommons.oconvM(tstmp, "MTS");

        String acct = uCommons.FieldOf(rec, NamedCommon.IMark, 2);
        String file = uCommons.FieldOf(rec, NamedCommon.IMark, 3);
        String item = uCommons.FieldOf(rec, NamedCommon.IMark, 4);
        String recd = uCommons.FieldOf(rec, NamedCommon.IMark, 5);

        String src = SourceDAT;
        if (SourceDAT.equals("")) src = NamedCommon.dbhost;

        String encSeed = "";
        if (ENCR) {
            encSeed = uCipher.GetCipherKey();
            acct = uCipher.v2Scramble(uCipher.keyBoard, acct, encSeed);
            file = uCipher.v2Scramble(uCipher.keyBoard, file, encSeed);
            item = uCipher.v2Scramble(uCipher.keyBoard, item, encSeed);
            recd = uCipher.v2Scramble(uCipher.keyBoard, recd, encSeed);
            src  = uCipher.v2Scramble(uCipher.keyBoard, SourceDAT, encSeed);
//            passport = encSeed;
        }

        rec = BuildJsonObject(dstmp, tstmp, src, acct, file, item, passport, recd, encSeed);
        return rec;
    }

    private static void BatchOfEvents(String fqfn, String timedate) {

        String rec = ReadDiskRecord(fqfn);
        if (rec.equals("")) return;

        File fTmp = new File(fqfn);
        if (fTmp.exists() && ! debug) {
            fTmp.delete();
            if (fTmp.exists()) {
                uCommons.uSendMessage("Cannot delete "+fqfn);
                System.exit(0);
            }
        }
        fTmp = null;

        ArrayList<String> lines = new ArrayList<String>(Arrays.asList(rec.split("\\r?\\n")));
        rec = "";
        int eol = lines.size();

        for (int i=0; i< eol; i++) {
            rec = lines.get(i);
            if (!rec.equals("")) {
                if (!kProducer.kSend(topic, "key", rec)) {
                    uCommons.uSendMessage("Failed to log this line: " + rec);
                }
            }
        }
        lines = null;
    }

    private static String BuildJsonObject(String dts, String tts, String src, String acct, String file, String item, String passport, String recd, String encSeed) {
        JSONObject jObj = new JSONObject();
        jObj.put("passport", passport+"~"+encSeed);
        jObj.put("sourceinstance", src);
        jObj.put("sourceaccount", acct);
        jObj.put("date", dts);
        jObj.put("time", tts);
        jObj.put("file", file);
        jObj.put("item", item);
        jObj.put("record", recd);
        String ans = jObj.toString();
        jObj = null;
        return ans;
    }

    private static void SetValues(String cfile) {
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        props = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(props);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        props.clear();

        props = LoadProperties(cfile);
        if (KERROR) System.exit(0);

        brokers = props.getProperty("brokers", "");                     // kafka broker(s)
        topic = props.getProperty("topic", "UniLibre");                 // stream topic
        matchStr = props.getProperty("extension", matchStr);                       // suffix of items to collect from uDelta.Log
        SourceDAT = props.getProperty("source", "");                    // the mounted folder pointing to uDelta.Log
        ClientID = props.getProperty("clientid", "DataLogger");         // Kafka Client-ID
        ENCR = props.getProperty("encrypt", "false").toLowerCase().equals("true");  // is the data encrypted or clear-text

        try {
            version = Double.valueOf(props.getProperty("version", ""));
        } catch (NumberFormatException nfe) {
            version = 1.0;
        }

        try {
            pause = Integer.valueOf(props.getProperty("pause", "250"));
        } catch (NumberFormatException nfe) {
           uCommons.uSendMessage("FATAL: Pause value must be an integer and > 100.");
            System.exit(0);
        }
        try {
            heartbeat = Integer.valueOf(props.getProperty("heartbeat", "100"));
        } catch (NumberFormatException nfe) {
           uCommons.uSendMessage("FATAL: HeartBeat value must be an integer > 100.");
            System.exit(0);
        }

        if (brokers.equals("")) {
           uCommons.uSendMessage("FATAL: No Kafka broker(s) found.");
            System.exit(0);
        }

        NamedCommon.databaseType = GetValue(props, "dbtype", "");
        NamedCommon.protocol = GetValue(props, "protocol", "");
        switch (NamedCommon.protocol) {
            case "u2cs":
                NamedCommon.dbhost = GetValue(props, "u2host", "");
                NamedCommon.dbPort = GetValue(props, "u2port", NamedCommon.dbPort);
                if (NamedCommon.dbhost.equals("")) {
                    uCommons.uSendMessage("FATAL: No Source data directory specified.");
                    System.exit(0);
                }
                NamedCommon.dbpath = GetValue(props, "u2path", "");
                NamedCommon.dbuser = GetValue(props, "u2user", "");
                NamedCommon.passwd = GetValue(props, "u2pass", "");
                NamedCommon.datAct = GetValue(props, "u2acct", "");
                NamedCommon.minPoolSize = GetValue(props, "minpoolsize", "");
                NamedCommon.maxPoolSize = GetValue(props, "maxpoolsize", "");
                NamedCommon.CPL = GetValue(props, "cpl", "").toLowerCase().equals("true");
                NamedCommon.uSecure = GetValue(props, "secure", "").toLowerCase().equals("true");
//                if (SourceDAT.equals("")) SourceDAT = NamedCommon.dbhost;
                break;
            case "real":
                NamedCommon.realhost = GetValue(props, "realhost", NamedCommon.realhost);
                NamedCommon.realuser = GetValue(props, "realuser", NamedCommon.realuser);
                NamedCommon.realdb = GetValue(props, "realdb", NamedCommon.realdb);
                NamedCommon.realac = GetValue(props, "realac", NamedCommon.realac);
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
                return;
            default:
                uCommons.uSendMessage("FATAL: Unknown database source definition in " + cfile);
                System.exit(0);
        }

        String encSRC = u2Commons.ReadAnItem("BP.UPL", "@ENCRYPT", "1", "0", "0");
        if (NamedCommon.ZERROR) return;
        if (encSRC.equals("1")) ENCR = true;
        SourceDB.DisconnectSourceDB();          // why ??

        stopSW = (props.getProperty("stop", "").toLowerCase().equals("true"));
        props.clear();
        kProducer.SetBroker(brokers);
        kProducer.SetTopic(topic);
        kProducer.SetClientID(ClientID);
    }

    private static String[] GetFileNames() {

        if (SourceDAT.equals("")) return SelectDeltas();

        if (SourceDir == null) SourceDir = new File(SourceDAT);
        matchingFiles = null;

        walk = null;
        try {
            walk = Files.walk(Paths.get(SourceDAT));
            List<String> result = walk.map(x -> x.toString()).filter(f -> f.endsWith(matchStr)).collect(Collectors.toList());
            matchingFiles = new String[result.size()];
            for (int i=0; i< result.size(); i++) { matchingFiles[i] = result.get(i); }
            result.clear();
            result = null;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
        int nbrmatches = matchingFiles.length;
        String tmp = "";
        for (int ff = 0; ff < nbrmatches; ff++) {
            tmp = String.valueOf(matchingFiles[ff]);
            while (tmp.contains(matchStr) || tmp.contains(SourceDAT)) {
                tmp = tmp.replace(matchStr, "");
                tmp = tmp.replace(SourceDAT, "");
            }
            matchingFiles[ff] = tmp;
        }
        tmp = "";
        Arrays.sort(matchingFiles);
        walk = null;
        return matchingFiles;
    }

    private static String[] SelectDeltas() {
        matchingFiles = null;
        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        switch (NamedCommon.protocol) {
            case "u2cs":
                boolean doOpen = false;
                if (NamedCommon.U2File == null ) doOpen = true;
                if (!doOpen) if (!NamedCommon.U2File.getFileName().equals(cdcFile)) doOpen = true;
                if (doOpen) NamedCommon.U2File = u2Commons.uOpen(cdcFile);

                if (ucmd == null) {
                    try {
                        ucmd = NamedCommon.uSession.command();
                    } catch (UniSessionException e) {
                        if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                        uCommons.uSendMessage("Cannot set uCmd " + e.getMessage());
                        System.exit(0);
                    }
                    ucmd.setCommand(cmd);
                }
                try {
                    ucmd.exec();
                } catch (UniCommandException e) {
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    uCommons.uSendMessage("Cannot execute " + cmd);
                    uCommons.uSendMessage(e.getMessage());
                    System.exit(0);
                }
                int nbrmatches = ucmd.getAtSelected();
                matchingFiles = new String[nbrmatches];
                if (nbrmatches > 0) {
//                    uCommons.uSendMessage(cmd);
                    for (int ff = 0; ff < nbrmatches; ff++) {
                        try {
                            matchingFiles[ff] = NamedCommon.uSession.selectList(0).next().toString();
                        } catch (UniSelectListException e) {
                            matchingFiles[ff] = "";
                            uCommons.uSendMessage(e.getMessage());
                        } catch (UniSessionException e) {
                            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                            matchingFiles[ff] = "";
                            uCommons.uSendMessage(e.getMessage());
                        }
                    }
                }
                break;
            case "real":
                // --------------------------------------------------------------------------------
                String srtn = "{SAR}";
                String file = "{file="+cdcFile+"}";
                String sCmd = "{cmd=SELECT "+cdcFile+" (G50}";  // select upto 50 records at a time.
                String sel  = "";
                String atr  = "";
                String cSel = srtn + sCmd + file + atr + sel;
                String reply= u2Commons.MetaBasic(cSel);
                String line, rID, rRec;
                String[] lines;
                String[] lParts;
                int eol=0;
                arrIIDs.clear();
                arrRecs.clear();
                lines = reply.split("<rt>");
                eol = lines.length;
                for (int l=0 ; l < eol ; l++) {
                    line = lines[l]+"!!"; //+NamedCommon.KMark+NamedCommon.KMark+NamedCommon.KMark;
                    lParts = line.split(NamedCommon.KMark);
                    rID  = line.split(NamedCommon.KMark)[0];

                    if (!rID.endsWith(matchStr)) continue;

                    rRec = line.split(NamedCommon.KMark)[1];
                    rRec = rRec.substring(0, rRec.length()-2);
                    if (!rID.equals("")) {
                        arrIIDs.add(rID);
                        arrRecs.add(rRec);
                    }
                }
                haveRecs = true;
                eol =arrIIDs.size();
                matchingFiles = new String[eol];
                for (int l=0 ; l < eol ; l++) {
                    matchingFiles[l] = arrIIDs.get(l);
                }
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
                return null;
        }
        return matchingFiles;
    }

    private static void DeleteEventsTake() {
        int eoi = 0;
        switch (NamedCommon.protocol) {
            case "u2cs":
                eoi = matchingFiles.length;
                for (int i=0 ; i < eoi ; i++) {
                    if (SourceDAT.equals("")) {
                        try {
                            NamedCommon.U2File.setRecordID(matchingFiles[i]);
                            NamedCommon.U2File.deleteRecord();
                        } catch (UniFileException e) {
                            // no problem - it was probably deleted by another process
                        }
                    }
                }
                deleteItems = null;
                break;
            case "real":
                eoi = arrIIDs.size();
                String srtn = "{DEL}";
                String file = "{file=" + cdcFile + "}";
                String item = "";
                String atr = "";
                String cSel = "";
                for (int i = 0; i < eoi; i++) {
                    if (arrRecs.get(i).equals("")) continue;
                    item = "{item=" + arrIIDs.get(i) + "}";
                    cSel = srtn + file + item + atr;
                    String reply = u2Commons.MetaBasic(cSel);
                    if (!reply.equals("ok")) {
                        uCommons.uSendMessage("DELETION ERROR on item " + item + "  reply " + reply);
                    }
                }
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
        }
    }

    public static String GetValue(Properties runProps, String key, String def) {
        String value = runProps.getProperty(key, def);
        if (value.equals(null) || value.equals("")) {
            value = def;
        } else {
            if (value.startsWith("ENC(")) {
                String tmp = value.substring(4, (value.length() - 1));
                value = uCipher.Decrypt(tmp);
            }
        }
        return value;
    }

    public static void main(String[] args) {
        String cFile = System.getProperty("conf", "uKafka.properties");
        debug = System.getProperty("debug", "false").toLowerCase().equals("true");
        if (!kafkaCommons.isLicenced()) System.exit(1);

        passport = (License.domain+NamedCommon.loAplha).substring(0,16);
        System.out.println(" ");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage("uDataLogger(): Will push database deltas into uStreams  ");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage("                      Using: " + cFile);
        SetValues(cFile);
        if (NamedCommon.ZERROR) System.exit(0);
        if (NamedCommon.dbhost.replaceAll("\\ ", "").equals("")) NamedCommon.dbhost = "";

        if (!SourceDAT.equals("")) {
            uCommons.uSendMessage("Looking in " + SourceDAT + " for *" + matchStr);
        } else {
            uCommons.uSendMessage("Looking in " + cdcFile + " for *" + matchStr);
        }
        uCommons.uSendMessage("Sending deltas to broker(s): " + brokers);
        uCommons.uSendMessage("                   on topic: " + topic);
        uCommons.uSendMessage("                    LogType: " + matchStr);
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage(" ");

        int lCnt = 0;
        String fqfn, rec, tdFile;
        boolean kPass = false;

        while (!stopSW) {
            GetFileNames();
            int eoi = matchingFiles.length;
//            if (eoi > 0) uCommons.uSendMessage("   ). " + eoi + " items to process.");
            for (int f = 0; f < eoi; f++) {
                rec="";
                tdFile="";
                if (!haveRecs) {
                    tdFile = matchingFiles[f];
                    if (!SourceDAT.equals("")) {
                        fqfn = SourceDAT + tdFile + matchStr;
                        BatchOfEvents(fqfn, tdFile);
                        kPass = true;
                    } else {
                        fqfn = tdFile;
                        rec = BuildRecord(fqfn, "");
                        if (rec.equals("")) continue;           // safe-guard : MANY DataLogger processes !!!
                        kPass = kProducer.kSend(topic, "key", rec);
                    }
                    itemCnt++;
                    itemsLogged++;
                } else {
                    if (matchingFiles[f].equals("")) continue;
                    fqfn = arrRecs.get(f);
                    if (!fqfn.equals("")) {
                        rec = BuildRecord(fqfn, "");
                    } else {
                        rec = "";
                    }
                    if (rec.equals("")) continue;
                    kPass = kProducer.kSend(topic, "key", rec);
                }

                if (kPass) {
                    if (itemCnt >= 100) {
                        uCommons.uSendMessage(itemsLogged + " events logged.");
                        itemCnt = 0;
                    }
                } else {
                    uCommons.uSendMessage("FAILURE to log " + tdFile);
                }
            }
//            if (SourceDAT.equals("") && eoi > 0) DeleteEventsTake();
            Sleep(pause);
            if (eoi == 0) lCnt++;
            KERROR = false;
            NamedCommon.ZERROR=false;
            if (lCnt > heartbeat) {
                Sleep(pause);
                SetValues(cFile);
                uCommons.uSendMessage("uDataLogger   <<Heartbeat>>");
                lCnt = 0;
            }
        }

        SourceDir = null;
        kProducer.Close();

        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage("Master stop switch turned ON. Stopping now.");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage(" ");
    }

}
