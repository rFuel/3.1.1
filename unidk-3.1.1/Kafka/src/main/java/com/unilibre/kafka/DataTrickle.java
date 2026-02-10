package com.unilibre.kafka;
/*  Copyright UniLibre on 2015. ALL RIGHTS RESERVED  */


// ---------------------------------------------------------------------------------
//  DataTrickle connects with a U2 host with read only permissions.
//
//  "register" property gives a list of files account file,acctount file,etc.
//  Will loop through each file and trickle all records DIRECTLY into Kafka
//
// If doDeltas is true, it will only take records which have changed
// ---------------------------------------------------------------------------------

import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import org.json.JSONObject;

import java.io.File;
import java.util.Properties;

import static com.unilibre.commons.u2Commons.uOpen;

public class DataTrickle {

    private static String acct= "", file= "", files="", group, filter="", filters="";
    private static String brokers, topic, ClientID, cfile="", dot = ".";
    private static String passport="", encSeed="", newHash="", hashKey="";
    private static String saslUser="", saslPass="",saslKey="";
    private static String[] fileList;
    private static String[] filterList;
    private static UniFile srcFile = null;
    private static int pcnt=1500, wait=15, lHash=15, batchsize=1;
    private static Properties dtProps = null;
    private static boolean ENCR = true, verbose, debug, dirOpen, doDeltas=false;
    private static boolean sasl = false, batching=false;

    private static void Initialise() {

        dtProps = uCommons.LoadProperties(cfile);
        if (NamedCommon.ZERROR || dtProps == null) System.exit(0);

        dirOpen  = dtProps.getProperty("directopen", "false").toLowerCase().equals("true");

        // use directopen=true when you do NOT have an empty UV account where you have permissions to write to the VOC
        // in this case, attached directly to the data and open files directly.

        brokers  = dtProps.getProperty("brokers", "");
        topic    = dtProps.getProperty("uplift", "UniLibre");
        group    = dtProps.getProperty("group", "uStreams");
        ClientID = "DataTrickle";
        ENCR     = dtProps.getProperty("encrypt", "false").toLowerCase().equals("true");
        verbose  = dtProps.getProperty("verbose", "false").toLowerCase().equals("true");
        debug    = dtProps.getProperty("debug", "false").toLowerCase().equals("true");
        files    = dtProps.getProperty("register", "");
        filters  = dtProps.getProperty("select", "");
        doDeltas = dtProps.getProperty("do-deltas", "false").toLowerCase().equals("true");
        NamedCommon.CPL = dtProps.getProperty("cpl", "false").toLowerCase().equals("true");
        NamedCommon.uSecure = dtProps.getProperty("secure", "false").toLowerCase().equals("true");

        if (!NamedCommon.CPL) NamedCommon.uSecure=false;
        fileList    = files.split("\\,");
        filterList  = new String[fileList.length];
        String[] tempArr = filters.split(",");

        for (int f=0 ; f < fileList.length ; f++) {
            if (f < tempArr.length) {
                filterList[f] = tempArr[f];
            } else {
                filterList[f] = "";
            }
        }
        tempArr = null;

        try {
            pcnt = Integer.valueOf(dtProps.getProperty("proccnt", "1500"));
        } catch (NumberFormatException nfe) {
            pcnt = 1500;
        }
        try {
            wait = Integer.valueOf(dtProps.getProperty("waitcnt", "15"));
        } catch (NumberFormatException nfe) {
            wait = 1500;
        }

        try {
            batchsize = Integer.valueOf(dtProps.getProperty("batch", "1"));
            if (batchsize > 1) batching = true;
        } catch (NumberFormatException nfe) {
            batchsize = 1;
        }

        if (!dtProps.getProperty("u2host", "").equals("")) NamedCommon.dbhost = dtProps.getProperty("u2host", "dtProp-error");
        if (!dtProps.getProperty("u2port", "").equals("")) NamedCommon.dbPort = dtProps.getProperty("u2port", "dtProp-error");
        if (!dtProps.getProperty("u2path", "").equals("")) NamedCommon.dbpath = dtProps.getProperty("u2path", "dtProp-error");
        if (!dtProps.getProperty("u2user", "").equals("")) NamedCommon.dbuser = dtProps.getProperty("u2user", "dtProp-error");
        if (!dtProps.getProperty("u2pass", "").equals("")) NamedCommon.passwd = dtProps.getProperty("u2pass", "dtProp-error");
        if (!dtProps.getProperty("u2acct", "").equals("")) NamedCommon.datAct = dtProps.getProperty("u2acct", "dtProp-error");

        if (!dtProps.getProperty("dbtype", "").equals("")) NamedCommon.databaseType = dtProps.getProperty("dbtype", "dtProp-error");
        if (!dtProps.getProperty("protocol", "").equals("")) NamedCommon.protocol = dtProps.getProperty("protocol", "dtProp-error");
        if (!dtProps.getProperty("cpl", "").equals("")) NamedCommon.CPL = (dtProps.getProperty("cpl", "dtProp-error").toLowerCase().equals("true"));
        if (!dtProps.getProperty("secure", "").equals("")) NamedCommon.uSecure = (dtProps.getProperty("secure", "dtProp-error").toLowerCase().equals("true"));
        if (!dtProps.getProperty("minpoolsize", "").equals("")) NamedCommon.minPoolSize = dtProps.getProperty("minpoolsize", "1");
        if (!dtProps.getProperty("maxpoolsize", "").equals("")) NamedCommon.maxPoolSize = dtProps.getProperty("maxpoolsize", "4");

        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();

        if (dirOpen && NamedCommon.protocol.equals("u2cs")) {
            uCommons.uSendMessage("Direct Open is ON so account settings are vital:");
            uCommons.uSendMessage("Connect to: " + NamedCommon.dbhost + "  to account: " + NamedCommon.datAct);
        }

        if (doDeltas) {
            File hashDir = new File("./hash");
            if (!hashDir.exists()) hashDir.mkdir();
        }
    }

    public static void Process() {
        if (dtProps == null) Initialise();
        if (acct.equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Message error: missing account property";
            return;
        }
        if (file.equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Message error: missing file property";
            return;
        }

        String kafClient = ClientID+"_"+acct+"_"+file;
        while (kafClient.contains(".")) { kafClient = kafClient.replace(".", "_"); }

        if (sasl) kProducer.SetSASL(true, saslUser, saslPass, saslKey);

        kProducer.SetBroker(brokers);
        kProducer.SetTopic(topic);
        kProducer.SetClientID(kafClient);
        kProducer.SetGroup(group);
        if (batching) kProducer.SetBatchSize(batchsize);
        //  for run-time: -Dlog4j.configuration=R:\\upl\conf\log4j.properties
        kProducer.producer = kProducer.kConnect();
        if (kProducer.producer != null) {
            uCommons.uSendMessage("Connected to Kafka broker(s) [" + brokers + "] to produce events on [" + topic + "] as client [" + kafClient + "]");
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Kafka connectivity issues need to be resolved.";
            return;
        }

        if (file.length() > lHash) lHash = file.length();
        uCommons.uSendMessage("***");
        uCommons.uSendMessage("*** DataTrickle ***  >>account " + uCommons.LeftHash(acct, 10) + "   >>file " + uCommons.LeftHash(file, lHash) + "   >>process " + pcnt + "    >>pause " + wait);
        uCommons.uSendMessage("***");

        if (dirOpen) {
            try {
                uCommons.uSendMessage("Direct Open: " + file);
                srcFile = NamedCommon.uSession.openFile(file);
            } catch (UniSessionException e) {
                System.out.println(e.getMessage());
                srcFile = null;
            }
        } else {
            NamedCommon.datAct = acct;
            srcFile = uOpen(file);
        }

        if (srcFile == null) return;
        boolean proceed;
        
        try {
            proceed = RegisterThisProcess(acct+"~"+file);
            uCommons.uSendMessage("Processing " + file + "  from " + acct);
            if (proceed) {
                ReadAndWrite();
                DeregisterThisProcess(acct + "~" + file);
            } else {
                uCommons.uSendMessage(file + "  from " + acct + " is being processed on another process.");
            }
        } catch (UniSessionException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = e.getMessage();
        } catch (UniCommandException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = e.getMessage();
        } catch (UniSelectListException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = e.getMessage();
        }
        kProducer.Close();
        return;
    }

    private static void ReadAndWrite() throws UniSessionException, UniCommandException, UniSelectListException {

        UniCommand ucmd = NamedCommon.uSession.command();
        String sel = "SELECT " + srcFile.getFileName() + filter;
        uCommons.uSendMessage("Executing: " + sel);
        ucmd.setCommand(sel);
        ucmd.exec();
        int AtSelected = ucmd.getAtSelected();
        int slx = String.valueOf(AtSelected).length();
        uCommons.uSendMessage("      >> " + AtSelected + " records selected");
        String nrec, logRec, kafKey;
        UniString rID, rec;
        boolean eof = false, kPass = true;
        int tcnt = 0, rcnt = 0, totC=0;
        uCommons.uSendMessage("Processing selected records.");

        if (debug) uCommons.uSendMessage("[debug]  is ON");

        long startM = 0;
        double laps=0;
        double div = 1000000000.00;

        while (!eof) {
            rID = NamedCommon.uSession.selectList(0).next();
            if (rID.toString().equals("")) { eof=true; continue; }

            if (debug) uCommons.uSendMessage("[debug]  rID = " + rID.toString());

            try {
                srcFile.setRecordID(rID);
                rec = srcFile.read();
                nrec =  uCommons.UV2SQLRec(null, rec);

                // Dealerships have control chars in every data field !!
                if (NamedCommon.CleanseData) nrec = u2Commons.Cleanse(nrec);
                if (doDeltas) { if (!isNew(nrec, rID.toString())) continue; }

                if (debug) uCommons.uSendMessage("[debug]      build logrec");
                logRec = BuildLogRecord(nrec, rID.toString());

                kafKey = "[" + acct + "].[" + file + "].[" + rID.toString() + "]";

                if (debug) startM = System.nanoTime();
                if (batching) {
                    if (debug) uCommons.uSendMessage("[debug]      adding to kafka batch");
                    kPass = kProducer.kBatchCollector(topic, kafKey, logRec);
                } else {
                    if (debug) uCommons.uSendMessage("[debug]      send to kafka");
                    kPass = kProducer.kSend(topic, kafKey, logRec);
                }
                if (debug) {
                    laps = (System.nanoTime() - startM);
                    laps = laps / div;
                    uCommons.uSendMessage("[debug]      done in " + laps + " seconds");
                }

                rcnt++;
                totC++;
                tcnt++;
                if (rcnt >= pcnt) {
                    if (verbose) uCommons.uSendMessage("runStat: " + uCommons.RightHash(String.valueOf(totC), slx) + " records read, " + uCommons.RightHash(String.valueOf(tcnt), slx) + " records logged.");
                    if (wait > 0) uCommons.Sleep(wait);
//                    Initialise();
                    rcnt = 0;
                }

                if (!kPass) {
                    uCommons.uSendMessage("FAILURE to log " + acct + " " + file + " " + rID.toString());
                    nrec = "";
                    rec = null;
                    logRec="";
                } else {
                    if (doDeltas) uCommons.WriteDiskRecord("hash/"+hashKey, newHash);
                }
            } catch (UniFileException e) {
                rec = null;
                eof = true;
                nrec= "";
                logRec="";
                tcnt--;
                continue;
            }
        }

        if (batching) {
            if (debug) uCommons.uSendMessage("[debug]  flush batch to kafka");
            kProducer.kBatch(); // flush remaining entries in the batch.
        }

        uCommons.uSendMessage(".");
        uCommons.uSendMessage("Completed: " + totC + " records read, " + tcnt + " records logged.");
    }

    private static boolean isNew(String nrec, String rID) {
        hashKey= NamedCommon.dbhost + dot + acct + dot + file + dot + rID;
        String hash   = uCommons.ReadDiskRecord("hash/"+hashKey).replaceAll("\\r?\\n", "");
        NamedCommon.ZERROR=false; NamedCommon.Zmessage="";

        String hashMe = rID + NamedCommon.IMark + nrec;
        newHash= uCommons.GetHash(hashMe);
        if (hash.equals("")) return true;
        if (!newHash.equals(hash)) return true;

        return false;
    }

    private static String BuildLogRecord(String rec, String item) {
        String today = uCommons.GetToday();
        today = uCommons.oconvD(today, "D4-");
        String[] dParts = today.split("-");
        String time  = uCommons.GetTime();
        time  = uCommons.oconvM(time, "MTS");
        String dts = dParts[2] + dParts[1] + dParts[0] + time.replace(":", "");;
        String src = NamedCommon.dbhost;

        if (ENCR) rec = uCipher.v2Scramble(uCipher.keyBoard, rec, encSeed);

        JSONObject jObj = new JSONObject();
        jObj.put("date", dts.substring(0,8));
        jObj.put("time", dts.substring(8,dts.length()));
        jObj.put("sourceinstance", src);
        jObj.put("sourceaccount", acct);
        jObj.put("passport", passport+"~"+encSeed);
        jObj.put("file", file);
        jObj.put("item", item);
        jObj.put("record", rec);            // may be scrambled !
        String ans = jObj.toString();
//        if (ENCR)  ans = uCipher.jEncrypt(ans);   // Need JRE 11+
        return ans;
    }

    private static boolean RegisterThisProcess(String file) {
        boolean found=false, okay=true;
        String[] pidArray = GetDtrunners();
        for (int r=0 ; r < pidArray.length ; r++) {
            String[] rParts = pidArray[r].split("\t");
            if (rParts.length < 2) continue;
            if (NamedCommon.pid == rParts[0]) {
                uCommons.uSendMessage("This PID is already recorded.");
                okay=false; found=true; break;
            }
            if (rParts[1].equals(file)) {
                if (!rParts[0].toLowerCase().equals("done")) {
                    uCommons.uSendMessage("File " + file + " is being processed on PID " + rParts[0]);
                    okay = false;
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            String newArray = "";
            for (int r=0 ; r < pidArray.length ; r++) {
                if (pidArray[r].length() > 1) newArray += pidArray[r] + "\n";
            }
            newArray += NamedCommon.pid + "\t" + file + "\n";
            uCommons.WriteDiskRecord(NamedCommon.BaseCamp+"/conf/DTRUNNERS", newArray);
        }
        return okay;
    }

    private static void DeregisterThisProcess(String file) {
        String[] pidArray = GetDtrunners();
        for (int r=0 ; r < pidArray.length ; r++) {
            String[] rParts = pidArray[r].split("\t");
            if (rParts.length < 2) continue;
            if (rParts[0].equals(NamedCommon.pid) && rParts[1].equals(file)) {
                pidArray[r] = "Done\t"+rParts[1];
                break;
            }
        }
        String newArray = "";
        for (int r=0 ; r < pidArray.length ; r++) {
            if (!pidArray[r].equals("")) { newArray += pidArray[r] + "\n"; }
        }
        uCommons.WriteDiskRecord(NamedCommon.BaseCamp+"/conf/DTRUNNERS", newArray);
    }

    private static String[] GetDtrunners() {
        String dtControl = uCommons.ReadDiskRecord(NamedCommon.BaseCamp+"/conf/DTRUNNERS");
        NamedCommon.ZERROR=false; NamedCommon.Zmessage="";
        String[] runArray = dtControl.split("\n");
        return runArray;
    }

    public static void main(String[] args) {

        if (!kafkaCommons.isLicenced()) System.exit(1);

        uCommons.showErrors = false;

        passport = (License.domain+NamedCommon.loAplha).substring(0,16);

        cfile = System.getProperty("conf", "notonfile");

        System.out.println(" ");
        System.out.println(" ");
        System.out.println(" ");
        System.out.println("-------------------------------------------------------------");
        System.out.println("DataTrickle()  starting...   (using " + cfile + ")");
        System.out.println("-------------------------------------------------------------");

        Initialise();
        for (int f=0 ; f < fileList.length ; f++) { System.out.println("  >> " + fileList[f]); }

        if (ENCR)  encSeed = uCipher.GetCipherKey();

        String[] fParts;
        boolean proceed = true;
        while(proceed) {
            for (int f = 0; f < fileList.length; f++) {
                fParts = (fileList[f] + " # # #").split(" ");
                acct = fParts[0];
                file = fParts[1];
                filter = " " + filterList[f];
                while (acct.contains(" ")) {
                    acct = fParts[0].replace(" ", "");
                }
                while (file.contains(" ")) {
                    file = fParts[1].replace(" ", "");
                }
                // --------------------------------------------------------------------------------
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("**");
                uCommons.uSendMessage("-------------------------------------------------------------------------");
                if (NamedCommon.ZERROR) break;
                Process();
                if (NamedCommon.ZERROR) break;
                NamedCommon.Reset();
                uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
                Properties rfProps = uCommons.LoadProperties("rFuel.properties");
                if (NamedCommon.ZERROR) break;
                uCommons.SetCommons(rfProps);
                uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
                Initialise();
                rfProps = null;
            }
            // --------------------------------------------------------------------------------
            if (!doDeltas) {
                proceed = false;
            } else {
                uCommons.Sleep(10);
                uCommons.uSendMessage("-------------------------------------------------------------------------");
                uCommons.uSendMessage("--             Loop again, looking for deltas.                         --");
                uCommons.uSendMessage("-------------------------------------------------------------------------");
            }
        }
        if (NamedCommon.ZERROR) uCommons.uSendMessage(NamedCommon.Zmessage);
        uCommons.uSendMessage("-------------------------------------------------------------------------");
        uCommons.uSendMessage("Done.");
    }

}
