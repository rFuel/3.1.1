package com.unilibre.dataworks;

// -------------------------------------------------------------------------------
// Original program MoveData still works as intended - for raw and uni files.
// If NamedCommon.SmartMovers is true, it sends the listOfFile it finds in here.
// MoveData still trims this list to 20 - can take that limit out at some point.
// -------------------------------------------------------------------------------
// This process does these things:
// 1.   read dat files from data/ins
// 2.   MOVE them to data/out
// 3.   Send a message to the dispatcher queue.
// -------------------------------------------------------------------------------

import com.unilibre.MQConnector.activeMQ;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import com.unilibre.core.coreCommons;
import org.json.JSONObject;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.*;
import java.util.Properties;

public class InsertCollecter {

    private static final int MAXFILES = 100;
    private static final String incorrid = "Inserts for ";
    private static final String toQue = "900.SQL.Dispatcher";
    private static final String src = NamedCommon.slash + "ins" + NamedCommon.slash;
    private static final String tgt = NamedCommon.slash + "out" + NamedCommon.slash;
    private static final String heartbeat = "<<heartbeat>> ... InsertCollecter()";
    private static final String watchSTR = ".dat";
    private static String datDIR, tgtDir;
    private static int nbrFound=0, waiting=0, dCnt;

    public static void Prepare() {
        String opsys = System.getProperty("os.name");
        if (opsys.toLowerCase().contains("windows")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
            String slash = "/";
            NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
        }
        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);

        Properties sProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/this.server");
        String broker = sProps.getProperty("brokers", "");
        Properties bkrProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/"+broker);
        uCommons.BkrCommons(bkrProps);
        if (NamedCommon.ZERROR) System.exit(0);

        datDIR = NamedCommon.BaseCamp + NamedCommon.slash + "data" + src;
        tgtDir = NamedCommon.BaseCamp + NamedCommon.slash + "data" + tgt;
    }

    public static void handleFiles(String[] listOfFiles) {
        String msg, table, infile, outfile, base;
        String[] fParts;
        for (int i = 0; i < listOfFiles.length; i++) {
            infile = listOfFiles[i];
            if (infile.equals("")) continue;
            table = RunControl.GetFileOnly(infile);
            //
            // start with [demo]_[raw]_[CLIENT_RFUEL]_210625_123456.dat
            //
            base = table.replaceFirst("\\.dat$", "");
            fParts = base.split("]");
            //
            // finish with [demo]_[raw]_[CLIENT_RFUEL]
            //
            table = fParts[0] + "]" + fParts[1] + "]" + fParts[2] + "]";

            outfile = infile.replace(src, tgt);
            Path source = Paths.get(infile);
            Path target = Paths.get(outfile);
            if (safeMove(source, target, table)) {
                dCnt++;
                if (dCnt % 500 == 0) {
                    uCommons.uSendMessage("Dispatched " + uCommons.RightHash(uCommons.oconvM(String.valueOf(dCnt), "MD0~"), 6) + " files");
                }
            }
        }
    }

    public static boolean safeMove(Path sourceDir, Path targetDir, String table) {
        try {
            if (Files.exists(sourceDir) && !Files.exists(targetDir)) {
                Files.copy(sourceDir, targetDir, StandardCopyOption.REPLACE_EXISTING);
                JSONObject jObj = new JSONObject();
                jObj.put("table", table);
                jObj.put("fqpn", targetDir.toString());
                activeMQ.produce(NamedCommon.messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, incorrid + table, toQue, jObj.toString());
                jObj = null;
                if (Files.exists(targetDir)) Files.delete(sourceDir);
                return true;
            }
        } catch (AtomicMoveNotSupportedException e) {
            uCommons.uSendMessage("ERROR: " + e.getMessage());
            uCommons.uSendMessage("Atomic move not supported. Fallback or investigate FileSafe compatibility.");
        } catch (IOException e) {
            uCommons.uSendMessage("Failed to move " + sourceDir);
            uCommons.uSendMessage(e.getMessage());
            File checkDir = new File(NamedCommon.BaseCamp + tgt);
            if (!checkDir.exists()) checkDir.mkdir();
        }
        return false;
    }

    public static void FileWatcher() throws IOException, InterruptedException {
        File dir = new File(datDIR);
        File[] datFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(watchSTR);
            }
        });

        dir = null;

        if (datFiles != null) {
            String thisFile;
            String[] fParts;
            for (File file : datFiles) {
                nbrFound++;
                Path sourceDir = Paths.get(datDIR+file.getName());
                Path targetDir = Paths.get(tgtDir+file.getName());
                thisFile = file.getName();
                thisFile = thisFile.replaceFirst("\\.dat", "");
                fParts = thisFile.split("]");
                thisFile = fParts[0] + "]" + fParts[1] + "]" + fParts[2] + "]";
                safeMove(sourceDir, targetDir, thisFile);
                sourceDir=null;
                targetDir=null;
                fParts   =null;
                if (nbrFound > MAXFILES) break;
            }
        } else {
            System.out.println("Directory not found or inaccessible.");
        }
        datFiles = null;
    }

    public static void main(String[] args) {
        if (NamedCommon.BaseCamp.contains("/home/andy")) NamedCommon.BaseCamp = NamedCommon.DevCentre;

        for (int i=0 ; i<3; i++) { System.out.println(" "); }
        Prepare();
        uCommons.uSendMessage("Collecter has started ...");
        uCommons.uSendMessage("Dispatching files from "+datDIR+"  ....");
        uCommons.uSendMessage("                    to " + NamedCommon.BaseCamp + NamedCommon.slash + "data" + tgt);
        for (int i=0 ; i<3; i++) { System.out.println(" "); }

        while (!coreCommons.StopNow()) {
            try {
                nbrFound=0;
                FileWatcher();
                if (nbrFound == 0) {
                    waiting++;
                    if (waiting > MAXFILES) {
                        uCommons.uSendMessage(heartbeat);
                        waiting=0;
                    }
                } else {
                    waiting=0;
                }
            } catch (IOException e) {
                uCommons.uSendMessage("ERROR 1 : " + e.getMessage());
            } catch (InterruptedException e) {
                uCommons.uSendMessage("ERROR 2 : " + e.getMessage());
            }
            uCommons.Sleep(0);
        }
    }

}
