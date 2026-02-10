package com.unilibre.dataworks;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.commons.*;
import java.io.*;
import java.nio.channels.FileLock;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;


public class RunControl {

    private static int nbrFiles, maxFiles=20;
    private static long sent, got;
    private static double laps, div = 1000000000.00;
    private static boolean loaded = false, checkVolume=true, cVolumes=true;
    private static DecimalFormat df = new DecimalFormat("#0.00");
    private static FileLock[] lockArr = null;
    private static String opsys = System.getProperty("os.name"), conf=NamedCommon.BaseCamp+NamedCommon.slash+"conf"+NamedCommon.slash;
    private static String slowfile = "";

    private static String spcr = "------------------------------------------------";

    public static void SetSlowfile(String process) {
        slowfile = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash + "SLOW"+process;
    }

    public static void SetMaxFiles(int inval) {
        maxFiles = inval;
    }

    public static void SetVolumes(boolean inval) {
        cVolumes = inval;
    }

    public static boolean GetVolumes() {
        return cVolumes;
    }

    public static boolean ThingsToDo() {
        checkVolume = GetVolumes();
        if (NamedCommon.isRDS) {
            if (NamedCommon.RDSdir.equals("")) {
                uCommons.uSendMessage("FATAL: configured for RDS but no S3 bucket is defined!!");
                String stopFile = "";
                uCommons.WriteDiskRecord(stopFile, "STOP");
                NamedCommon.ZERROR = true;
                return false;
            }
            String cmd = "aws s3 mv " + MoveData.fLocn + " s3://" + NamedCommon.RDSdir + " --recursive --exclude \"*\" --include \"*.dat\"";
            if (opsys.toLowerCase().contains("windows")) cmd = "cmd /c " + cmd;
            uCommons.nixExecute(cmd, false);
            // ---------------------------------------------------------------------
            // awk returns ONLY the file names with leading or trailing spaces.
            // ---------------------------------------------------------------------
            cmd = "aws s3 ls s3://" + NamedCommon.RDSdir + " | awk '{print $4}'";
            String fileList =uCommons.nixExecCmd(cmd, 5);
            ArrayList<String> filearr = new ArrayList<String>(Arrays.asList(fileList.split("\\r?\\n")));
            if (filearr.get(0).equals("")) filearr.remove(0);
            MoveData.ListOfFiles = new String[filearr.size()];
            for (int i=0 ; i < filearr.size(); i++) { MoveData.ListOfFiles[i] = ""; }
            int arrCnt=0, eof=filearr.size(), datCnt = 0;
            String item = "";
            while (arrCnt < eof) {
                datCnt = 0;
                while (datCnt < 100 && arrCnt < eof) {
                    item = filearr.get(arrCnt);
                    arrCnt++;
                    if (item.equals("")) continue;
                    System.out.println(item);
                    MoveData.ListOfFiles[datCnt] = item;
                    datCnt++;
                }
            }
            if (MoveData.ListOfFiles.length > 0) System.exit(0);
        } else {
            MoveData.ListOfFiles = GetFileNames();
        }
        nbrFiles = MoveData.ListOfFiles.length;
        if (nbrFiles > 0 && (!MoveData.ListOfFiles[0].equals(""))) {
            MoveData.heartbeat = false;
            return true;
        } else {
            return false;
        }
    }

    public static void MoveFiles() {

        if (NamedCommon.uCon == null) {
            if (!NamedCommon.tConnected) SqlCommands.ConnectSQL();
            if (!NamedCommon.tConnected) return;
            NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
        }

        MoveData.busy = true;
        boolean remServer = (!MoveData.remote.equals(""));
        MoveData.datfiles = new File(MoveData.fLocn);
        if (!NamedCommon.isRDS) {
            boolean junk;
            junk = MoveData.datfiles.setReadable(true, false);
            junk = MoveData.datfiles.setWritable(true, false);
            junk = MoveData.datfiles.setExecutable(true, false);
        }

        MoveData.ok2delete = false;
        String cmd = MoveData.scp + MoveData.remote;
        String ThisFile = "";
        String FileName = "";
        String CopyThis = "";
        String DelThis = "";
        nbrFiles = MoveData.ListOfFiles.length;
        String fBytes;
        if (NamedCommon.debugging) {
            uCommons.uSendMessage("   * ------------------------------------------------------");
            uCommons.uSendMessage("Load " + nbrFiles + " dat file(s)");
            uCommons.uSendMessage("   *");
        }
        for (int i = 0; i < nbrFiles; i++) {
            ThisFile = MoveData.ListOfFiles[i];
            if (ThisFile.equals("")) continue;
            MoveData.baseFle = ThisFile;
            if (NamedCommon.isRDS) {
                uCommons.uSendMessage("Loading: "+MoveData.fLocn + NamedCommon.slash+ThisFile);
                sent = System.nanoTime();
                RDSLoader(ThisFile);
                got = System.nanoTime();
                laps = (got - sent) / div;
                uCommons.eMessage += " Runtime: " + df.format(laps) + " seconds";
                uCommons.uSendMessage(uCommons.eMessage);
                cmd = "aws s3 rm s3://" + NamedCommon.RDSdir + "/" + ThisFile;
                uCommons.nixExecute(cmd, false);
            } else {
                File fl = new File(ThisFile);
                if (fl.canRead()) {
                    FileName = GetFileOnly(ThisFile);
                    if (SqlConnector(FileName)) {
                        MoveData.heartbeat = false;
                        uCommons.uSendMessage("Loading:   .) " + FileName);
                        fBytes = String.valueOf(fl.length());
                        sent = System.nanoTime();
                        BulkLoader(FileName);
                        got = System.nanoTime();
                        laps = (got - sent) / div;
                        if (loaded) {
                            if (NamedCommon.SqlDBJar.equals("MSSQL")) {
                                if (!NamedCommon.ZERROR && !NamedCommon.StopNow.contains("FAIL")) {
                                    uCommons.eMessage = NamedCommon.StopNow + "   .) Successfully loaded " +
                                            uCommons.oconvM(fBytes, "MD0,") + " bytes. ";
                                } else {
                                    uCommons.eMessage = NamedCommon.StopNow + "   .) FAILED to load file. ";
                                }
                            }
                        }
                        // eMessage is built above
                        uCommons.eMessage += " Runtime: " + df.format(laps) + " seconds";
                        uCommons.uSendMessage(uCommons.eMessage);

                        if (NamedCommon.ZERROR) return;
                        if (MoveData.ok2delete) {
                            if (remServer) {
                                CopyThis = cmd.replace("$", ThisFile);
                                DelThis = MoveData.ssh + "  " + MoveData.sqlHost + " \"rm " +
                                        MoveData.sqlDir + "/" + FileName + "\"";
                                uCommons.nixExecute(DelThis, true);
                                uCommons.nixExecute(CopyThis, true);
                            } else {
                                MoveData.DeleteFiles.add(ThisFile);
                            }
                        }
                    } else {
                        MoveData.BadFiles.add(MoveData.baseFle);
                    }
                }
            }
        }
        RemoveLogs();
        RemoveInsertedData();
        uCommons.uSendMessage("           .) Done. ");
    }

    public static String GetFileOnly(String thisFile) {
        int nbrSlash = 0;
        thisFile = thisFile.replace("\\", "/");
        nbrSlash = thisFile.length() - thisFile.replace("/", "").length();
        thisFile = uCommons.FieldOf(thisFile, "/", (nbrSlash + 1));
        return thisFile;
    }

    public static void RemoveInsertedData() {
        // check if KeepData is on (MoveData Keep dat files)
        boolean remServer = (!MoveData.remote.equals(""));
        if (!remServer) Deletefiles();
    }

    public static void Deletefiles() {
        int d=0;
        for (int i=0 ; i < MoveData.DeleteFiles.size() ; i++) {
            if (MoveData.DeleteFiles.get(i).equals("")) MoveData.DeleteFiles.remove(i);
        }
        while (!MoveData.DeleteFiles.isEmpty() && d==0) {
            String fname = MoveData.DeleteFiles.get(d);
            if (fname.equals("")) { MoveData.DeleteFiles.remove(d) ; continue; }
            File rTmp = new File(fname);
            if (!rTmp.exists()) {
                rTmp = null;
                rTmp = new File(fname+".load.done");
            }
            if (rTmp.exists() && rTmp.canWrite()) {
                if (NamedCommon.KeepData && !MoveData.isRaw) {
                    uCommons.RenameFile(fname, NamedCommon.BaseCamp+MoveData.External+GetFileOnly(fname));
                    uCommons.uSendMessage("           .) Moved to " + NamedCommon.BaseCamp+MoveData.External+GetFileOnly(fname));
                } else {
                    rTmp.delete();
                    uCommons.uSendMessage("           .) Deleted " + GetFileOnly(fname));
                }
                if (rTmp.exists()) {
                    if (NamedCommon.debugging) uCommons.uSendMessage("           .) " + rTmp.getName() + "    will not delete.");
                    uCommons.Sleep(1);
                } else {
                    MoveData.DeleteFiles.remove(d);
                }
            } else {
                MoveData.BadFiles.add(MoveData.DeleteFiles.get(d));
                if (NamedCommon.debugging) uCommons.uSendMessage("           .) " + MoveData.DeleteFiles.get(d) + " already deleted");
                MoveData.DeleteFiles.remove(d);
            }
        }
        if (MoveData.DeleteFiles.size() > 0) uCommons.uSendMessage("           .) " + MoveData.DeleteFiles.size()+" files left to be deleted.");
    }

    public static void RemoveLogs() {
        String hold1 = MoveData.matchStr;
        String hold2 = MoveData.ddir;
        MoveData.ddir = NamedCommon.BaseCamp + "/data/ins/logs/";
        MoveData.matchStr = "log";
        if (NamedCommon.debugging) uCommons.uSendMessage("-------- looking for logs --------");
        checkVolume = false;
        String[] dirtyFiles = GetFileNames();
        checkVolume = GetVolumes();
        int badFiles = dirtyFiles.length;
        if (badFiles > 0) {
            MoveData.matchStr = "log";
            dirtyFiles = GetFileNames();
            badFiles = dirtyFiles.length;
            if (badFiles > 0 && (!dirtyFiles[0].equals(""))) {
                RenameThese(dirtyFiles);
            }
            MoveData.matchStr = "Txt";
            dirtyFiles = GetFileNames();
            badFiles = dirtyFiles.length;
            if (badFiles > 0 && (!dirtyFiles[0].equals(""))) {
                RenameThese(dirtyFiles);
            }
        }
        dirtyFiles = null;
        MoveData.matchStr = hold1;
        MoveData.ddir = hold2;
    }

    private static void RenameThese(String[] listOfFile) {
        int nbrmatches = listOfFile.length;
        String tmp = "";
        uCommons.uSendMessage("found " + nbrmatches + " data error(s) ... removing now");
        for (int ff = 0; ff < nbrmatches; ff++) {
            tmp = String.valueOf(listOfFile[ff]);
            uCommons.uSendMessage("Reading data from " + tmp);
            String LogFile = uCommons.ReadDiskRecord(tmp);
            File rTmp = new File(tmp);
            if (rTmp.exists()) {
                String newFile = tmp + ".fail";
                uCommons.uSendMessage("         >>       " + newFile);
                try {
                    BufferedWriter bWriter = new BufferedWriter(new FileWriter(newFile));
                    bWriter.write(LogFile);
                    bWriter.flush();
                    bWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                rTmp.delete();
            }
        }
    }

    public static String[] GetFileNames() {
        if (slowfile.equals("")) {
            if (MoveData.isRaw) {
                SetSlowfile("fetch");
            } else {
                SetSlowfile("burst");
            }
        }
        // ----------------------------------------------------------------------------------------------
        String[] returnFiles   = new String[maxFiles];
        for (int f=0; f < maxFiles;  f++) { returnFiles[f]=""; }

        File directory = new File(MoveData.ddir);
        File[] datFilesArray = directory.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(MoveData.matchStr);
            }
        });

        if (datFilesArray == null) return returnFiles;
        int max = datFilesArray.length;

        // ----------------------------------------------------------------------------------------------
        int nbrmatches=0;
        String tmp;
        int fpos = 0;
        for (int f=0; f < max; f++) {
            if (datFilesArray[f] != null) {
                tmp = datFilesArray[f].toString();
                if (MoveData.isRaw) {
                    if (!tmp.contains("raw")) continue;
                } else {
                    if (tmp.contains("raw")) continue;
                }
                nbrmatches++;
                if (fpos < maxFiles) {
                    if (MoveData.BadFiles.indexOf(tmp) < 0) {
                        returnFiles[fpos] = tmp;
                        fpos++;
                    }
                }
            }
        }
        // ----------------------------------------------------------------
        if (checkVolume) {
            if (nbrmatches >= maxFiles) {
                if (!uCommons.FileExists(slowfile)) {
                    uCommons.uSendMessage("*** ----------------- ( " + nbrmatches + " ) ----------------------");
                    uCommons.uSendMessage("*** -------------  wait for target DB ----------------------");
                    uCommons.uSendMessage("*** --------------------------------------------------------");
                    uCommons.WriteDiskRecord(slowfile, "10");        // sleep for 10 seconds in 012 & 014
                }
            } else {
                if (uCommons.FileExists(slowfile)) {
                    uCommons.uSendMessage("*** ----------------- ( " + nbrmatches + " ) ----------------------");
                    uCommons.uSendMessage("*** --------- release SLOW setting -------------------------");
                    uCommons.uSendMessage("*** --------------------------------------------------------");
                    uCommons.DeleteFile(slowfile);
                }
            }
        }
        // ----------------------------------------------------------------
        // make sure the older ones get inserted first.
        Arrays.sort(returnFiles, Collections.reverseOrder());
        return returnFiles;
    }

    private static boolean SqlConnector(String thisFile) {
        if (thisFile.endsWith(MoveData.matchStr)) {
            String cDB = uCommons.FieldOf(thisFile, "\\[", 2);
            cDB = uCommons.FieldOf(cDB, "\\]", 1);
            String cSch= uCommons.FieldOf(thisFile, "\\[", 3);
            cSch= uCommons.FieldOf(cSch, "\\]", 1);
            if (cDB.startsWith("~") || cSch.startsWith("~")) {
                String fileOnly = thisFile.replace("." + MoveData.matchStr, "") + ".hosts";
                fileOnly = MoveData.fLocn + fileOnly;
                File rTmp = new File(fileOnly);
                if (rTmp.exists() && rTmp.canWrite()) {
                    NamedCommon.tHostList = new ArrayList<>(Arrays.asList(uCommons.ReadDiskRecord(fileOnly).split("\\r?\\n")));
                    String url = "";
                    String usr = "";
                    String pwd = "";
                    for (int h = 0; h < NamedCommon.tHostList.size(); h++) {
                        String thisHost = NamedCommon.tHostList.get(h).trim();
                        if (!thisHost.equals("")) {
                            // GITLAB 19
                            uCommons.GetThostDetails(thisHost);
                            if (!NamedCommon.ZERROR) {
                                if (cDB.equals("$DB$") || cDB.equals("~DB~")) {
                                    NamedCommon.SqlDatabase = APImsg.APIget("sqldb:" + thisHost);
                                    if (NamedCommon.SqlDatabase.equals(""))
                                        NamedCommon.SqlDatabase = APImsg.APIget("sqldb");
                                } else {
                                    NamedCommon.SqlDatabase = cDB;
                                }
                                if (cSch.equals("$SC$") || cSch.equals("~SC~")) {
                                    NamedCommon.SqlSchema = APImsg.APIget("schema:" + thisHost);
                                } else {
                                    NamedCommon.SqlSchema = cSch;
                                }
                                url = APImsg.APIget("jdbccon:" + thisHost);
                                usr = APImsg.APIget("jdbcusr:" + thisHost);
                                pwd = APImsg.APIget("jdbcpwd:" + thisHost);
                                String jdbcDBI = APImsg.APIget("sqldb:" + thisHost);
                                String jdbcSCH = APImsg.APIget("schema:" + thisHost);
                                String chk = url + "+" + jdbcDBI + "+" + jdbcSCH;
                                if (ConnectionPool.objPool.indexOf(chk) < 0) {
                                    try {
                                        ConnectionPool.AddToPool(url, usr, pwd);
                                        if (!NamedCommon.ZERROR) {
                                            ConnectionPool.objPool.add(chk);
                                        }
                                    } catch (SQLException e) {
                                        NamedCommon.ZERROR = true;
                                        NamedCommon.Zmessage = e.getMessage();
                                        uCommons.uSendMessage(e.getMessage());
                                    }
                                }
                            }
                            // ---------
                        }
                    }
                } else {
                    uCommons.uSendMessage(fileOnly + " permission issues. Skipping this file.");
                }
            }
        }
        return (!NamedCommon.ZERROR);
    }

    public static void BulkLoader(String thisFile) {

        MoveData.DDL.clear();
        MoveData.DDL.clear();

        if (NamedCommon.BulkLoad) {
            String BulkImport = SqlCommands.BulkImport(thisFile, NamedCommon.datPath);
            if (!BulkImport.equals("")) {
                if (NamedCommon.debugging) uCommons.uSendMessage(BulkImport);
                MoveData.DDL.add(BulkImport);
                BulkImport = BulkImport.replaceAll("\\\\", "/");
                SqlCommands.ExecuteSQL(MoveData.DDL);
                if (NamedCommon.StopNow.contains("<<FAIL>>") || NamedCommon.ZERROR) {
                    HoldTheFile(MoveData.fLocn,thisFile);
                    MoveData.failCnt++;
                    NamedCommon.ZERROR = false;
                    NamedCommon.Zmessage = "";
                } else {
                    loaded = true;
                    MoveData.ok2delete = true;
                }
            } else {
                uCommons.uSendMessage("     Bulk Statement empty ......");
            }
        } else {
            ArrayList insLines = new ArrayList<>(Arrays.asList(uCommons.ReadDiskRecord(MoveData.fLocn+thisFile).split("\\r?\\n")));
            uCommons.uSendMessage("           .) Sending commands for execution");
            SqlCommands.ExecuteSQL(insLines);
            if (NamedCommon.StopNow.contains("<<FAIL>>") || NamedCommon.ZERROR) {
                uCommons.uSendMessage("           .) Errors for found.");
                uCommons.uSendMessage("           .) StopNow: " + NamedCommon.StopNow);
                uCommons.uSendMessage("           .)  ZERROR: " + NamedCommon.ZERROR);
                HoldTheFile(MoveData.datPath, thisFile);
                MoveData.failCnt++;
                NamedCommon.ZERROR = false;
                NamedCommon.Zmessage = "";
            } else {
                loaded = true;
                MoveData.ok2delete = true;
            }
        }
        MoveData.DDL.clear();
    }

    public static void RDSLoader(String thisFile) {
        BulkLoader(thisFile);;
    }

    private static void HoldTheFile(String datPath, String thisFile) {
        File dfile = new File(MoveData.fLocn + thisFile);
        File efile = new File(MoveData.fLocn + MoveData.eLocn, thisFile);
        if (dfile.renameTo(efile)) {
            if (!NamedCommon.SqlReply.equals("")) {
                uCommons.eMessage = "         Relocated to " + MoveData.fLocn + MoveData.eLocn + thisFile;
                NamedCommon.Zmessage = "";
                uCommons.uSendMessage(uCommons.eMessage);
            } else {
                uCommons.uSendMessage("           .)  Relocated to " + efile.getPath() + " " + efile.getName());
            }
//            MoveData.ok2delete = false;
        } else {
            uCommons.uSendMessage("Cannot relocate " + datPath + thisFile + " --> it will be deleted");
//            MoveData.ok2delete = true;
        }
        MoveData.ok2delete = true;
        dfile = null;
        efile = null;
    }

//    public static String[] ReadFileNames(String indir, String inext) {
//        //
//        // indir    should be fqdn
//        // inext    okay !
//        // chkBad   boolean     check .BadFiles true / false
//        //
//        String lookin;
//        if (!indir.startsWith(NamedCommon.BaseCamp)) {
//            lookin = NamedCommon.BaseCamp + indir;
//        } else {
//            lookin = indir;
//        }
//        final String matchStr = inext;
//        File dir = new File(lookin);
//        File[] matchFiles = dir.listFiles(new FilenameFilter() {
//            public boolean accept(File dir, String name) {
//                return name.endsWith(matchStr);
//            }
//        });
//        int nbrmatches;
//        if (matchFiles == null) {
//            nbrmatches = 0;
//        } else {
//            nbrmatches = matchFiles.length;
//        }
//        String[] matchingFiles = new String[nbrmatches];
//        for (int ff = 0; ff < nbrmatches; ff++) {
//            matchingFiles[ff] = String.valueOf(matchFiles[ff]);
//        }
//        return matchingFiles;
//    }

}
