package com.unilibre.dataworks;

import com.unilibre.commons.ConnectionPool;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SqlCommands;
import com.unilibre.commons.uCommons;
import net.snowflake.client.jdbc.SnowflakeConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class sfLoadData {

    private static boolean isRaw = false;
    private static boolean isDbg = false;
    private static String fPath = "/upl/data/ins/";
    private static String matchStr = "dat";
    private static String csv = "csv";
    private static String useCase = "";
    private static ArrayList<String> DDL = new ArrayList<>();
    private static ArrayList<String> SPROC = new ArrayList<>();
    private static ArrayList<String> STAGE = new ArrayList<>();
    private static ArrayList<String> FFMT = new ArrayList<>();
    private static int wMax = 2000;
    private static double laps, div = 1000000000.00;
    private static FileInputStream fis;

    public static void main(String[] args) throws Exception {
        if (args.length > 0) isRaw = args[0].toUpperCase().equals("RAW");
        if (args.length > 1) isDbg = args[1].toUpperCase().equals("DEBUG");

        useCase = " [raw] ";
        if (!isRaw) useCase = " [uni] ";
        uCommons.uSendMessage("sfLoadData "+ useCase +"-----------------------------------------------------------------------------");

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            fPath = NamedCommon.BaseCamp + "/data/ins/";
            RenameDATfiles();
        }

        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(1);
        uCommons.SetCommons(rProps);
        SqlCommands.ConnectSQL();
        if (NamedCommon.ZERROR) return;
        NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);

        uCommons.uSendMessage("   BaseCamp   :: " + NamedCommon.BaseCamp);
        uCommons.uSendMessage("   fpath      :: " + fPath);
        uCommons.uSendMessage("     raw      :: " + isRaw);
        
        ProcessFiles();
        NamedCommon.ZERROR=false;
        NamedCommon.Zmessage="";
        SqlCommands.DisconnectSQL();
    }

    private static void ProcessFiles() {
        InitialiseRun();
        if (NamedCommon.StopNow.equals("<<FAIL>>")) return;

        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("Send2Snowflake ----------------------------------------------------------");
                uCommons.uSendMessage("looking for " + useCase + "  in " + fPath);
        boolean okay = true;
        int noData=wMax;
        while (true) {

            // Get a bunch of *.dat files (with or w/out raw in its name
            // rename from .dat to .csv to isolate them.

            String[] MoveFiles = GetFiles();
            int lx = MoveFiles.length, procCnt=0;
            if (lx > 5) lx = 5;                 // max 5 files in a tranche - other processes
            String[] useFiles = new String[lx];
            for (int x=0; x<lx; x++) {useFiles[x] = ""; }
            String tmp,ren;

            if (lx > 0) {
                if (!MoveFiles[0].equals("") && isDbg) uCommons.uSendMessage("   ...  rename " + lx + " files");
                for (int i = 0; i < lx; i++) {
                    tmp = MoveFiles[i];
                    if (tmp.equals("")) continue;
                    if (tmp.endsWith(matchStr)) {
                        ren = tmp.replace("." + matchStr, "." + csv);
                        if (!uCommons.RenameFile(tmp, ren)) continue;
                        tmp = ren;
                        useFiles[i] = ren;
                        procCnt++;
                    } else {
                        continue;
                    }
                }
            }

            if (procCnt == 0) {
                uCommons.Sleep(1);
                noData++;
                if (noData > wMax) {
                    uCommons.uSendMessage("[heartbeat] waiting for data ....");
                    noData = 0;
                }
                continue;
            }

            lx = procCnt;
            if (lx > 0) {
                if (isDbg) uCommons.uSendMessage("   ... process " + lx + " files");
                String[] done = new String[lx];
                for (int x = 0; x < lx; x++) {done[x] = "";}
                for (int i = 0; i < lx; i++) {
                    if (useFiles[i].equals("")) continue;
                    okay = CheckStoredProcs(useFiles[i]); if (!okay) break;
                    okay = CheckStageNames(useFiles[i]);  if (!okay) break;
                    okay = Send2Snowflke(useFiles[i]);
                    if (okay) {
                        done[i] = useFiles[i];
                    } else {
                        break;
                    }
                }
                if (!okay) break;
                if (isDbg) uCommons.uSendMessage("   ... remove "+done.length+" processed files");
                RemoveLoadedFiles(done);
            }
        }
    }

    private static boolean Send2Snowflke(String moveFile)  {
        // ---------------------------------------------------------------------------------------------------------
        // rFuel --> *.dat --> @{internal stage} --> tmp{fname} --> fname using INSERT INTO
        // DO NOT allow duplicate rows !
        // ---------------------------------------------------------------------------------------------------------
        boolean okay = false;
        NamedCommon.startM = System.nanoTime();
        String thisFile="", thisPath="";
        moveFile = moveFile.replace("\\", "/");
        thisFile = GetFileName(moveFile);
        thisPath = moveFile.replace(thisFile, "");

        String SCH = GetSC(moveFile);
        String TBL = GetFL(moveFile);
        String sfStageName = SCH + "." + TBL;

        // IMPORTANT - please read !! --------------------------------------
        // On site, it is likely that TBL will change regularly
        // However, when isRaw, SCH will ALWAYS be RAW

        uCommons.uSendMessage("... loading      :: " + thisPath + thisFile);

        if (!uCommons.FileExists(thisPath + thisFile)) {
            uCommons.uSendMessage("... skip         :: loaded by another processor.");
            return true;
        }

        try {
            // load dat file into @{stage}
            fis = new FileInputStream(moveFile);
            NamedCommon.uCon
                    .unwrap(SnowflakeConnection.class)
                    .uploadStream(
                            sfStageName,
                            "",
                            fis,
                            thisFile,
                            true);
            NamedCommon.uCon.commit();
        } catch (FileNotFoundException fnf) {
            uCommons.uSendMessage(fnf.getMessage());
            NamedCommon.StopNow = "<<FAIL>>";
            return okay;
        } catch (SQLException sqlx) {
            uCommons.uSendMessage(sqlx.getMessage());
            NamedCommon.StopNow = "<<FAIL>>";
            return okay;
        }
        fis = null;

        uCommons.uSendMessage("... CALL UPL.LOADER('" + SCH + "','" + TBL+ "');");
        String postExec = "CALL UPL.LOADER('" + SCH + "','" + TBL+ "');";
        DDL.clear();
        DDL.add(postExec);
        SqlCommands.ExecuteSQL(DDL);
        if (NamedCommon.StopNow.equals("<<FAIL>>")) return okay;

        uCommons.uSendMessage("... Done.");

        uCommons.uSendMessage(NamedCommon.StopNow);
        NamedCommon.lastM = System.nanoTime();
        laps = (NamedCommon.lastM - NamedCommon.startM);
        laps = laps / div;
        uCommons.uSendMessage(laps + " seconds  ");
        uCommons.uSendMessage("---------------------------------------------------------------");
        okay = true;
        return okay;
    }

    private static void InitialiseRun() {
        String[] schemas = new String[]{"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""};
        String exe="SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA;";

        // Get all SCHEMAS

        Statement stmt = null;
        ResultSet rs;
        try {
            stmt = NamedCommon.uCon.createStatement();
            rs = stmt.executeQuery(exe);
            int s=0;
            while (rs.next()) {
                schemas[s] = rs.getString("SCHEMA_NAME");
                s++;
                if (s > 20) {
                    uCommons.uSendMessage("email support@unilibre.com - Too many schemas to initialise.");
                    NamedCommon.StopNow = "<<FAIL>>";
                    return;
                }
            }
            stmt.close();
            rs.close();
            rs = null;
            stmt = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // Get all stored Procedures

        String sch, db, sc, ob, cmd;
        uCommons.uSendMessage("Collecting Stored Procedures ....");
        exe="SHOW PROCEDURES";
        for (int s=0; s<schemas.length; s++) {
            sch=schemas[s];
            if (sch.equals("")) continue;
            DDL.clear();
            DDL.add("USE SCHEMA "+sch);
            SqlCommands.ExecuteSQL(DDL);
            if (NamedCommon.StopNow.equals("<<FAIL>>")) continue;
            NamedCommon.StopNow = "";
            try {
                stmt = NamedCommon.uCon.createStatement();
                rs = stmt.executeQuery(exe);
                while (rs.next()) {
                    db = rs.getString("catalog_name");
                    sc = rs.getString("schema_name");
                    ob = rs.getString("name");
                    if (!sc.equals(sch)) continue;
                    ob = db + "." + sc + "." + ob;
                    if (SPROC.indexOf(ob) < 0) SPROC.add(ob);
                }
                stmt.close();
                rs.close();
                rs = null;
                stmt = null;
            } catch (SQLException sqlx) {
                uCommons.uSendMessage(sqlx.getMessage());
                NamedCommon.StopNow = "<<FAIL>>";
                return;
            }

        }

        // Get all Stages

        uCommons.uSendMessage("Collecting internal stages ....");
        exe="SHOW STAGES";
        for (int s=0; s<schemas.length; s++) {
            sch=schemas[s];
            if (sch.equals("")) continue;
            DDL.clear();
            DDL.add("USE SCHEMA "+sch);
            SqlCommands.ExecuteSQL(DDL);
            if (NamedCommon.StopNow.equals("<<FAIL>>")) continue;
            NamedCommon.StopNow = "";
            try {
                stmt = NamedCommon.uCon.createStatement();
                rs = stmt.executeQuery(exe);
                while (rs.next()) {
                    db = rs.getString("database_name");
                    sc = rs.getString("schema_name");
                    ob = rs.getString("name");
                    if (!sc.equals(sch)) continue;
                    ob = db + "." + sc + "." + ob;
                    if (STAGE.indexOf(ob) < 0) STAGE.add(ob);
                }
                stmt.close();
                rs.close();
                rs = null;
                stmt = null;
            } catch (SQLException sqlx) {
                uCommons.uSendMessage(sqlx.getMessage());
                NamedCommon.StopNow = "<<FAIL>>";
                return;
            }

        }

        // Get all File Formatters

        uCommons.uSendMessage("Collecting file formats ....");
        exe="SHOW FILE FORMATS";
        for (int s=0; s<schemas.length; s++) {
            sch=schemas[s];
            if (sch.equals("")) continue;
            DDL.clear();
            DDL.add("USE SCHEMA "+sch);
            SqlCommands.ExecuteSQL(DDL);
            if (NamedCommon.StopNow.equals("<<FAIL>>")) continue;
            NamedCommon.StopNow = "";
            try {
                stmt = NamedCommon.uCon.createStatement();
                rs = stmt.executeQuery(exe);
                while (rs.next()) {
                    db = rs.getString("database_name");
                    sc = rs.getString("schema_name");
                    ob = rs.getString("name");
                    if (!sc.equals(sch)) continue;
                    ob = db + "." + sc + "." + ob;
                    if (FFMT.indexOf(ob) < 0) FFMT.add(ob);
                }
                stmt.close();
                rs.close();
                rs = null;
                stmt = null;
            } catch (SQLException sqlx) {
                uCommons.uSendMessage(sqlx.getMessage());
                NamedCommon.StopNow = "<<FAIL>>";
                return;
            }

        }

    }

    private static boolean CheckStoredProcs(String moveFile) {
        boolean okay = true;
        String DB  = GetDB(moveFile);
        String chk = DB+".UPL.LOADER";
        if (SPROC.indexOf(chk) >= 0) return okay;

        String dataLoader = uCommons.ReadDiskRecord(NamedCommon.BaseCamp+NamedCommon.slash+"conf"+NamedCommon.slash+"sfLOADER.sp");
        Statement st = null;
        try {
            st = NamedCommon.uCon.createStatement();
            st.execute(dataLoader);
            st.close();
        } catch (SQLException e) {
            NamedCommon.StopNow = "<<FAIL>>";
            uCommons.uSendMessage(e.getMessage());
        }
        st=null;
        if (NamedCommon.StopNow.equals("<<FAIL>>")) return false;
        SPROC.add(chk);
        return okay;
    }

    private static boolean CheckStageNames(String moveFile) {
        boolean okay = true;
        String DB  = GetDB(moveFile);
        String SCH = GetSC(moveFile);
        String FL  = GetFL(moveFile);
        String chk = DB+"." +SCH+ "."+FL;
        if (STAGE.indexOf(chk) >= 0) return okay;

        // First time seeing this Stage name - create and add to list.

        String cmd = "CREATE STAGE IF NOT EXISTS "+chk+"\n";
        DDL.clear();
        DDL.add(cmd);
        SqlCommands.ExecuteSQL(DDL);
        if (NamedCommon.StopNow.equals("<<FAIL>>")) return false;
        STAGE.add(chk);
        return okay;
    }

    private static String GetDB(String inFile) {
        String DB;
        DB  = uCommons.FieldOf(inFile, "\\[", 2).replaceAll("\\\\", "");
        DB  = uCommons.FieldOf(DB, "\\]", 1).replaceAll("\\\\", "");
        return DB.toUpperCase();
    }

    private static String GetSC(String inFile) {
        String SCH;
        SCH = uCommons.FieldOf(inFile, "\\[", 3).replaceAll("\\\\", "");
        SCH = uCommons.FieldOf(SCH, "\\]", 1).replaceAll("\\\\", "");
        if (!isRaw && NamedCommon.uniBase) SCH = "uni";
        return SCH.toUpperCase();
    }

    private static String GetFL(String infile) {
        String FL;
        FL = uCommons.FieldOf(infile, "\\[", 4).replaceAll("\\\\", "");
        FL = uCommons.FieldOf(FL, "\\]", 1).replaceAll("\\\\", "");
        return FL.toUpperCase();
    }

    private static String GetFileName(String inFile) {
        int nbrSlash = inFile.length() - inFile.replace("/", "").length();
        inFile = uCommons.FieldOf(inFile, "/", (nbrSlash + 1));
        return inFile;
    }

    private static String[] GetFiles() {
        File dir = new File(fPath);
        File[] matchFiles = null;
        String tmp = matchStr, ren;
        if (isRaw) tmp = "(raw) " + tmp;
        matchFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(matchStr);
            }
        });
        int nbrmatches;
        if (matchFiles == null) {
            nbrmatches = 0;
        } else {
            nbrmatches = matchFiles.length;
        }

        // make sure the older ones get inserted first.

        String[] matchingFiles = new String[nbrmatches];
        if (nbrmatches > 0) {
            for (int ff = 0; ff < nbrmatches; ff++) {matchingFiles[ff] = "";}
            tmp = "";
            int fpos = 0;
            for (int ff = 0; ff < nbrmatches; ff++) {
                tmp = String.valueOf(matchFiles[ff]);
                if (isRaw) {
                    if (!tmp.contains("raw")) continue;
                } else {
                    if (tmp.contains("raw")) continue;
                }
                matchingFiles[fpos] = tmp;
                fpos++;
            }
            if (fpos > 0 && isDbg) uCommons.uSendMessage("   ... found " + fpos);
        }
        dir = null;
        matchFiles = null;
        return matchingFiles;
    }

    private static void RemoveLoadedFiles(String[] loadFiles) {
        int eoi = loadFiles.length;
        File file;
        for (int i=0; i<eoi; i++) {
            if (loadFiles[i].equals("")) continue;
            file = new File(loadFiles[i]);
            if (!file.delete()) uCommons.uSendMessage("Cannot delete "+loadFiles[1]);
            file=null;
        }
    }

    private static String GetColumns(String cmd) {
        String cols="", col="";
        NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
        if (NamedCommon.uCon == null) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
            return cols;
        }

        try {
            Statement stmt = NamedCommon.uCon.createStatement();
            ResultSet rs = stmt.executeQuery(cmd);
            while (rs.next()) {
                col = rs.getString("name");
                if (!col.equals("")) cols += col + ",";
            }
            cols = cols.substring(0, cols.length()-1);
            stmt.close();
            rs.close();
            rs = null;
            stmt = null;
        } catch (SQLException sqlx) {
            uCommons.uSendMessage(sqlx.getMessage());
            NamedCommon.StopNow = "<<FAIL>>";
            return "";
        }
        return cols;
    }

    private static void RenameDATfiles() {
        System.out.println("Renaming csv back to dat files");
        NamedCommon.BaseCamp = NamedCommon.DevCentre;
        String fPath = NamedCommon.BaseCamp + "/data/ins/";
        String matchStr = "csv";
        File dir = new File(fPath);
        File[] matchFiles = null;
        String tmp = matchStr, ren;
        uCommons.uSendMessage("looking for " + tmp + "  in " + fPath);
        matchFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(matchStr);
            }
        });

        int nbrmatches = matchFiles.length;
        uCommons.uSendMessage("   ... found " + nbrmatches);
        if (nbrmatches > 0) {
            for (int ff = 0; ff < nbrmatches; ff++) {
                tmp = String.valueOf(matchFiles[ff]);
                if (isRaw) {
                    if (!tmp.contains("raw")) continue;
                } else {
                    if (tmp.contains("raw")) continue;
                }
                ren = tmp.replace(".csv", ".dat");
                if (!uCommons.RenameFile(tmp, ren)) continue;
            }
        }
        System.out.println("done.");
    }

}
