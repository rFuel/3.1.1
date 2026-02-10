package com.unilibre.core;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.UniFileException;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BurstSplit {
    public static final String dataTags = "*.-@", tilde = "~", skipStr = "@@SKIP@@";
    private static FileWriter oversize;
    public static UniDynArray LineArray;
    private static ArrayList<Integer> AVF, AVT, MVF, MVT, SVF, SVT, ABK, MBK, SBK;
    public static ArrayList<String> sqlCmds = new ArrayList<String>();
    public static ArrayList<String> admpDat = new ArrayList<String>();
    public static ArrayList DDL;
    public static String nullSamples = "";
    private static String sqlStmt, atIM, atFM, atVM, atSM;
    private static String lastRow = "", sqlREC = "", thisWorkfile = "", dropwkfl = "";
    public static String quote = NamedCommon.Quote;
    public static String Komma = NamedCommon.Komma;
    private static String fDir = MessageProtocol.data + "ins";
    private static String pSCH = NamedCommon.SqlSchema;
    public static String message = "";
    public static String ams = "";
    private static String aAMS = "";
    private static String mAMS = "";
    private static String sAMS = "";
    private static String burst = "burst", slowTable="";
    private static String slowfile = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash + "SLOW";
    private static String ddID = "", ddLookFor="";
    private static int thisAv = 0;
    private static int thisMv = 0;
    private static int thisSv = 0;
    private static int rowCtr = 0, datRows = 0, nulRecs = 0, bigRows = 0, mtRecs = 0, thisSize=0;
    private static int pNum = 1, admpCnt=1;
    private static int cmdBlockSize = 100;
    private static int AA = 0, BB = 0, OutputCtr = 5000;
    private static int AvDone, MvDone, SvDone;
    private static int Expected=0, Actual=0;
    private static ArrayList<ArrayList<Integer>> AllDone;
    private static long sent = 0, got = 0, laps = 0, nanoSecs = 1000000000;
    private static int rubbishCollector = 60;
    private static boolean transformed = false;
    private static boolean dumpData = true;
    private static boolean deepDive = false;
    private static boolean Resilience = false;
    private static boolean IamRestarting = false;
    private static ArrayList<String> tagsMatched;

    public static String ProcessMap(String instr) {
        Initialise();
        Expected=0;
        Actual=0;
        IamRestarting = false;
        message = instr;
        boolean proceed = false;
        String reply = "";
        if (!NamedCommon.tConnected) {
            if (!SqlCommands.ConnectSQL()) {
                proceed = false;
            } else {
                proceed = true;
            }
        } else {
            proceed = true;
        }

        if (proceed) {
            String map = NamedCommon.xMap;
            String fqn = NamedCommon.BaseCamp + "/maps/" + NamedCommon.xMap;
            if (NamedCommon.uniBase) pSCH = "uni";
            reply = ProcessData(map);
        } else {
            reply = "<<FAIL>> no Target DB available";
            if (IamRestarting) reply = "RESTART";
        }
        return reply;
    }

    private static String ProcessData(String map) {
        sqlCmds.clear();
        admpDat.clear();
        nulRecs = 0;
        mtRecs = 0;
        rowCtr = 0;
        datRows = 0;
        bigRows = 0;
        Actual  = 0;
        Expected= 0;
        String reply = "<<FAIL>>";

        if (ManageTask(map)) {
            reply = "<<PASS>>";
            if (!sqlCmds.isEmpty()) {
                if (!NamedCommon.isNRT) {
                    uCommons.SQLDump(sqlCmds);
                    if (NamedCommon.ADMP) {
                        admpCnt = uCommons.ADMPdump(admpCnt, admpDat);
                        admpDat.clear();
                        admpCnt++;
                    }
                } else {
                    SqlCommands.ExecuteSQL(sqlCmds);
                    if (NamedCommon.ZERROR) uCommons.uSendMessage(NamedCommon.Zmessage);
                }
                if (NamedCommon.ZERROR) return "<<FAIL>> See RunERRORS queue for details.";
                sqlCmds.clear();
                rowCtr = 0;
            } else {
                if (NamedCommon.RunType.toUpperCase().equals("REST")) {
                    sqlCmds.add(0, "INSERT INTO NULL (ERR) VALUES ('No Result');");
                    uCommons.SQLDump(sqlCmds);
                    if (NamedCommon.ADMP) {
                        admpCnt = uCommons.ADMPdump(admpCnt, admpDat);
                        admpDat.clear();
                        admpCnt++;
                    }
                    if (NamedCommon.ZERROR) return "<<FAIL>> See RunERRORS queue for details.";
                }
            }

            if (Expected != Actual) NamedCommon.zMarker = "DATAERR*";  // tag the log lines
            uCommons.uSendMessage("-------------------------------[ Run Reconciliation ]-------------------------------------");
            uCommons.uSendMessage("-  .)                                                                                    -");
            uCommons.uSendMessage("-  .) " + uCommons.LeftHash(Expected + " rows were expected", 83)+"-");
            uCommons.uSendMessage("-  .) " + uCommons.LeftHash(Actual + " rows were received from [raw] select", 83)+"-");
            uCommons.uSendMessage("-  .)                                                                                    -");
            uCommons.uSendMessage("------------------------------------------------------------------------------------------");
            if (!NamedCommon.isPrt) {
                if (Expected != Actual) {
                    uCommons.uSendMessage("   .) Initiate re-start processes.");
                    if (Resilience) {
                        uCommons.uSendMessage("   .) Resilience mode will ignore data already processed.");
                    } else {
                        uCommons.uSendMessage("   .) Deleting data from this run.");
                        uCommons.uSendMessage("      >   Wait for files to load.......................");
                        uCommons.WaitOnFiles();
                        uCommons.uSendMessage("      >   Remove data from this run....................");
                        RemoveThisRunData();
                    }
                    uCommons.uSendMessage("   .) Initiate re-run now.");
                    IamRestarting = true;
                    uCommons.uSendMessage("   .)    Source File: " + NamedCommon.u2Source);
                } else {
                    if (Resilience) h2dbServer.Cleanup();
                }
            }
            NamedCommon.zMarker = "";
        } else {
            if (NamedCommon.ZERROR) reply = "<<FAIL>> - see RunERRORS queue";
            // Do NOT cleaup the Workfile - will be needed when we restart
        }
        if (IamRestarting) reply = "RESTART";
        return reply;
    }

    public static boolean ManageTask(String map) {
        if (NamedCommon.H2Server && !IamRestarting) Resilience = h2dbServer.CreateServer();
        if (SqlCommands.GetBatching()) SqlCommands.SetBatching(false);
        atIM = NamedCommon.IMark; // + NamedCommon.IMark;
        atFM = NamedCommon.FMark; // + NamedCommon.FMark;
        atVM = NamedCommon.VMark; // + NamedCommon.VMark;
        atSM = NamedCommon.SMark; // + NamedCommon.SMark;

        if (!NamedCommon.tConnected) {
            SqlCommands.ConnectSQL();
            if (NamedCommon.ZERROR) return false;
        }

        NamedCommon.tblCols = NamedCommon.burstCols;

        if (NamedCommon.AMS.toLowerCase().equals("split") && NamedCommon.burstCols.contains("NestPosition")) {
            String[] tCols = NamedCommon.tblCols.split("\\,");
            tCols = SqlCommons.CheckCols(tCols);
            tCols = SqlCommons.SplitCols(tCols);
            String newCols = "";
            for (int xx = 0; xx < tCols.length; xx++) { newCols += "," + tCols[xx]; }
            NamedCommon.tblCols = newCols.substring(1, newCols.length()).replaceAll("\\ \\ ", " ").replaceAll("\\ ", ",");
            newCols = "";
        }

        DDL = new ArrayList<String>();
        UniDynArray inRec;
        int lapse = 0;
        int grandTot = 0;
        int pCnt = 0, skipCnt = 0, tCnt = 0, totTime = 0;
        String[] tCols;
        String qry = "", fname = "";

        // ## Gitlab 921
        if (!NamedCommon.datOnly && !IamRestarting) {
            if (NamedCommon.RunType.equals("REFRESH") && !NamedCommon.isPrt) {
                qry = "DELETE FROM ";
                switch (NamedCommon.SqlDBJar) {
                    case "MSSQL":
                        qry += "[" + NamedCommon.SqlDatabase + "].";
                        qry += "[upl].[UPLCTL] ";
                        break;
                    case "SNOWFLAKE":
                        qry += "upl.UPLCTL";
                        break;
                    case "ORACLE":
                        qry += NamedCommon.SqlDatabase + ".";
                        qry += "upl_UPLCTL ";
                        break;
                    case "MYSQL":
                        qry += NamedCommon.SqlDatabase + ".upl_UPLCTL ";
                        break;
                    default:
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "[FATAL] Cannot identify DB type !!";
                        uCommons.uSendMessage(NamedCommon.Zmessage);
                        return false;
                }

                fname = NamedCommon.sqlTarget;
                fname = fname.replaceAll("\\.", "_");
                fname = fname.replaceAll("\\,", "_");
                fname = fname.replaceAll("\\ ", "_");

                String vSch = NamedCommon.SqlSchema;
                if (NamedCommon.uniBase) vSch = "uni";

                switch (NamedCommon.SqlDBJar) {
                    case "SNOWFLAKE":
                        qry += " where Target = '" + NamedCommon.SqlDatabase + "." + vSch + "." + fname + "'";
                        break;
                    default:
                        qry += " where Target = '" + "[" + NamedCommon.SqlDatabase + "].[" + vSch + "]." + "[" + fname + "]'";
                }
                DDL.clear();
                DDL.add(qry);
                uCommons.uSendMessage("   .) " + qry);
                SqlCommands.ExecuteSQL(DDL);
                DDL.clear();
                if (NamedCommon.ZERROR) return false;
            }
        }

        cmdBlockSize = NamedCommon.DatRows;

        boolean okay = true;
        String msgFmt = "";
        String showCtr = "";
        sqlStmt = "";
        sqlStmt = sqlStmt + "INSERT INTO [" + NamedCommon.SqlDatabase + "].";
        sqlStmt = sqlStmt + "[" + pSCH + "].";
        sqlStmt = sqlStmt + "[" + NamedCommon.sqlTarget + "] (";
        int rCnt = 0;
        String csvName, content;
        ArrayList<String> AllcsvLines = new ArrayList<String>();
        // -------------------- for each csv file in the map -------------------------
        int csvLen = NamedCommon.csvList.length;
        for (int i = 0; i < csvLen; i += 1) {
            csvName = NamedCommon.BaseCamp + "/maps/" + NamedCommon.csvList[i];
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
                return false;
            }
        }
        LineArray = uCommons.PrepareCsvDetails(AllcsvLines);
        // ---------------------------------------------------------------------------
        NamedCommon.tblCols = NamedCommon.tblCols.replaceAll("\\,\\,", ",");
        if (NamedCommon.ZERROR) return false;

        if (!IamRestarting) {
            if (!NamedCommon.isKafka && !NamedCommon.datOnly) SqlCommands.DropCreateTable();
        }
        if (NamedCommon.ZERROR) return false;

        if (NamedCommon.isNRT) {
            NamedCommon.tblCols = NamedCommon.tblCols.replaceAll("\\*", "");
            NamedCommon.tblCols = NamedCommon.tblCols.replaceAll("\\-", "");
            NamedCommon.tblCols = NamedCommon.tblCols.replaceAll("\\.", "");
            NamedCommon.tblCols = NamedCommon.tblCols.replaceAll("\\@", "");
        }

        boolean chkKey = true;      // chkKey is used to select specific records - needs a UV connection
        boolean uniFail = false;
        String wkfile = "", trunctable = "";
        String type = "";
        // ---------------------------------------------------------------------------
        // When selecting specfic source records in a map - it is BEST to
        // load the records into a specific raw table.
        // e.g. SELECT ACCOUNTS LIKE ...L8 go into [raw].[ACCOUNTS_L8_RFUEL]
        // So, when this process hits it, it can select from that file only
        // without having to use a UV licence !!
        // ---------------------------------------------------------------------------
        // Never going back to UV for data - it's too chatty & slows the UVDB
        //      - always use rawsel from the map or message.
        // ---------------------------------------------------------------------------
        String sel = APImap.APIget("select");
        String qFile = "";
        if (!NamedCommon.isNRT) {
            String rMsg = "        RunType: ***** " + NamedCommon.RunType + " *****";
            uCommons.uSendMessage((NamedCommon.block + NamedCommon.block).substring(0, rMsg.length()));
            uCommons.uSendMessage(rMsg);
            uCommons.uSendMessage((NamedCommon.block + NamedCommon.block).substring(0, rMsg.length()));

            // NamedCommon.thisProc  = rawsel from map

            if (sel.equals("")) sel = "NO";
            if (NamedCommon.thisProc.equals("")) chkKey = false;
            if (sel.equals("NO") || !NamedCommon.thisProc.equals("")) {
                uniFail = false;
                chkKey = false;
            }
            // ## Gitlab 921
            if (!NamedCommon.datOnly) {
                String oldF = " " + NamedCommon.u2Source + " ";
                String newF = " " + qFile + " ";

                wkfile = "workfile";
                String wkFocus = "[" + NamedCommon.rawDB + "].[upl].";
                NamedCommon.serial = NamedCommon.pid;
                wkfile = wkfile + NamedCommon.serial;
                dropwkfl = "";
                dropwkfl = SqlCommands.DropTable(NamedCommon.rawDB, "upl", wkfile);
                fname = wkFocus + "[" + wkfile + "]";
                thisWorkfile = fname;
                tCols = new String[]{"-uID", "-Serial", "Spare"};
                String createTable = SqlCommands.CreateTable(NamedCommon.rawDB, "upl", wkfile, tCols);
                DDL.clear();
                DDL.add(createTable);
                SqlCommands.ExecuteSQL(DDL);
                if (NamedCommon.ZERROR) return false;
                if (NamedCommon.StopNow.contains("<<FAIL>>")) {
                    uCommons.uSendMessage(NamedCommon.StopNow + "  " + createTable);
                } else {
                    uCommons.uSendMessage("   .) " + fname + " created");
                }
                DDL.clear();
                if (!NamedCommon.isDocker) {
                    trunctable = SqlCommands.TruncateTable(NamedCommon.rawDB, "upl", wkfile);
                }
            }
            DDL.clear();

            if (NamedCommon.RunType.equals("INCR")) {
                type = "DeltaKey_";
            } else {
                type = "FetchKey_";
            }
        }

        if (NamedCommon.ZERROR) {
            NamedCommon.Zmessage = "";
            return false;
        }

        String stCnt = "";
        String srCnt = "";
        String snCnt = "";
        String ssCnt = "";
        String sBgRw = "";

        // ## Gitlab 921
        String rawTable = "";
        if (!NamedCommon.datOnly) {
            rawTable = GetRawTable();
            if (NamedCommon.ZERROR) {
                NamedCommon.Zmessage = "Cannot find raw table";
                uCommons.uSendMessage(NamedCommon.Zmessage);
                NamedCommon.Zmessage = "";
                return false;
            }
        } else {
            rawTable = NamedCommon.sqlTarget;
        }

        String checkCol = "rowsOUT";
        if (!NamedCommon.emptyrows) checkCol = "rowsIN";

        String idCol = "uID", rdCol="RawData", enCol="encSeed";
        if (NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE")) {
            idCol = idCol.toUpperCase();
            rdCol = rdCol.toUpperCase();
            enCol = enCol.toUpperCase();
        }

        // -------------------[ IMPORTANT ]---------------------------
        // The raw DB is held in NamedCommon.rawDB !! Nowhere else! //
        // thisDB is prepared for non-RAW data selection            //
        // -----------------------------------------------------------

        // ## Gitlab 921
        String thisDB = "";
        if (!NamedCommon.datOnly) {
            thisDB = APImsg.APIget("sqldb:" + NamedCommon.tHostList.get(0));
            if (thisDB.equals("")) thisDB = NamedCommon.SqlDatabase;        // no tHost
            if (NamedCommon.isNRT && thisDB.equals("$DB$")) thisDB = NamedCommon.rawDB;
        } else {
            thisDB = "$DB$";
        }

        // -----------------------------------------------------------
        // ## Gitlab 921
        int expecting = 0;
        int available = -10;
        if (!NamedCommon.datOnly) {
            if (!uniFail && !NamedCommon.ZERROR) {
                ResultSet rs;
                NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
                if (NamedCommon.uCon == null) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                    return false;
                }

                if (!NamedCommon.rawDB.equals("")) {
                    switch (NamedCommon.SqlDBJar) {
                        case "SNOWFLAKE":
                            ExtractManager.dbFocus = NamedCommon.rawDB + "." + NamedCommon.rawSC + "." + rawTable;
                            break;
                        default:
                            ExtractManager.dbFocus = "[" + NamedCommon.rawDB + "]." + "[" + NamedCommon.rawSC + "].[" + rawTable + "] ";
                    }
                } else {
                    switch (NamedCommon.SqlDBJar) {
                        case "SNOWFLAKE":
                            ExtractManager.dbFocus = thisDB + "." + "raw." + rawTable;
                            break;
                        default:
                            ExtractManager.dbFocus = "[" + thisDB + "]." + "[raw].[" + rawTable + "] ";
                    }
                }

                String stdEncseed = "";
                if (NamedCommon.encRaw && !NamedCommon.AES) {
                    if (!NamedCommon.cSeed.equals("")) {
                        stdEncseed = NamedCommon.cSeed;
                    } else {
                        stdEncseed = ExtractManager.GetEncSeed();
                    }
                }
                String encSeed = "";
                if (!NamedCommon.isNRT && !NamedCommon.datOnly) {
                    try {
                        qry = "";

                        //--------------------------------------------------
                        // 1. Columns to select
                        // -------------------------------------------------

                        if (!NamedCommon.Quote.equals("")) {
                            // assume the quote char is '
                            qry = "SELECT LoadDte, uID, RawData ";
                            if (!NamedCommon.RunType.equals("REFRESH")) {
//                                qry = "SELECT max(CAST(REPLACE(LoadDte, '''','') as decimal(25,0))) as LoadDte, uID, RawData ";
                                qry = "SELECT LoadDte, uID, RawData ";
                            }
                        } else {
                            if (NamedCommon.RunType.equals("REFRESH")) {
                                qry = "SELECT LoadDte, uID, RawData ";
                            } else {
//                                qry = "SELECT max(CAST(LoadDte as decimal(25,0))) as LoadDte, uID, RawData ";
                                qry = "SELECT LoadDte, uID, RawData ";
                            }
                        }

                        if (" PART ".indexOf(NamedCommon.RunType) > 0) qry = "SELECT uID, RawData ";

                        //--------------------------------------------------
                        // 2. Which table to selct from
                        //--------------------------------------------------

                        String dbObj="",  nolock = "";
                        slowTable = "";

                        if (NamedCommon.SqlDBJar.equals("MSSQL") && NamedCommon.noLock) nolock = " WITH (NOLOCK)";
                        switch (NamedCommon.SqlDBJar) {
                            case "MSSQL":
                                dbObj += "[" + NamedCommon.rawDB + "].";
                                dbObj += "[raw].[" + rawTable + "]";
                                qry += "from " + dbObj + nolock + " ";
                                slowTable = "_"+dbObj;
                                break;
                            case "SNOWFLAKE":
                                dbObj = NamedCommon.SqlDatabase + ".raw." + rawTable;
                                qry += "from " + dbObj;
                                slowTable = "_"+dbObj;
                                break;
                            case "ORACLE":
                                qry += "from ";
                                dbObj = thisDB + "." + "RAW_" + rawTable;
                                qry += dbObj;
                                slowTable = "_"+dbObj;
                                break;
                            case "MYSQL":
                                qry += "from ";
                                dbObj = thisDB + "." + "raw_" + rawTable + " ";
                                qry += dbObj;
                                if (" REFRESH PART ".indexOf(NamedCommon.RunType) > 0) {
                                    if (NamedCommon.thisProc.toUpperCase().startsWith("WITH") || NamedCommon.thisProc.toUpperCase().startsWith("SELECT")) {
                                        qry = NamedCommon.thisProc;
                                    } else {
                                        qry += NamedCommon.thisProc; // e.g. rawsel=where ProcNum='1'
                                    }
                                }
                                slowTable = "_"+dbObj;
                                break;
                            default:
                                NamedCommon.ZERROR = true;
                                NamedCommon.Zmessage = "[FATAL] Cannot identify DB type !!";
                                uCommons.uSendMessage(NamedCommon.Zmessage);
                                return false;
                        }

                        // once the table goes into AMQ, dots are replaced with underscores
                        slowTable = slowTable.replace(".", "_");

                        //--------------------------------------------------
                        // 3. Selection filters
                        //--------------------------------------------------

                        boolean hasWhere=false;
                        if (!NamedCommon.thisProc.equals("")) {
                            qry = "SELECT LoadDte, uID, RawData FROM " + dbObj + nolock + " " + NamedCommon.thisProc;
                            hasWhere = true;  // the where is in thisProc
                        }

                        if (NamedCommon.encRaw && !NamedCommon.AES) {
                            if (NamedCommon.cSeed.equals("")) {
                                // encSeed is not in rfuel.properties - it's been inserted into the raw table
                                if (hasWhere) {
                                    qry += " and uID <> 'encSeed'";
                                } else {
                                    qry += " where uID <> 'encSeed'";
                                    hasWhere = true;
                                }
                            }
                        }

                        if (!NamedCommon.item.equals("") && !NamedCommon.item.equals("*")) {
                            if (hasWhere) {
                                qry += " and uID = '" + NamedCommon.item + "'";
                            } else {
                                qry += " where uID = '" + NamedCommon.item + "'";
                                hasWhere=true;
                            }
                        }

                        String rID = "", rDat = "", grpPfx="";
                        String useRAW = "";
                        if (NamedCommon.SqlDBJar.equals("MSSQL")) {
                            useRAW = "[" + NamedCommon.rawDB + "].[raw].[" + rawTable + "]";
                        } else {
                            useRAW = NamedCommon.rawDB + ".raw_" + rawTable;
                        }
                        String sQry = "";
                        if (!NamedCommon.SqlDBJar.equals("ORACLE")) grpPfx = " group by LoadDte, uID, RawData";

                        // this version of sQry is when we do NOT go back to the uv host and just pull the latest updates from raw.
                        // the 012 process sets "LoadDte" in the message when it runs an INCR process.
                        // that message is then automatically Hop.send to here.
                        //
                        if (APImsg.APIget("INCR_LOADDTE").equals("")) {
                            if (NamedCommon.RunType.equals("INCR")) {
                                sQry = qry;
                            } else {
                                if (chkKey) {
                                    //  chkKey & is not an INCR process
                                    sQry = "WITH \n";
                                    sQry += "   T1 as ( " + qry + " ), \n";
                                    sQry += "   T3 as ( select max(LoadDte) as LoadDte from " + useRAW + nolock + " ) \n";
                                    sQry += " SELECT T1.uID, T1.RawData FROM T3,T1" + nolock + " \n";
                                    sQry += " where T1.LoadDte = T3.LoadDte \n";
                                    grpPfx = " group by T1.LoadDte, T1.uID, T1.RawData";
                                } else {
                                    sQry = qry;
                                }
                            }
                        } else {
                            // an INCR process - BatchID is set to the fetch LoadDte.
                            if (hasWhere) {
//                                sQry = qry + " and cast(LoadDte as decimal(25,0)) = '" + NamedCommon.BatchID + "'";
                                if (NamedCommon.RunType.toUpperCase().equals("INCR")) {
//                                    sQry = qry + " and cast(LoadDte as decimal(25,0)) >= '" + NamedCommon.BatchID + "'";
                                    sQry = qry + " and LoadDte = '" + NamedCommon.BatchID + "'";
                                } else {
                                    sQry = qry + " and LoadDte = '" + NamedCommon.BatchID + "'";
                                }
                                if (!NamedCommon.cSeed.equals("")) {
                                    sQry += " and uID <> 'encSeed'";
                                }
                            } else {
                                sQry = qry;
//                                sQry+= " where cast(LoadDte as decimal(25,0)) = '" + NamedCommon.BatchID + "'";
                                sQry+= " where LoadDte = '" + NamedCommon.BatchID + "'";
                                if (!NamedCommon.cSeed.equals("")) {
                                    sQry += " and uID <> 'encSeed'";
                                }
                            }
                        }

                        if (!NamedCommon.RunType.equals("REFRESH") && NamedCommon.thisProc.equals("")) sQry += grpPfx;

                        if (!NamedCommon.SqlDBJar.equals("MSSQL") && !NamedCommon.SqlDBJar.equals("SNOWFLAKE")) {
                            while (sQry.indexOf("uID") >= 0) {
                                sQry = sQry.replace("uID", "zID");
                            }
                            idCol = "zID";
                        }

                        skipCnt = 0;
                        tCnt = 0;
                        totTime = 0;
                        pCnt = 0;
                        sent = System.nanoTime();

                        if (NamedCommon.sConnected) {
                            uCommons.uSendMessage("   .) Disconnect SourceDB");
                            SourceDB.DisconnectSourceDB();
                        }

                        rs = null;
                        NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
                        if (NamedCommon.uCon == null) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                            return false;
                        }

                        if (NamedCommon.SqlDBJar.equals("MSSQL")) sQry = NamedCommon.jdbcSep + sQry;

                        // Changed for Kiwibank where LOTS of data being inserted, overloads SQL
                        // the 014 selects were not getting all the 012 data bulk inserted !!!

                        if (NamedCommon.TranIsolation) {
                            if (NamedCommon.uCon.getTransactionIsolation() != Connection.TRANSACTION_READ_UNCOMMITTED) {
                                NamedCommon.uCon.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                            }
                        }
                        Statement stmt = NamedCommon.uCon.createStatement();
                        stmt.setFetchSize(NamedCommon.FetchSize);

                        // wait for other process which may also be trying to select from slowtable (many u2Files into 1 SQL table)
                        // no harm in waiting a little longer - it save time in the long run.
                        uCommons.uSendMessage("Wait for HOLD on "+slowfile+slowTable);
                        coreCommons.SlowDown(slowTable);

                        if (!NamedCommon.isPrt) {
                            uCommons.uSendMessage("--------------------------------[ Data Readiness Checking ]-------------------------------");

                            // pause inserting data into slowtable while this process tries to select data from it.
                            uCommons.uSendMessage("Place  HOLD   on "+slowfile+slowTable);
                            uCommons.WriteDiskRecord(slowfile+slowTable, "10");

                            // previousAvailable     the result of the previous select count(*)
                            // hardMatch            the ABSOLUTE hard MUST match this value
                            int previousAvailable=0, hardMatch=-1;
                            //
                            // How many rows are we expecting? Get from uplctl if it is there OR keep selecting until you get 5 answers the same.
                            //
                            if (NamedCommon.thisProc.equals("")) {
                                String uplTarget = "[" + NamedCommon.rawDB + "].[raw].[" + rawTable + "]";
//                                String uplSelect = "select top 1 cast(BatchID as decimal(25,0)), "+checkCol+" from upl.UPLCTL ";
                                String uplSelect = "select top 1 BatchID, "+checkCol+" from upl.UPLCTL ";
                                uplSelect += "where Target = '" + uplTarget+"' and Source like '%"+NamedCommon.u2Source+"]'";
                                if (!APImsg.APIget("INCR_LOADDTE").equals("")) {
//                                    uplSelect += " and cast(BatchID as decimal(25,0)) = '" + APImsg.APIget("INCR_LOADDTE") + "'";
                                    uplSelect += " and BatchID = '" + APImsg.APIget("INCR_LOADDTE") + "'";
                                } else {
//                                    uplSelect += " and cast(BatchID as decimal(25,0)) = '" + APImsg.APIget("INCR_LOADDTE") + "'";
                                    uplSelect += " and BatchID = '" + APImsg.APIget("INCR_LOADDTE") + "'";
                                }
                                uCommons.uSendMessage("Using UPLCTL ...");
                                uCommons.uSendMessage(uplSelect);
                                while (true) {
                                    try {
                                        rs = stmt.executeQuery(uplSelect);
                                        while (rs.next()) {
                                            expecting = Integer.parseInt(rs.getString(checkCol));
                                            uCommons.uSendMessage("******** " + uCommons.oconvM(String.valueOf(expecting), "MD0~") + " rows expected from Fetch ***************");
                                        }
                                        break;
                                    } catch (SQLException se) {
                                        SqlCommands.ReconnectService();
                                        NamedCommon.uCon = ConnectionPool.getConnection(NamedCommon.jdbcCon);
                                        NamedCommon.uCon.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                                        stmt = NamedCommon.uCon.createStatement();
                                        stmt.setFetchSize(NamedCommon.FetchSize);
                                    }
                                }
                                if (expecting == 0) {
                                    uCommons.uSendMessage("WARNING: UPLTCTL may not have been updated !!");
                                }
                                rs.close();
                                rs = null;
                                previousAvailable = expecting;
                                hardMatch = expecting;
                            } else {
                                uCommons.uSendMessage("Using RawSel: " + NamedCommon.thisProc);
                                previousAvailable = -1;
                            }
                            uCommons.uSendMessage("raw.Table readiness check .......................................");
                            String sqlCount = "";
                            if (sQry.indexOf("max(CAST") > 0) {
                                if (sQry.indexOf("REPLACE") > 0) {
//                                    sqlCount = sQry.replace("max(CAST(REPLACE(LoadDte, '''','') as decimal(25,0))) as LoadDte, uID, RawData", "count(*) as ctr");
                                    sqlCount = sQry.replace("LoadDte, uID, RawData", "count(*) as ctr");
                                } else {
//                                    sqlCount = sQry.replace("max(CAST(LoadDte as decimal(25,0))) as LoadDte, uID, RawData", "count(*) as ctr");
                                    sqlCount = sQry.replace("LoadDte, uID, RawData", "count(*) as ctr");
                                }
                            } else {
                                sqlCount = sQry.replace("LoadDte, uID, RawData", "count(*) as ctr");
                            }
                            if (sqlCount.indexOf(" group by") > 0) sqlCount = sqlCount.substring(0, sqlCount.indexOf("group by"));
                            uCommons.uSendMessage(sqlCount);
                            System.out.println(" ");
                            uCommons.uSendMessage("******** " +
                                    uCommons.oconvM(String.valueOf(hardMatch), "MD0~") +
                                    " starting value.");
                            int matchCnt=0, tryMatch=0;
                            long startTime = System.currentTimeMillis();
                            long timeoutMillis = 1 * 60 * 1000; // 1 minute
                            while (matchCnt < 5) {
                                if ((System.currentTimeMillis() - startTime > timeoutMillis) && matchCnt == 0) {
                                    String sendTo = MessageProtocol.GetQueueName("014")+"_000";
                                    uCommons.uSendMessage("[ERROR] # Timeout: no consistency — sending " +
                                            NamedCommon.u2Source+" back to "+sendTo+" #");
                                    Hop.start(NamedCommon.message, "", uCommons.GetNextBkr(NamedCommon.Broker),
                                            sendTo, "", NamedCommon.CorrelationID);
                                    NamedCommon.BurstRestarted = true;

                                    // This is an example of how "other" processes can un-set the HOLD
                                    uCommons.uSendMessage("Remove HOLD from "+slowfile+slowTable);
                                    uCommons.DeleteFile(slowfile+slowTable);
                                    return false;
                                }
                                while (true) {
                                    // KEEP the hold in place - another process may have removed it.
                                    uCommons.WriteDiskRecord(slowfile+slowTable, "10");
                                    tryMatch++;
                                    try {
                                        rs = stmt.executeQuery(sqlCount);
                                        available = 0;
                                        while (rs.next()) {
                                            available = Integer.parseInt(rs.getString("ctr"));
                                        }
                                        break;
                                    } catch (SQLException se) {
                                        SqlCommands.ReconnectService();
                                        NamedCommon.uCon = ConnectionPool.getConnection(NamedCommon.jdbcCon);
                                        if (NamedCommon.TranIsolation)
                                            NamedCommon.uCon.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                                        stmt = NamedCommon.uCon.createStatement();
                                        stmt.setFetchSize(NamedCommon.FetchSize);
                                    }
                                }
                                rs.close();
                                rs = null;

                                // KEEP the hold in place - another process may have removed it.
                                uCommons.WriteDiskRecord(slowfile+slowTable, "10");

                                if (NamedCommon.debugging) {
                                    uCommons.uSendMessage("Compare " +
                                            uCommons.oconvM(String.valueOf(available), "MD0~") +
                                            " ****** against ******" +
                                            uCommons.oconvM(String.valueOf(previousAvailable), "MD0~"));
                                }

                                if (available == previousAvailable) {
                                    matchCnt++;
                                } else {
                                    matchCnt = 0;  // reset on mismatch
                                    if (hardMatch > 0) { previousAvailable = hardMatch; } else { previousAvailable = available; }
                                }
                                uCommons.uSendMessage("******** " +
                                        uCommons.oconvM(String.valueOf(available), "MD0~") +
                                        " rows available now *********************  (" +
                                        matchCnt + "/5)");

                                // delay to make SURE the count is consistent.
                                if (matchCnt < 5) uCommons.Sleep(2);
                                expecting = available;
                            }
                            Expected = expecting;
                            uCommons.uSendMessage("------------------------------------------------------------------------------------------");
                            uCommons.uSendMessage("-                                                                                        -");
                            uCommons.uSendMessage("-         Expecting: "+Expected+ " rows to be processed !!!");
                            uCommons.uSendMessage("-                                                                                        -");
                            uCommons.uSendMessage("------------------------------------------------------------------------------------------");
                        }

                        if (!NamedCommon.isNRT) {
                            uCommons.uSendMessage("   .) Selecting data");
                            System.out.println("************************[ rawQry ]**********************************");
                            System.out.println(sQry);
                            System.out.println("*******************************************************************");
                            sel = sQry;
                            uCommons.uSendMessage("   .) use LoadDte " + NamedCommon.BatchID);
                            uCommons.uSendMessage("   .) Processing data (show progress in " + (OutputCtr+1) + " [raw] row intervals)");
                            if (!NamedCommon.BulkLoad)
                                uCommons.uSendMessage("******** Manual INSERTs will be used ********");
                        }

                        sQry = sQry.replaceAll("\\r?\\n", "");

                        if (Resilience && IamRestarting) {
                            uCommons.uSendMessage("******** Excluding " + h2dbServer.AlreadyProcessed() + " rows already processed ***************");
                        }

                        while (true) {
                            try {
                                rs = stmt.executeQuery(sQry);
                                break;
                            } catch (SQLException se) {
                                SqlCommands.ReconnectService();
                                NamedCommon.uCon = ConnectionPool.getConnection(NamedCommon.jdbcCon);
                                if (NamedCommon.TranIsolation) NamedCommon.uCon.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                                stmt = NamedCommon.uCon.createStatement();
                                stmt.setFetchSize(NamedCommon.FetchSize);
                            }
                        }
                        if (NamedCommon.debugging) uCommons.uSendMessage("Execution completed.");
                        uCommons.uSendMessage("Remove HOLD from "+slowfile+slowTable);
                        uCommons.DeleteFile(slowfile+slowTable);

                        uCommons.uSendMessage("   .) ************************* PROCESSING NOW **************************");

                        boolean skipThis;
                        //
                        // If you want to see what Burst is doing on a record-by-record level,
                        // deep.dive<is>true<tm>
                        // deep.dive.string<is>blah-blah<tm> which is a string contained in the rID string
                        // e.g. rID = 123456TL42  ddLookFor = 2345
                        //
                        deepDive = (APImsg.APIget("deep.dive").toLowerCase().equals("true"));
                        ddLookFor= (APImsg.APIget("deep.dive.string"));
                        GarbageCollector.setStart(System.nanoTime());
                        GarbageCollector.setCollection(120);
                        String[] tmpArr;

                        // From this point onward, any loss of connection with the TargetDB
                        // means that the whole process must restart.

                        while (rs.next()) {
                            GarbageCollector.CleanUp();
                            coreCommons.SlowDown(burst);
                            skipThis = false;
                            tCnt++;
                            rID = rs.getString(idCol);
                            if (deepDive) {
                                ddID = rID;
                                uCommons.uSendMessage("Got next ["+rID+"] -----------------------------------");
                            }
                            if (rID.equals(enCol)) continue;
                            rDat = rs.getString(rdCol);

                            // when we RESTART, don't process and uID's which have previously been processed

                            if (Resilience && IamRestarting) {
                                if (h2dbServer.HasProcessed(rID)) {
                                    rCnt++;
                                    pCnt++;
                                    continue;
                                }
                            }

                            //                                                          //
                            // null handling in case raw is empty or FetchKeys failed.  //
                            //                                                          //

                            if (rDat == null || rID == null) {
                                nulRecs++;
                                skipThis = true;
                                continue;
                            }
                            if (rDat.length() == 0) {
                                mtRecs++;
                                skipThis = true;
                                continue;
                            }
                            while (rDat.contains("\r") || rDat.contains("\n")) {
                                rDat = rDat.replace("\r", "");
                                rDat = rDat.replace("\n", "");
                            }
                            // gitlab #16
                            if (NamedCommon.encRaw) {
                                encSeed = stdEncseed;
                                if (stdEncseed.equals("") && !NamedCommon.AES) {
                                    tmpArr = rDat.split("~");
                                    rDat = rDat.split("~")[0];
                                    if (tmpArr.length > 1) encSeed = tmpArr[1];
                                }
                                if (deepDive) uCommons.uSendMessage("    v2UnScramble");
                                rDat = uCipher.v2UnScramble(uCipher.keyBoard25, rDat, encSeed);
                                tmpArr = null;
                            }
                            // ----------
                            if (!quote.equals("")) {
                                if (rDat.startsWith(quote) && rDat.endsWith(quote)) {
                                    rDat = rDat.substring(1, rDat.length());
                                    rDat = rDat.substring(0, (rDat.length() - 1));
                                }
                                if (rID.startsWith(quote) && rID.endsWith(quote)) {
                                    rID = rID.substring(1, rID.length());
                                    rID = rID.substring(0, (rID.length() - 1));
                                }
                            }
                            if (!skipThis) {
                                rCnt++;
                                pCnt++;
                                NamedCommon.uID = new UniString(rID);
                                sqlREC = rDat;
                                if (deepDive) uCommons.uSendMessage("    SQL2UVRec");
                                inRec = uCommons.SQL2UVRec(rID + atIM + rDat);
                                if (inRec == null) {
                                    nulRecs++;
                                    if (NamedCommon.showNulls) {
                                        uCommons.uSendMessage("NULL record @ " + NamedCommon.uID);
                                    }
                                } else {
                                    if (deepDive) uCommons.uSendMessage("    ProcessRow");
                                    ProcessRow(LineArray, inRec);
                                    if (!NamedCommon.ZERROR) {
                                        if (Resilience) Resilience = h2dbServer.Insert(rID);
                                        grandTot++;
                                        NamedCommon.runSilent = false;
                                    } else {
                                        return false;
                                    }
                                }
                            }
                            if (pCnt > OutputCtr) {
                                msgFmt = "";
                                showCtr = uCommons.oconvM(String.valueOf(rCnt), "MD0,");
                                showCtr = uCommons.RightHash(showCtr, 15);
                                msgFmt += showCtr;
                                got = System.nanoTime();
                                laps = (got - sent) / nanoSecs;
                                lapse = (int) (long) laps;
                                totTime += lapse;
                                showCtr = uCommons.oconvM(String.valueOf(datRows), "MD0,");
                                showCtr = uCommons.RightHash(showCtr, 15);
                                msgFmt += " row(s) read in and " + showCtr
                                        + " rows written out in :: "
                                        + lapse + " seconds";
                                uCommons.uSendMessage(msgFmt);
                                pCnt = 0;
                                sent = System.nanoTime();
                                GarbageCollector.CleanUp();
                            }
                            if (deepDive) uCommons.uSendMessage("    GetNext");
                        }

                        //
                        // ----- end loop ----------
                        //
                        if (pCnt > 0) {
                            msgFmt = "";
                            showCtr = uCommons.oconvM(String.valueOf(pCnt), "MD0,");
                            showCtr = uCommons.RightHash(showCtr, 15);
                            msgFmt += showCtr;
                            got = System.nanoTime();
                            laps = (got - sent) / nanoSecs;
                            lapse = (int) (long) laps;
                            totTime += lapse;
                            showCtr = uCommons.oconvM(String.valueOf(datRows), "MD0,");
                            showCtr = uCommons.RightHash(showCtr, 15);
                            msgFmt += " row(s) read in and " + showCtr +
                                    " rows written out in :: " +
                                    lapse + " seconds";
                            uCommons.uSendMessage(msgFmt);
                            pCnt = 0;
                        }
                        if (chkKey) {
                            if (tCnt != grandTot) {
                                uCommons.uSendMessage("****.>  ERROR: Should have checked " + grandTot + " rows");
                                uCommons.uSendMessage("****.>       : Only " + tCnt + " rows were checked.");
                                uCommons.uSendMessage("****.>       : Possible cause - raw has not finished loading or is incomplete");
                                System.out.println(" ");
                                NamedCommon.ZERROR = true;
                                NamedCommon.Zmessage = "Should have checked " + grandTot + " rows but " + tCnt + " were checked.";
                            }
                        }

                        got = System.nanoTime();
                        laps = (got - sent) / nanoSecs;
                        lapse = (int) (long) laps;
                        totTime += lapse;
                        String extra = "-";
                        stCnt = String.valueOf(tCnt);
                        if (chkKey) stCnt = String.valueOf(grandTot);
                        int lx = stCnt.length() + 2;
                        stCnt = uCommons.RightHash(uCommons.oconvM(stCnt, "MD0,"), lx);
                        srCnt = uCommons.RightHash(uCommons.oconvM(String.valueOf(rCnt), "MD0,"), lx);
                        snCnt = uCommons.RightHash(uCommons.oconvM(String.valueOf(mtRecs), "MD0,"), lx);
                        ssCnt = uCommons.RightHash(uCommons.oconvM(String.valueOf(nulRecs), "MD0,"), lx);
                        sBgRw = uCommons.RightHash(uCommons.oconvM(String.valueOf(bigRows), "MD0,"), lx);
                        if (bigRows > 0) {
                            extra = MessageProtocol.data + NamedCommon.u2FileRef + ".2big";
                        }
                        showCtr = uCommons.oconvM(String.valueOf(rCnt), "MD0,");
                        msgFmt = "    .> all " + showCtr;
                        msgFmt += " row(s) read in and ";
                        showCtr = uCommons.oconvM(String.valueOf(datRows), "MD0,");
                        msgFmt += showCtr + " rows written out in :: " + totTime + " seconds";
                        uCommons.uSendMessage(msgFmt);
                        msgFmt = "    .> Finished ****************************************************";
                        uCommons.uSendMessage(msgFmt);
                        uCommons.uSendMessage("    .>  checked " + stCnt + " rows from raw");
                        uCommons.uSendMessage("    .>  process " + srCnt + " related row(s)");
                        uCommons.uSendMessage("    .>  skipped " + ssCnt + " empty raw record(s)");
                        uCommons.uSendMessage("    .>  ignored " + snCnt + " unrelated row(s)");
                        uCommons.uSendMessage("    .> withheld " + sBgRw + " big row(s) in file [" + extra + "]");
                        uCommons.uSendMessage("    .> *************************************************************");
                        if (nullSamples.length() > 1) {
                            uCommons.uSendMessage("    .> Sample of empty records:-");
                            uCommons.uSendMessage("    .> " + nullSamples);
                            uCommons.uSendMessage("    .> *************************************************************");
                        }
                        if (rCnt == 0 && !NamedCommon.RunType.equals("INCR")) {
                            NamedCommon.ZERROR = false;
                            NamedCommon.Zmessage = "";
                            uCommons.uSendMessage("Source or target data could not be found (or was null - maybe a REFRESH run?)");
                            return true;
                        }
                        Actual = tCnt;

                        if (NamedCommon.isPrt) Actual = Expected;

                        rs.close();
                        stmt.close();
                        rs = null;
                        stmt = null;
                        if (NamedCommon.StopNow.contains("<<FAIL>>")) {
                            uCommons.eMessage = NamedCommon.StopNow + "    " + sQry;
                        }

                        // --------------------------------------------------------------------------------------
                        // Dumping after every UV record in Burst seems weird - it stresses the TargetDB
                        // If this section is removed, larger (DataSize) files are built then sent - like fetch
                        // --------------------------------------------------------------------------------------

                        if (!sqlCmds.isEmpty()  && sqlCmds.size() > cmdBlockSize) {
                            uCommons.SQLDump(sqlCmds);
                            sqlCmds.clear();
                            rowCtr = 0;
                            if (NamedCommon.ADMP) {
                                admpCnt = uCommons.ADMPdump(admpCnt, admpDat);
                                admpDat.clear();
                                admpCnt++;
                            }
                            if (NamedCommon.ZERROR) return false;
                        }

                        // Changed for migration project at Kiwibank
//                        if (!NamedCommon.allowDups && !NamedCommon.ZERROR && !NamedCommon.RunType.equals("INCR")) {
                        if (!NamedCommon.allowDups && !NamedCommon.ZERROR) {
                            tCols = NamedCommon.tblCols.split("\\,");
                            String ctlLine = "", useSCH = NamedCommon.SqlSchema;
                            if (NamedCommon.uniBase && !NamedCommon.SqlSchema.equals(NamedCommon.rawSC)) useSCH = "uni";
                            boolean doIDX = false;
                            if (!NamedCommon.isPrt) {
                                doIDX = true;
                            } else {
                                if (uCommons.APIGetter("lastpart").toLowerCase().equals("true")) doIDX = true;
                            }
                            if (doIDX) {
                                ctlLine = SqlCommands.CreateIndex(NamedCommon.SqlDatabase, useSCH, NamedCommon.sqlTarget, tCols);
                                if (!ctlLine.equals("")) {
                                    String fName = NamedCommon.sqlTarget;
                                    fName = fName.replaceAll("\\.", "_");
                                    fName = fName.replaceAll("\\,", "_");
                                    fName = fName.replaceAll("\\ ", "_");

                                    uCommons.uSendMessage("   .) Create index on table " + fName + " - if not exists");
                                    DDL.clear();
                                    DDL.add(ctlLine);
                                    SqlCommands.ExecuteSQL(DDL);

                                    if (NamedCommon.ZERROR) return false;
                                }
                            }
                        }
                    } catch (SQLException e) {
                        //
                        // This catch clause is invoked when the link to the SQL host is lost or interrupted.
                        //      if Reslience is on (H2 database is installed) it is best to re-process the message
                        //      and do not System.exit()
                        //
                        if (!String.valueOf(e.getErrorCode()).startsWith("08")) {
                            if (!uCommons.eMessage.equals("") && !uCommons.eMessage.endsWith("\n")) {
                                uCommons.eMessage += "\n";
                            }
                            uCommons.eMessage += ">>TargetDB ABORT:: Cannot access data ";
                            uCommons.eMessage += "\n" + e.getMessage();
                            uCommons.eMessage += "\n" + MessageProtocol.messageText;
                            uCommons.uSendMessage("*******************************************************************");
                            uCommons.uSendMessage(uCommons.eMessage);
                            uCommons.uSendMessage("*******************************************************************");
                            uCommons.ReportRunError(uCommons.eMessage);
                            NamedCommon.ZERROR = false;
                            NamedCommon.Zmessage = "";
                            return false;
                        } else {
                            SqlCommands.ReconnectService();
                            IamRestarting = true;
                            return false;
                        }
                    }
                } else {
                    // ---- TagLine ----
                    if (!NamedCommon.isNRT) {
                        String takeFile = NamedCommon.u2Source + "_" + NamedCommon.datAct;
                        //
                        // needs development for Snowflake
                        qry = "SELECT LoadDte, uID, RawData from " +
                                "[" + NamedCommon.rawDB + "].[raw].[" + takeFile + "] " +
                                "WHERE uID = '" + NamedCommon.item + "' " +
                                "AND LoadDte = '" + NamedCommon.BatchID + "'";
                        String rID = "", rDat = "";
                        sent = System.nanoTime();
                        //
                        uCommons.uSendMessage("   .) Selecting raw data");
                        System.out.println("************************[ Query ]**********************************");
                        System.out.println(qry);
                        System.out.println("*******************************************************************");
                        uCommons.uSendMessage("   .) use LoadDte " + NamedCommon.BatchID);
                        uCommons.uSendMessage("   .) Processing data (show progress in " + (OutputCtr+1) + " [raw] row intervals)");
                        if (!NamedCommon.BulkLoad) uCommons.uSendMessage("******** Manual INSERTs will be used ********");
                        rs = null;
                        NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
                        if (NamedCommon.uCon == null) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                        }
                        Statement stmt = null;
                        try {
                            stmt = NamedCommon.uCon.createStatement();
                            rs = stmt.executeQuery(qry);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        GarbageCollector.setStart(System.nanoTime());
                        String[] tmpArr;
                        while (true) {
                            try {
                                if (!rs.next()) break;
                                GarbageCollector.CleanUp();
                                try {
                                    rID = rs.getString(idCol);
                                    if (rID.equals(enCol)) continue;
                                    rDat = rs.getString(rdCol);
                                    if (NamedCommon.encRaw) {
                                        if (!NamedCommon.AES) {
                                            tmpArr = rDat.split("~");
                                            if (tmpArr.length > 1) {
                                                rDat = rDat.split("~")[0];
                                                encSeed = tmpArr[1];
                                            } else {
                                                encSeed = stdEncseed;
                                            }
                                        }
                                        rDat = uCipher.v2UnScramble(uCipher.keyBoard25, rDat, encSeed);
                                        tmpArr = null;
                                    }

                                    // Dealerships have control chars in every data field !!
                                    if (NamedCommon.CleanseData) rDat = u2Commons.Cleanse(rDat);
                                } catch (SQLException e) {
                                    uCommons.uSendMessage(e.getMessage());
                                    continue;
                                }
                                if (rDat == null || rID == null) {
                                    nulRecs++;
                                    continue;
                                }
                                if (rDat.length() == 0) {
                                    mtRecs++;
                                    continue;
                                }
                                while (rDat.contains("\r") || rDat.contains("\n")) {
                                    rDat = rDat.replace("\r", "");
                                    rDat = rDat.replace("\n", "");
                                }
                                if (!quote.equals("")) {
                                    if (rDat.startsWith(quote) && rDat.endsWith(quote)) {
                                        rDat = rDat.substring(1, rDat.length());
                                        rDat = rDat.substring(0, (rDat.length() - 1));
                                    }
                                    if (rID.startsWith(quote) && rID.endsWith(quote)) {
                                        rID = rID.substring(1, rID.length());
                                        rID = rID.substring(0, (rID.length() - 1));
                                    }
                                }
                                inRec = uCommons.SQL2UVRec(rID + atIM + rDat);
                                ProcessRow(LineArray, inRec);
                                inRec = null;
                            } catch (SQLException e) {
                                uCommons.uSendMessage(e.getMessage());
                                continue;
                            }
                        }
                        try {
                            rs.close();
                            stmt.close();
                            rs = null;
                            stmt = null;
                        } catch (SQLException e) {
                            uCommons.uSendMessage(e.getMessage());
                        }
                    }
                }

                if (!sqlCmds.isEmpty() && sqlCmds.size() > cmdBlockSize) {
                    uCommons.SQLDump(sqlCmds);
                    sqlCmds.clear();
                    rowCtr = 0;
                    if (NamedCommon.ADMP) {
                        admpCnt = uCommons.ADMPdump(admpCnt, admpDat);
                        admpDat.clear();
                        admpCnt++;
                    }
                    if (NamedCommon.ZERROR) return false;
                }

                if (!APImsg.APIget("record").equals("")) {
                    inRec = uCommons.SQL2UVRec(APImsg.APIget("record"));
                    ProcessRow(LineArray, inRec);
                    inRec = null;
                    NamedCommon.datCt = sqlCmds.size();
                    rCnt = NamedCommon.datCt;
                    if (!NamedCommon.isNRT)
                        uCommons.uSendMessage("   .) " + NamedCommon.datCt + " additional row(s) sent for insertion");
                }
                if (!NamedCommon.isNRT) uCommons.uSendMessage("   .) Finished unloading raw data");
            } else {
                uCommons.uSendMessage("***.) Failed to obtain source file ID's");
                okay = false;
            }
        } else {
            NamedCommon.serial = NamedCommon.pid;
            HandleDatFiles();
            okay = true;
        }

        if (!trunctable.equals("")) {
            uCommons.uSendMessage("      > Truncate table " + thisWorkfile);
            DDL.clear();
            DDL.add(trunctable);
            SqlCommands.ExecuteSQL(DDL);
            uCommons.Sleep(3);
            trunctable = "";
            dropwkfl = "";
        }

        if (!dropwkfl.equals("")) {
            uCommons.uSendMessage("      > Dropping table " + thisWorkfile);
            DDL.clear();
            DDL.add(dropwkfl);
            SqlCommands.ExecuteSQL(DDL);
            uCommons.Sleep(1);      // give SQL DB a chance to catch up.
            dropwkfl = "";
        }

        if (!sqlCmds.isEmpty()) {
            if (NamedCommon.isNRT) {
                SqlCommands.ExecuteSQL(sqlCmds);
            } else {
                uCommons.SQLDump(sqlCmds);
                if (NamedCommon.ADMP) {
                    admpCnt = uCommons.ADMPdump(admpCnt, admpDat);
                    admpDat.clear();
                    admpCnt++;
                }
            }
            if (NamedCommon.ZERROR) return false;
            sqlCmds.clear();
            rowCtr = 0;
        }

        if (!uniFail && !NamedCommon.ZERROR && sqlCmds.size() > 0) {
            if (NamedCommon.isNRT) {
                SqlCommands.ExecuteSQL(sqlCmds);
                sqlCmds.clear();
                rowCtr = 0;
            }
            if (NamedCommon.ZERROR) return false;
        }

        // ## Gitlab 921
        if (!NamedCommon.ZERROR) {
            /* ------------------- clean-up the run ------------------- */
            DDL.clear();

            if (!NamedCommon.datOnly) {
                if (!uniFail && !NamedCommon.isNRT) {
                    String vSch = NamedCommon.SqlSchema;
                    if (NamedCommon.uniBase) vSch = "uni";
                    String source = "[" + NamedCommon.SqlDatabase + "]." + "[raw].[" + rawTable + "]";
                    String target = "[" + NamedCommon.SqlDatabase + "].[" + vSch + "].[" + NamedCommon.sqlTarget + "]";
                    if (NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE")) {
                        source = source.replaceAll("\\[", "");
                        source = source.replaceAll("\\]", "");
                        target = target.replaceAll("\\[", "");
                        target = target.replaceAll("\\]", "");
                    }
                    MessageProtocol.UplCtl.set(0, NamedCommon.BatchID);
                    MessageProtocol.UplCtl.set(1, NamedCommon.RunType);
                    MessageProtocol.UplCtl.set(2, NamedCommon.serial);
                    MessageProtocol.UplCtl.set(3, NamedCommon.xMap);
                    MessageProtocol.UplCtl.set(4, "014-Burst");
                    MessageProtocol.UplCtl.set(5, source);
                    MessageProtocol.UplCtl.set(6, sel);
                    MessageProtocol.UplCtl.set(7, target);
                    MessageProtocol.UplCtl.set(8, String.valueOf(rCnt));
                    MessageProtocol.UplCtl.set(9, stCnt.replaceAll("\\,", ""));
                    MessageProtocol.UplCtl.set(10, srCnt.replaceAll("\\,", ""));
                    MessageProtocol.UplCtl.set(11, String.valueOf(mtRecs));
                    MessageProtocol.UplCtl.set(12, String.valueOf(nulRecs));
                    MessageProtocol.UplCtl.set(13, String.valueOf(bigRows));
                    MessageProtocol.UplCtl.set(14, String.valueOf(datRows));
                }
            } else {
                if (uniFail) okay = false;
            }
        } else {
            okay = false;
        }

        return okay;
    }

    private static void HandleDatFiles() {
        // Step 1
        // Get dat files for NamedCommon.u2Source + _ + NamedCommon.DatAct
        //
        String thisFile = NamedCommon.sqlTarget + "_" + NamedCommon.datAct;
        String matchStr = ".dat";
        String fDir = NamedCommon.BaseCamp + "/data/ins";
        File rTmp;
        File dir = new File(fDir);
        File[] matchFiles = null;
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
        String[] matchingFiles = new String[nbrmatches];
        for (int ff = 0; ff < nbrmatches; ff++) { matchingFiles[ff] = ""; }
        String tmp = "";
        int fpos = 0;
        for (int ff = 0; ff < nbrmatches; ff++) {
            tmp = String.valueOf(matchFiles[ff]);
            if (!tmp.contains("raw")) continue;
            if (!tmp.contains(thisFile)) continue;
            matchingFiles[fpos] = tmp;
            fpos++;
        }
        matchFiles = null;
        dir = null;
        // make sure the older ones get inserted first.
        Arrays.sort(matchingFiles, Collections.reverseOrder());
        //
        // Step 2
        // loop through all the files
        //
        UniDynArray inRec;
        int eol=0, fCnt=0, rCnt=0, pCnt=0, lapse=0, totTime=0, eof=matchingFiles.length;
        String datRec, row, rID, rDat, encSeed, msgFmt, showCtr;
        String[] lines;
        String[] columns;
        for (int f=0 ; f < eof ; f++) {
            if (matchingFiles[f].equals("")) continue;
            fCnt++;
            uCommons.uSendMessage("   .) Processing: " + (f+1) + " of " + eof + "  " + matchingFiles[f]);
            datRec = uCommons.ReadDiskRecord(matchingFiles[f]);
            lines = datRec.split("\\r?\\n");
            eol = lines.length;
            uCommons.uSendMessage("   .)           : " + eol + " rows");
            //
            // Step 3
            // process each dat file line as if it came from SQL
            //
            for (int l=0 ; l < eol ; l++) {
                GarbageCollector.CleanUp();
                row = lines[l];
                columns = row.split("\\"+NamedCommon.Komma);
                if (columns.length < 7) continue;       // stops null exceptions
                rID     = columns[1];
                rDat    = columns[6];
                if (rDat.length() == 0) {
                    mtRecs++;
                    continue;
                }
                while (rDat.contains("\r") || rDat.contains("\n")) {
                    rDat = rDat.replace("\r", "");
                    rDat = rDat.replace("\n", "");
                }
                if (NamedCommon.encRaw) {
                    String[] tmpArr;
                    encSeed = "";
                    if (!NamedCommon.AES) {
                        tmpArr = rDat.split("~");
                        rDat = rDat.split("~")[0];
                        if (tmpArr.length > 1) {
                            encSeed = tmpArr[1];
                            rDat = uCipher.v2UnScramble(uCipher.keyBoard25, rDat, encSeed);
                        }
                    } else {
                        rDat = uCipher.v2UnScramble(uCipher.keyBoard25, rDat, encSeed);
                    }
                    tmpArr = null;
                }
                if (!quote.equals("")) {
                    if (rDat.startsWith(quote) && rDat.endsWith(quote)) {
                        rDat = rDat.substring(1, rDat.length());
                        rDat = rDat.substring(0, (rDat.length() - 1));
                    }
                    if (rID.startsWith(quote) && rID.endsWith(quote)) {
                        rID = rID.substring(1, rID.length());
                        rID = rID.substring(0, (rID.length() - 1));
                    }
                }
                rCnt++;
                pCnt++;
                inRec = uCommons.SQL2UVRec(rID + atIM + rDat);
                if (inRec == null) {
                    nulRecs++;
                    if (NamedCommon.showNulls) {
                        uCommons.uSendMessage("NULL record @ " + NamedCommon.uID);
                    }
                } else {
                    ProcessRow(LineArray, inRec);
                }
                if (pCnt > OutputCtr) {
                    msgFmt = "";
                    showCtr = uCommons.oconvM(String.valueOf(rCnt), "MD0,");
                    showCtr = uCommons.RightHash(showCtr, 15);
                    msgFmt += showCtr;
                    got = System.nanoTime();
                    laps = (got - sent) / nanoSecs;
                    lapse = (int) (long) laps;
                    totTime += lapse;
                    showCtr = uCommons.oconvM(String.valueOf(datRows), "MD0,");
                    showCtr = uCommons.RightHash(showCtr, 15);
                    msgFmt += " row(s) read in and " + showCtr
                            + " rows written out in :: "
                            + lapse + " seconds";
                    uCommons.uSendMessage(msgFmt);
                    pCnt = 0;
                    sent = System.nanoTime();
                    GarbageCollector.CleanUp();
                }
            }

            rTmp = new File(matchingFiles[f]);
            rTmp.delete();
            rTmp = null;
        }
        if (fCnt == 0) uCommons.uSendMessage("Found 0 raw files for " + thisFile);
    }

    public static String GetRawTable() {
        String rawTable = "";
        String[] options = new String[3];
        String[] optkeys = new String[3];
        optkeys[0] = "sqlTable";
        optkeys[1] = "rawTable";
        optkeys[2] = "u2File";
        options[0] = uCommons.APIGetter("sqlTable");
        options[1] = uCommons.APIGetter("rawTable");
        options[2] = uCommons.APIGetter("u2File");
        boolean found = false;
        if (!NamedCommon.isNRT) uCommons.uSendMessage("   .) Find the raw table :-");
        int eoi = options.length;
        for (int i=0 ; i < eoi; i++) {
            rawTable = options[i];
            if (!rawTable.equals("")) {

                // why datAct and not APImsg.APIget("dacct")
                if (!rawTable.endsWith("_" + NamedCommon.datAct)) rawTable += "_" + NamedCommon.datAct;

                rawTable = rawTable.replaceAll("\\.", "_");
                rawTable = rawTable.replaceAll("\\,", "_");
                rawTable = rawTable.replaceAll("\\ ", "_");

                if (!NamedCommon.isNRT) {
                    uCommons.uSendMessage("      .("+i+") Check table [" + NamedCommon.rawDB + "].[raw].[" + rawTable + "]");
                }
                found = uCommons.TableExists(rawTable);
                if (!found) {
                    rawTable = "";
                    uCommons.uSendMessage("      .("+i+") Not found");
                    if (NamedCommon.ZERROR) {
                        uCommons.uSendMessage(NamedCommon.Zmessage);
                        NamedCommon.Zmessage = "";
                        return "";
                    }
                } else {
                    break;
                }
            } else {
                uCommons.uSendMessage("      .("+i+") Check for " + optkeys[i] + " - not used.");
            }
        }

        if (rawTable.equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Cannot find the raw table to obtain data !";
        } else {
            if (!NamedCommon.isNRT) uCommons.uSendMessage("      .) Using table " + rawTable);
        }
        return rawTable;
    }

    public static boolean FlushIds(ArrayList<String> thisGrp, int pCnt, String serial, String fname) {
        boolean EndSw = false;
        DDL = new ArrayList<String>();
        int totNbr = thisGrp.size();
        String val = "";
        for (int ix = 0; ix < totNbr; ix++) {
            val = thisGrp.get(ix);
            if (val.length() == 0 || val.equals("[<END>]")  || val.equals("[<EOP>]")) {
                thisGrp.remove(ix);
                ix--;
                totNbr--;
                if (val.contains("[<EOP>]")) EndSw = true;
            }
        }
        if (totNbr > 0) {
            String thisFile = fname + "_" + pCnt + ".ids";
            String cols = "#uID,-Serial,Spare";
            cols = cols.replaceAll("\\-", "");
            cols = cols.replaceAll("\\.", "");
            cols = cols.replaceAll("\\*", "");
            cols = cols.replaceAll("\\#", "");
            String insCmd = "INSERT INTO " + thisWorkfile + " (" + cols + ") VALUES (";

            BufferedWriter bWriter = null;
            if (NamedCommon.BulkLoad) {
                bWriter = uCommons.CreateFile(fDir, fname + "_" + pCnt, ".ids");
            }
            if (!NamedCommon.ZERROR) {
                boolean firstLine = true, uProceed = true;
                for (int ix = 0; ix < totNbr; ix++) {
                    val = quote + thisGrp.get(ix) + quote;
                    if (!val.contains("[<END>]")) {
                        try {
                            if (!firstLine) bWriter.newLine();
                            if (NamedCommon.BulkLoad) {
                                bWriter.write(val + Komma + serial + Komma + "MT");
                            } else {
                                DDL.add(insCmd + val + Komma + quote + serial + quote + Komma + quote + "MT" + quote + ")");
                            }
                            firstLine = false;
                        } catch (IOException e) {
                            uCommons.uSendMessage(e.getMessage());
                        }
                    }
                }

                if (NamedCommon.BulkLoad) {
                    try {
                        bWriter.newLine();
                        bWriter.flush();
                        bWriter.close();
                    } catch (IOException e) {
                        uCommons.eMessage = "[FATAL]   Cannot close " + fname + "_" + pCnt + ".ids";
                        uCommons.eMessage += "\n" + MessageProtocol.messageText;
                        String ZERROR = uCommons.eMessage;
                        uCommons.uSendMessage(uCommons.eMessage);
                        NamedCommon.ZERROR = true;
                        uProceed = false;
                    }
                }

                if (uProceed) {
                    if (NamedCommon.BulkLoad) {
                        String buildFile = fname + "_" + pCnt + ".ids";
                        File tmpFl = new File(fDir, buildFile);
                        boolean junk;
                        junk = tmpFl.setReadable(true, false);
                        junk = tmpFl.setWritable(true, false);
                        junk = tmpFl.setExecutable(true, false);

                        String fromDir = NamedCommon.datPath;
                        if (!fromDir.equals(NamedCommon.slash)) fromDir += NamedCommon.slash;
                        String BulkImport = SqlCommands.BulkImport(thisFile, fromDir);

                        DDL.clear();
                        DDL.add(BulkImport);
                        SqlCommands.ExecuteSQL(DDL);
                        DDL.clear();
                        tmpFl.delete();
                        tmpFl = null;
                    }  else {
                        uCommons.uSendMessage("   .) ******** Manually Loading ID's");
                        SqlCommands.ExecuteSQL(DDL);
                        uCommons.uSendMessage("   .) ******** Done. You need Bulk Inserts !!");
                    }
                    DDL.clear();
                    if (NamedCommon.StopNow.contains("<<FAIL>>")) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "ERROR: Could not bulk load " + thisFile;
                        uCommons.eMessage = NamedCommon.Zmessage;
                        NamedCommon.Zmessage = "";
                    }
                    if (!uCommons.eMessage.equals("")) uCommons.uSendMessage(uCommons.eMessage);
                }
            }
        } else {
            EndSw = true;
        }
        return EndSw;
    }

    public static void ProcessRow(UniDynArray lineArray, UniDynArray inRec) {
        Initialise();
        PrepareCsvFilter(lineArray, inRec);
        if (deepDive) uCommons.uSendMessage("    ProcessRow");
        ExtractData(lineArray, inRec);
        GarbageCollector.CleanUp();

        if (NamedCommon.ZERROR) return;     // if ZERROR, SQL is probably off-line

        if (sqlCmds.size() >= cmdBlockSize && !NamedCommon.isNRT) {
            uCommons.SQLDump(sqlCmds);
            sqlCmds.clear();
            if (NamedCommon.ZERROR) uCommons.uSendMessage("Data not dumped");
            if (NamedCommon.ADMP) {
                admpCnt = uCommons.ADMPdump(admpCnt, admpDat);
                admpDat.clear();
                admpCnt++;
            }
            GarbageCollector.CleanUp();
        }

    }

    public static void SetDumpFlag(boolean val) {
        dumpData = val;
    }

    public static void Initialise() {
        atIM = NamedCommon.IMark; // + NamedCommon.IMark;
        atFM = NamedCommon.FMark; // + NamedCommon.FMark;
        atVM = NamedCommon.VMark; // + NamedCommon.VMark;
        atSM = NamedCommon.SMark; // + NamedCommon.SMark;
        AVF = new ArrayList<>();
        AVT = new ArrayList<>();
        MVF = new ArrayList<>();
        MVT = new ArrayList<>();
        SVF = new ArrayList<>();
        SVT = new ArrayList<>();
        ABK = new ArrayList<>();
        MBK = new ArrayList<>();
        SBK = new ArrayList<>();
        AllDone = new ArrayList<ArrayList<Integer>>();
        OutputCtr = NamedCommon.showAT;
        if (NamedCommon.bshowAT > 0) OutputCtr = NamedCommon.bshowAT;
        nullSamples = "";
        if (" REFRESH FULL ".indexOf(NamedCommon.RunType) > 0) {
            File bigdata = new File(MessageProtocol.data + NamedCommon.u2FileRef + ".2big");
            if (!bigdata.exists()) bigdata.delete();
        }
    }

    public static void PrepareCsvFilter(UniDynArray lineArray, UniDynArray inRec) {
        int nbrLines = lineArray.dcount();
        String av, mv, sv, sTag, sCnv;
        boolean skipLine;

        NamedCommon.fHandles.clear();
        NamedCommon.fTagNames.clear();
        NamedCommon.fHandles.add(0, null);
        NamedCommon.fTagNames.add(0, "DO!NOT!USE!ZERO");
        if (!NamedCommon.fmvArrayIsSet) {
            if (NamedCommon.fmvArray == null) {
                NamedCommon.fmvArray = new UniDynArray();
            } else {
                int eol = NamedCommon.fmvArray.dcount();
                for (int uda = 0; uda < eol; uda++) { if (NamedCommon.fmvArray.extract(0) != null) NamedCommon.fmvArray.delete(0); }
            }
            NamedCommon.fmvArray.replace(0, "");
            NamedCommon.fmvArrayIsSet = true;
        }
        AVF.add(0, 0);
        AVT.add(0, 0);
        ABK.add(0, 0);
        MVF.add(0, 0);
        MVT.add(0, 0);
        MBK.add(0, 0);
        SVF.add(0, 0);
        SVT.add(0, 0);
        SBK.add(0, 0);
        thisAv = 0;
        thisMv = 0;
        thisSv = 0;
        Integer[] ans = new Integer[3];
        String[] sTmp = new String[3];
        int idx = 0;
        for (int i = 1; i <= nbrLines; i++) {
            idx++;
            AVF.add(idx, 0);
            AVT.add(idx, 0);
            ABK.add(idx, 0);
            MVF.add(idx, 0);
            MVT.add(idx, 0);
            MBK.add(idx, 0);
            SVF.add(idx, 0);
            SVT.add(idx, 0);
            SBK.add(idx, 0);
            /*            Set-up the file Translates & Joins                    */

            if (NamedCommon.isWhse) {
                sTag = "";
            } else {
                sTag = String.valueOf(lineArray.extract(i, 7));
            }
            sCnv = String.valueOf(lineArray.extract(i, 4));
            skipLine = false;
            if (sCnv.contains("|") && inRec != null) skipLine = cnvControl(sCnv);

            if (!skipLine) {
                av = String.valueOf(lineArray.extract(i, 1));
                mv = String.valueOf(lineArray.extract(i, 2));
                sv = String.valueOf(lineArray.extract(i, 3));

                if (av.trim().equals("")) av = "0";
                if (mv.trim().equals("")) mv = "1";
                if (sv.trim().equals("")) sv = "1";

                for (int clr = 0; clr < ans.length; clr++) { ans[clr] = 0; }
                for (int clr = 0; clr < sTmp.length; clr++) { sTmp[clr] = ""; }

                String tempRec = GetTaggedRec(sTag);
                if (av.toLowerCase().contains("n")) {
                    if (!sTag.equals("") && !tempRec.equals("")) {
                        sTmp = UnPackMultiValuedField("A" + av, uCommons.SQL2UVRec(GetTaggedRec(sTag)));
                    } else {
                        sTmp = UnPackMultiValuedField("A" + av, inRec);
                    }
                } else {
                    sTmp = new String[]{av, av};
                }
                ans = StringtoInteger(sTmp);

                ABK.set(idx, ans[0]);
                AVF.set(idx, ans[0]);
                if (ans[1] > AVT.get(idx)) AVT.set(idx, ans[1]);
                thisAv = ans[0];

                int[] aloop = new int[]{ans[0], ans[1]};
                for (int ii = aloop[0]; ii <= aloop[1]; ii++) {
                    thisAv = ii;
                    for (int clr = 0; clr < ans.length; clr++) { ans[clr] = 0; }
                    for (int clr = 0; clr < sTmp.length; clr++) { sTmp[clr] = ""; }

                    if (mv.toLowerCase().contains("n")) {
                        if (!sTag.equals("") && !tempRec.equals("")) {
                            sTmp = UnPackMultiValuedField("M" + mv, uCommons.SQL2UVRec(tempRec));
                        } else {
                            sTmp = UnPackMultiValuedField("M" + mv, inRec);
                        }
                    } else {
                        sTmp = new String[]{mv, mv};
                    }
                    ans = StringtoInteger(sTmp);
                    thisMv = ans[0];
                    MBK.set(idx, thisMv);
                    MVF.set(idx, thisMv);
                    if (ans[1] > MVT.get(idx)) MVT.set(idx, ans[1]);

                    int[] mloop = new int[]{ans[0], ans[1]};
                    for (int iii = mloop[0]; iii <= mloop[1]; iii++) {
                        thisMv = iii;
                        for (int clr = 0; clr < ans.length; clr++) { ans[clr] = 0; }
                        for (int clr = 0; clr < sTmp.length; clr++) { sTmp[clr] = ""; }

                        if (sv.toLowerCase().contains("n")) {
                            if (!sTag.equals("") && !tempRec.equals("")) {
                                sTmp = UnPackMultiValuedField("S" + sv, uCommons.SQL2UVRec(GetTaggedRec(sTag)));
                            } else {
                                sTmp = UnPackMultiValuedField("S" + sv, inRec);
                            }
                        } else {
                            sTmp = new String[]{sv, sv};
                        }
                        ans = StringtoInteger(sTmp);
                        thisSv = ans[0];
                        SBK.set(idx, thisSv);
                        SVF.set(idx, thisSv);
                        if (ans[1] > SVT.get(idx)) SVT.set(idx, ans[1]);
                    }
                    mloop = null;
                }
                aloop = null;
            } else {
                AVF.remove(idx);
                AVT.remove(idx);
                ABK.remove(idx);
                MVF.remove(idx);
                MVT.remove(idx);
                MBK.remove(idx);
                SVF.remove(idx);
                SVT.remove(idx);
                SBK.remove(idx);
                idx--;
            }
        }
        for (int clr = 0; clr < ans.length; clr++) { ans[clr] = 0; }
        for (int clr = 0; clr < sTmp.length; clr++) { sTmp[clr] = ""; }

        int ctr = 0, aLen = nbrLines + 1;
        while (ctr <= 3) {
            ArrayList<Integer> donesw = new ArrayList<Integer>();
            AllDone.add(ctr, donesw);
            for (int ad = 0; ad < aLen; ad++) { donesw.add(ad, 0); }
            ctr++;
        }
    }

    private static Integer[] StringtoInteger(String[] sTmp) {
        Integer[] result = new Integer[sTmp.length];
        for (int tt = 0; tt < sTmp.length; tt++) {
            try {
                result[tt] = Integer.valueOf(sTmp[tt]);
            } catch (NumberFormatException nfe) {
                result[tt] = 0;
            }
        }
        return result;
    }

    private static boolean cnvControl(String sCnv) {
        String[] cnvCmds = uStrings.gSplit2Array(sCnv, "}");
        int nbrCmds = cnvCmds.length;
        boolean skipLine = false;
        for (int xx = 0; xx < nbrCmds; xx++) {
            String thisTag = "";
            String thisFile = "";
            thisTag = uStrings.gSplit2Array(cnvCmds[xx], "\\")[0];
            thisTag = uStrings.gSplit2Array(thisTag, " ")[0];
            if (cnvCmds[xx].contains("|")) {
                thisTag = uStrings.gSplit2Array(cnvCmds[xx], "|")[0];
                thisTag = uStrings.gSplit2Array(thisTag, " ")[0];
            }

            switch (thisTag) {
                case "=assoc":
                    for (int rm = 0; rm < nbrCmds; rm++) { cnvCmds[rm] = ""; }
                    break;
                case "R":
                    // R - reorganise and relate with other fields
                    skipLine = true;
                    break;
                case "X":
                    // X - translate = read a record from another file
                    //   - e.g. : X|CLIENT|$CLIENT
                    skipLine = true;
                    String[] tmpX;

                    tmpX = uStrings.gSplit2Array(cnvCmds[xx], "|");
                    String xFile = tmpX[1];
                    String xVar = tmpX[2];
                    uCommons.uSendMessage("      Preparing \"X\"-late");
                    if (NamedCommon.fTagNames.indexOf(xVar) < 0) {
                        if (u2Commons.uOpenFile(xFile, "2")) {
                            String fVar = xVar;                             /* e.g. $CLIENT         */
                            NamedCommon.fHandles.add(NamedCommon.U2File);   /* handle to CLIENT     */
                            NamedCommon.fTagNames.add(fVar);
                            AA = NamedCommon.fTagNames.indexOf(fVar);
                            NamedCommon.fmvArray.insert(AA, 1, 0, fVar);
                        }
                    }
                    break;
                case "JOIN":
                    // JOIN - use a pre-defined SELECT stmt to get rows from another file
                    uCommons.uSendMessage("      Preparing \"JOIN\" ");
                    skipLine = true;
                    String[] tmpcmdStg = uStrings.gSplit2Array(cnvCmds[xx], "|");
                    thisFile = tmpcmdStg[1];
                    String cmd = tmpcmdStg[2];
                    String fVar = tmpcmdStg[3];
                    thisFile = uStrings.gSplit2Array(thisFile, " ")[0];
                    boolean uniFailed = false;
                    boolean fOpen = false;
                    switch (NamedCommon.protocol) {
                        case "u2cs":
                            NamedCommon.U2File = u2Commons.uOpen(thisFile);
                            fOpen = (NamedCommon.U2File != null);
                            break;
                        case "real":
                            fOpen = (!rCommons.ReadAnItem("MD", thisFile, "", "", "").equals(""));
                            NamedCommon.U2File = null;
                            break;
                        case "u2mnt":
                            fOpen = (!u2Commons.ReadAnItem("VOC", thisFile, "", "", "").equals(""));
                            break;
                        case "u2sockets":
                            fOpen = (!u2Commons.ReadAnItem("VOC", thisFile, "", "", "").equals(""));
                            break;
                        case "rmi.u2cs":
                            System.out.println("rmi:  is being developed - not ready yet");
                        default:
                            fOpen = u2Commons.uOpenFile(thisFile, "2");
                    }

                    if (fOpen) {
                        NamedCommon.fHandles.add(NamedCommon.U2File);       /* handle to CLIENT */
                        NamedCommon.fTagNames.add(fVar);                    /* e.g. $CLIENT     */
                        AA = NamedCommon.fTagNames.indexOf(fVar);
                        NamedCommon.fmvArray.replace(AA, 1, 0, fVar);
                    } else {
                        uniFailed = true;
                    }

                    if (uniFailed) {
                        uCommons.uSendMessage("================================================");
                        uCommons.uSendMessage("***   Cannot execute command in ProcessRow() ");
                        uCommons.uSendMessage("***   Command is " + cmd);
                        uCommons.uSendMessage("***   1.) Does the file exist (spelling) in " + NamedCommon.datAct + " account");
                        uCommons.uSendMessage("***   2.) Does the 'EVAL' work (quotes)");
                        uCommons.uSendMessage("***   TEST this command at TCL before restarting !! ");
                        uCommons.uSendMessage("***   ... no action taken ...");
                        uCommons.uSendMessage("================================================");
                        skipLine = true;
                    }
                    break;
                default:
                    skipLine = false;
            }
        }
        return skipLine;
    }

    public static void ExtractData(UniDynArray lineArray, UniDynArray inRec) {

        String[] tCols = NamedCommon.tblCols.split("\\,");
        int nbrItems = lineArray.dcount();
        int SumDone, i, loopCt;
        int a, m, s, iVF, procCnt;
        boolean skipLine;
        String tmpStr;
        String av, mv, sv, tg, rp;
        int mt, st, mbk, sbk;
        String cav, cmv, csv;
        SumDone = 0;
        AvDone = 0;
        MvDone = 0;
        SvDone = 0;
        UniDynArray thisLine;
        ArrayList<String> VAL = new ArrayList<>();
        ArrayList<String> REPL = new ArrayList<>();
        ArrayList<String> TMPL = new ArrayList<>();
        String value, c = "", Base;
        i = 1;
        iVF = 0;
        procCnt = 1;
        loopCt = 1;
        if (inRec.dcount() < 1) {
            if (NamedCommon.showNulls) {
                uCommons.uSendMessage("   .) empty record for item [" + NamedCommon.uID + "]");
            } else {
                if (nullSamples.length() < 40) nullSamples += String.valueOf(NamedCommon.uID) + " ";
            }
            nulRecs++;
            if (NamedCommon.isWhse) nbrItems = -1;
        }
        boolean endOfLoop = (i > nbrItems);
        boolean AvSw, MvSw, SvSw;
        String APart, MPart, SPart;

        boolean storeAMS;
        MPart = ".1";
        SPart = ".1";
        ams = "";

        String[] dbgline = new String[]{"", "", "", "", ""};
        String dbgcsv = "", dbgifr = "", dbgito = "", dbgths = "", dbgSep = "&";

        boolean dbglog = (APImsg.APIget("dbglog").toLowerCase().equals("true"));
        tagsMatched = new ArrayList<>();
        boolean taggedColumn = false;

        while (!endOfLoop) {
            thisLine = new UniDynArray(lineArray.extract(i));
            mt = 0;
            st = 0;
            mbk = 0;
            sbk = 0;

            SvSw = (AllDone.get(3).get(i) > 0);
            MvSw = (AllDone.get(2).get(i) > 0);
            AvSw = (AllDone.get(1).get(i) > 0);

            cav = String.valueOf(thisLine.extract(1, 1)).toUpperCase();
            cmv = String.valueOf(thisLine.extract(1, 2)).toUpperCase();
            csv = String.valueOf(thisLine.extract(1, 3)).toUpperCase();
            if (!cav.contains("N")) cav = GetValue(cav);
            if (!cmv.contains("N")) cmv = GetValue(cmv);
            if (!csv.contains("N")) csv = GetValue(csv);
            if (NamedCommon.ZERROR) {
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return;
            }

            tmpStr = String.valueOf(thisLine.extract(1, 4)) + "   ";
            skipLine = (tmpStr.contains("X|") || tmpStr.contains("JOIN"));

            if (!skipLine) {
                av = cav;
                mv = cmv;
                sv = csv;
                storeAMS = (cav + cmv + csv).toLowerCase().contains("n");   // is this an N loop?

                iVF = procCnt;
                if (storeAMS) {
                    if (NamedCommon.isWhse || NamedCommon.isNRT) {
                        av = String.valueOf(AVF.get(iVF));
                        mv = String.valueOf(MVF.get(iVF));
                        sv = String.valueOf(SVF.get(iVF));
                        mt = MVT.get(iVF);
                        st = SVT.get(iVF);
                        mbk = MBK.get(iVF);
                        sbk = SBK.get(iVF);
                    } else {
                        if (csv.contains("!")) {
                            NamedCommon.showLineage = true;
                            csv = csv.replaceAll("\\!", "");
                        }
                        if (cmv.contains("!")) {
                            NamedCommon.showLineage = true;
                            cmv = cmv.replaceAll("\\!", "");
                        }
                        if (cav.contains("!")) {
                            NamedCommon.showLineage = true;
                            cav = cav.replaceAll("\\!", "");
                        }

                        if (sv.contains("N")) {
                            SvSw = true;
                            st = 0;
                        }
                        if (mv.contains("N")) {
                            MvSw = true;
                            mt = 0;
                        }
                        if (av.contains("N")) {
                            AvSw = true;
//                        av = "1";
                        }
                    }
                }
            } else {
                av = cav;
                mv = cmv;
                sv = csv;
            }

            if (deepDive & ddID.contains(ddLookFor))  uCommons.uSendMessage("    Looking at "+ddID+"<"+av+","+mv+","+sv+">");

            if (csv.toLowerCase().contains("n") && SvSw) sv = String.valueOf(st + 1);
            if (cmv.toLowerCase().contains("n") && MvSw) mv = String.valueOf(mt + 1);

            // Sometimes, users define an "N" loop but the data does not support it.
            // This results in lots of null columns in the SQL table.
            //
            // While the code below "works" - it is best to NOT use "N" in the map
            //       when the data does NOT require it.
            // -----------------------------------------------------------------------

            if (dbglog) {
                dbgcsv = "<" + cav + "," + cmv + "," + csv + ">";
                dbgifr = "<" + AVF.get(iVF) + "," + MVF.get(iVF) + "," + SVF.get(iVF) + ">";
                dbgito = "<" + AVT.get(iVF) + "," + MVT.get(iVF) + "," + SVT.get(iVF) + ">";
                dbgths = "<" + av + "," + mv + "," + sv + ">";
            }

            NamedCommon.isAssoc = false;
            rp = String.valueOf(thisLine.extract(1, 8));
            c = String.valueOf(thisLine.extract(1, 4));
            tg = String.valueOf(thisLine.extract(1, 7));
            if (!tg.equals("")) {
                taggedColumn = true;
                if (dataTags.contains(tg)) taggedColumn = false;
            } else {
                taggedColumn = false;
            }

            transformed = false;

            if (SvSw && csv.contains("N") && NamedCommon.isRest) {
                value = "@@";
            } else {
                if (MvSw && cmv.contains("N") && NamedCommon.isRest) {
                    value = "@@";
                } else {
                    if (AvSw && cav.contains("N") && NamedCommon.isRest) {
                        value = "@@";
                    } else {
                        if (av.equals("")) av = "0";
                        if (mv.equals("")) mv = "1";
                        if (sv.equals("")) sv = "1";

                        a = Integer.valueOf(av);
                        m = Integer.valueOf(mv);
                        s = Integer.valueOf(sv);
                        if (m == 0) m = 1;
                        if (s == 0) s = 1;

                        if (!taggedColumn) {
                            if (a == 0) {
                                Base = String.valueOf(NamedCommon.uID);
                            } else {
                                Base = String.valueOf(inRec.extract(a, m, s));
                                if (NamedCommon.CleanseData)  Base = u2Commons.Cleanse(Base);

                            }
                        } else {
                            Base = GetTaggedData((iVF), tg, thisLine);
                        }
                        if (deepDive & ddID.contains(ddLookFor))  uCommons.uSendMessage("       Base: "+Base);

                        if (Base.contains("\r")) Base = Base.replaceAll("\\r", "");
                        if (c.length() > 1 && Base.length() > 0 && !transformed) {
                            if (!Base.contains(NamedCommon.FMark)) {
                                if (deepDive & ddID.contains(ddLookFor))  uCommons.uSendMessage("       Cnv : "+c);
                                value = ConvRawData(c, Base);
                                if (value.equals(skipStr)) {
                                    skipLine = true;
                                    value = "";
                                }
                            } else {
                                int caCnt = u2Commons.sDcount(Base, "A");
                                String tmp;
                                value = "";
                                for (int ca = 1; ca <= caCnt; ca++) {
                                    tmp = u2Commons.sExtract(Base, ca, 0, 0);
                                    if (ca > 1 && ca <= caCnt) value += NamedCommon.FMark;
                                    if (deepDive & ddID.contains(ddLookFor))  uCommons.uSendMessage("       Cnv : "+c);
                                    tmp = ConvRawData(c, tmp);
                                    if (value.equals(skipStr)) continue;
                                    value += tmp;
                                    tmp = "";
                                }
                                tmp = value;
                                tmp = "";
                            }
                        } else {
                            value = Base;
                        }
                        if (deepDive & ddID.contains(ddLookFor))  uCommons.uSendMessage("       Base: "+value);

                        if (!quote.equals("")) {
                            if (value.startsWith(quote)) {
                                value = value.substring(1, value.length());
                            }
                            if (value.endsWith(quote)) {
                                value = value.substring(0, (value.length() - 1));
                            }
                        }

                        if (NamedCommon.isRest) {
                            HandleMultiValues(thisLine, REPL, TMPL, procCnt, rp, a + "-" + m + "-" + s, value);
                            if (NamedCommon.ZERROR) return;
                        }
                        APart = String.valueOf(loopCt);
                        if (m == 0) m = 1;
                        if (s == 0) s = 1;
                        if (m <= mt && mt != mbk) MPart = "." + m;
                        if (s <= st && st != sbk) SPart = "." + s;
                        ams = APart + MPart + SPart;
                    }
                }
            }
            if (value.equals("@@") && NamedCommon.isRest) {
                String datum = "", dElement = "";
                int avf, avt, mvf, mvt, svf, svt;
                int aval, mval, sval;
                boolean isAV = false, isMV = false, isSV = false;
                UniDynArray workrec;
                if (taggedColumn) {
                    // tg is used for =assoc values. These values are stored in
                    // NamedCommon.DataList and are "matched" to be the same in
                    // mv depth as well as sv depth.
                    if (tagsMatched.indexOf(tg) < 0) MatchTagValues(tg);
                    datum = GetTaggedData(iVF, tg, thisLine);
                } else {
                    // use the actual U2 record
                    workrec = inRec;
                    if (AvSw || av.toLowerCase().contains("n")) {
                        av = av.toLowerCase().replace("n", "");
                        if (av.equals("")) av = "1";
                        avf = Integer.valueOf(av);
                        avt = workrec.dcount(0);
                        isAV = true;
                    } else {
                        avf = Integer.valueOf(av);
                        avt = avf;
                    }
                    for (aval = avf; aval <= avt; aval++) {
                        if (MvSw || mv.toLowerCase().contains("n")) {
                            mv = mv.replace("n", "");
                            if (mv.equals("")) mv = "1";
                            mvf = Integer.valueOf(mv);
                            mvt = workrec.dcount(aval);
                            isMV = true;
                        } else {
                            mvf = Integer.valueOf(mv);
                            mvt = mvf;
                        }
                        for (mval = mvf; mval <= mvt; mval++) {
                            if (SvSw || sv.toLowerCase().contains("n")) {
                                sv = sv.replace("n", "");
                                if (sv.equals("")) sv = "1";
                                svf = Integer.valueOf(sv);
                                svt = workrec.dcount(aval, mval);
                                isSV = true;
                            } else {
                                svf = Integer.valueOf(sv);
                                svt = svf;
                            }
                            for (sval = svf; sval <= svt; sval++) {
                                if (deepDive & ddID.contains(ddLookFor)) uCommons.uSendMessage("    Looking at "+ddID+"<"+aval+","+mval+","+sval+">");
                                dElement = workrec.extract(aval, mval, sval).toString();
                                if (deepDive & ddID.contains(ddLookFor)) uCommons.uSendMessage("    element " + dElement);
                                datum += dElement;
                                if (sval > 0 && sval < svt && isSV) datum += atSM;
                                if (deepDive & ddID.contains(ddLookFor)) uCommons.uSendMessage("    datum "+datum);
                            }
                            if (mval > 0 && mval < mvt && isMV) datum += atVM;
                        }
                        if (aval > 0 && aval < avt && isAV) datum += atFM;
                    }
                }

                Base = datum;
                if (deepDive & ddID.contains(ddLookFor)) uCommons.uSendMessage("    Base "+datum);
                if (c.length() > 1 && Base.length() > 0 && !transformed) {
                    if (!Base.contains(NamedCommon.FMark)) {
                        value = ConvRawData(c, Base);
                        if (value.equals(skipStr)) {
                            skipLine = true;
                            value = "";
                        }
                    } else {
                        int caCnt = u2Commons.sDcount(Base, "A");
                        String tmp;
                        value = "";
                        for (int ca = 1; ca <= caCnt; ca++) {
                            tmp = u2Commons.sExtract(Base, ca, 0, 0);
                            if (ca > 1 && ca <= caCnt) value += NamedCommon.FMark;
                            tmp = ConvRawData(c, tmp);
                            if (value.equals(skipStr)) continue;
                            value += tmp;
                        }
                        tmp = "";
                    }
                } else {
                    value = Base;
                }
                if (deepDive & ddID.contains(ddLookFor)) uCommons.uSendMessage("    HandleMultiValues ");
                if (!skipLine) HandleMultiValues(thisLine, REPL, TMPL, procCnt, rp, cav + "-" + cmv + "-" + csv, value);
                if (NamedCommon.ZERROR) return;
                if (deepDive & ddID.contains(ddLookFor)) uCommons.uSendMessage("    done. ");
            }

            if (value.contains(NamedCommon.Komma) && NamedCommon.Komma.length() > 0) {
                String oldVal = "\\" + NamedCommon.Komma;
                String newVal = "!&";
                String rplVal = oldVal + oldVal;
                while (value.contains(NamedCommon.Komma)) {
                    value = value.replaceAll(oldVal, newVal);
                }
                while (value.contains("!&")) {
                    value = value.replaceAll(newVal, rplVal);
                }
            }

            /* ------------------------------------------------------------------------------ */

            if (dbglog) {
                dbgline[0] += uCommons.LeftHash(dbgcsv, 12) + dbgSep;
                dbgline[1] += uCommons.LeftHash(dbgifr, 12) + dbgSep;
                dbgline[2] += uCommons.LeftHash(dbgito, 12) + dbgSep;
                dbgline[3] += uCommons.LeftHash(dbgths, 12) + dbgSep;
                dbgline[4] += uCommons.LeftHash("[" + value + "]", 12) + dbgSep;
            }

            /* ------------------------------------------------------------------------------ */

            if (!skipLine) {
                VAL.add(procCnt - 1, value);
                procCnt++;
            }
            i++;
            if (i > nbrItems) {
                if (dbglog) {
                    System.out.println("  ID = " + NamedCommon.uID);
                    System.out.println(" csv = " + dbgline[0]);
                    System.out.println("from = " + dbgline[1]);
                    System.out.println("  to = " + dbgline[2]);
                    System.out.println("-----------------------------------------------------------------------------------------");
                    System.out.println("vals = " + dbgline[4]);
                    System.out.println("this = " + dbgline[3]);
                    dbgline[0] = "";
                    dbgline[1] = "";
                    dbgline[2] = "";
                    dbgline[3] = "";
                    dbgline[4] = "";
                }

                if (dumpData) {
                    OutputDataRow(VAL, ams);
                    if (NamedCommon.ZERROR) return;
                }
                VAL.clear();

                ams = "";
                MPart = ".0";
                SPart = ".0";
                if (NamedCommon.isWhse) {
                    if (!SvSw) SvDone = 0;
                    if (!MvSw) MvDone = 0;
                    if (!AvSw) AvDone = 0;
                    SumDone = FlipValues(nbrItems);
                    if (SvDone > 0) SVF = new ArrayList<>(SBK);
                    if (SvDone > 0 && MvDone > 0) MVF = new ArrayList<>(MBK);
                    if (SvDone > 0 && MvDone > 0 && AvDone > 0) AVF = new ArrayList<>(ABK);
                } else {
                    SumDone = 3;
                }

                loopCt++;
                i = 1;
                iVF = 0;
                procCnt = 1;
            }
            endOfLoop = (SumDone >= 3);
        }

        if (!VAL.isEmpty() && dumpData) {
            OutputDataRow(VAL, ams);
            VAL.clear();
        }
        if (NamedCommon.isRest) {
            REPL.clear();
            TMPL.clear();
        }
        if (NamedCommon.debugging && tagsMatched.size() > -1) {
            for (int t = 0; t < tagsMatched.size(); t++) {
                tg = tagsMatched.get(t);
                int pos = NamedCommon.SubsList.indexOf(tg);
                String dvals = NamedCommon.DataList.get(pos);
                uCommons.uSendMessage("Tag : " + tg + " ------------------------------");
                uCommons.uSendMessage("Data: " + dvals);
            }
        }
        lineArray = null;
        inRec = null;
        tCols = null;
        dbgline = null;
        dbgcsv = null;
        dbgifr = null;
        dbgito = null;
        dbgths = null;
        if (NamedCommon.isNRT) {
            SqlCommands.ExecuteSQL(sqlCmds);
            sqlCmds.clear();
        }
    }

    public static void MatchTagValues(String tg) {
        int pos = NamedCommon.SubsList.indexOf(tg);
        if (pos < 0) return;
        String values = NamedCommon.DataList.get(pos);
        String newstr = values;
        String thisVal = "", newVals = "";

        // 1. Get the Max AV MV and SV values
        int nbrAvs = 0, nbrMvs = 0, nbrSvs = 0;
        int maxAvs = 0, maxMvs = 0, maxSvs = 0;
        nbrAvs = u2Commons.CountValues(NamedCommon.FMark, values) + 1;
        if (nbrAvs > maxAvs) maxAvs = nbrAvs;
        for (int a = 1; a <= maxAvs; a++) {
            thisVal = u2Commons.sExtract(values, a, 0, 0);
            nbrMvs = u2Commons.CountValues(NamedCommon.VMark, thisVal) + 1;
            if (nbrMvs > maxMvs) maxMvs = nbrMvs;
            for (int m = 1; m <= maxMvs; m++) {
                nbrSvs = u2Commons.CountValues(NamedCommon.SMark, thisVal) + 1;
                if (nbrSvs > maxSvs) maxSvs = nbrSvs;
            }
        }

        // 2. Backfill to ensure all places are filled.
        for (int a = 1; a <= maxAvs; a++) {
            thisVal = u2Commons.sExtract(values, a, 0, 0);
            newVals = BackFill(thisVal, maxMvs, NamedCommon.VMark);
            if (!newVals.equals(thisVal)) newstr = u2Commons.sReplace(newstr, a, 0, 0, newVals);
            if (thisVal.contains(NamedCommon.SMark)) {
                for (int m = 1; m <= maxMvs; m++) {
                    thisVal = u2Commons.sExtract(values, a, m, 0);
                    newVals = BackFill(thisVal, maxSvs, NamedCommon.SMark);
                    if (!newVals.equals(thisVal)) newstr = u2Commons.sReplace(newstr, a, m, 0, newVals);
                }
            }
        }
        NamedCommon.DataList.set(pos, newstr);
        tagsMatched.add(tg);
    }

    public static String BackFill(String inString, int maxval, String uvMarker) {
        if (!NamedCommon.isAssoc) return inString;
        String element = "", lastVal = "";
        StringBuilder ans = new StringBuilder();
        int thisCnt = u2Commons.CountValues(uvMarker, inString);
        if (thisCnt == maxval) {
            ans.append(inString);
        } else {
            if (thisCnt > maxval) maxval = thisCnt;
            for (int i = 0; i < maxval; i++) {
                if (!ans.toString().equals("")) ans.append(uvMarker);
                element = uCommons.FieldOf(inString, uvMarker, (i + 1));
                if (element.trim().equals("")) {
                    if (!NamedCommon.Sparse) {
                        ans.append(lastVal);
                    } else {
                        ans.append(" ");
                    }
                } else {
                    ans.append(element);
                }
                if (!element.trim().equals("")) lastVal = element;
            }
        }
        return ans.toString();
    }

    private static String GetValue(String inval) {
        int ans = 0;
        if (!inval.equals("")) {
            try {
                ans = Integer.valueOf(inval);
            } catch (NumberFormatException nfe) {
                String chk = uCommons.DynamicSubs(inval);
                if (chk.toUpperCase().equals("N")) {
                    return chk.toUpperCase();
                } else {
                    try {
                        ans = Integer.valueOf(chk);
                    } catch (NumberFormatException nfe1) {
                        if (!String.valueOf(ans).toUpperCase().equals("N")) {
                            NamedCommon.Zmessage = "Cannot understand \"" + inval + "\" in the csv.";
                            NamedCommon.ZERROR = true;
                            return inval;
                        }
                    }
                }
            }
        }
        return String.valueOf(ans);
    }

    private static void HandleMultiValues(UniDynArray thisLine, ArrayList REPL, ArrayList TMPL, int procCnt, String rp, String lineage, String value) {
        if (rp.trim().equals("")) return;
        rp = rp.trim();
        String tpl = String.valueOf(thisLine.extract(1, 9));
        if (procCnt > REPL.size()) {
            REPL.add(rp);
            TMPL.add(tpl);
        } else {
            REPL.add(procCnt - 1, rp);
            TMPL.add(procCnt - 1, tpl);
        }
        // indexOf will not work when there are similar replacement strings.
        int fnd = -1, lCnt = NamedCommon.SubsList.size();
        for (int f = 0; f < lCnt; f++) {
            if (NamedCommon.SubsList.get(f).equals(rp)) {
                fnd = f;
                break;
            }
        }
        if (fnd < 0) {
            NamedCommon.SubsList.add(procCnt - 1, rp);
            fnd = -1;
            for (int f = 0; f < lCnt; f++) {
                if (NamedCommon.SubsList.get(f).equals(rp)) {
                    fnd = f;
                    break;
                }
            }
            if (fnd < 0) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "Cannot add " + rp + " to SubsList";
                return;
            }
            NamedCommon.DataList.add(fnd, "");
            NamedCommon.AsocList.set(fnd, NamedCommon.isAssoc);
            NamedCommon.DataLineage.add(fnd, lineage);
            NamedCommon.TmplList.add(fnd, "");
        } else {
            NamedCommon.DataList.set(fnd, value);
            NamedCommon.AsocList.set(fnd, NamedCommon.isAssoc);
            NamedCommon.DataLineage.set(fnd, lineage);
            NamedCommon.TmplList.set(fnd, tpl);
        }
        NamedCommon.isAssoc = false;
        if (NamedCommon.Templates.indexOf(tpl) < 0) NamedCommon.Templates.add(tpl);
    }

    private static String GetTaggedRec(String thisTag) {
        String rec = "", tmp = "";
        for (int aa = 1; aa < 100; aa++) {
            tmp = String.valueOf(NamedCommon.fmvArray.extract(aa, 1));
            if (tmp.equals("")) break;
            if (tmp.equals(thisTag)) {
                for (int bb = 2; bb < 1000; bb++) {
                    tmp = String.valueOf(NamedCommon.fmvArray.extract(aa, bb));
                    if (tmp.equals("")) break;
                    rec = tmp;
                }
                if (rec.length() > 0) break;
            }
        }
        return rec;
    }

    private static String GetTaggedData(int ivf, String thisTag, UniDynArray thisLine) {
        String result = "";
        int maxInt = 999999999;
        String av = String.valueOf(thisLine.extract(1, 1));
        String mv = String.valueOf(thisLine.extract(1, 2));
        String sv = String.valueOf(thisLine.extract(1, 3));
        String cnv = String.valueOf(thisLine.extract(1, 4));
        String tag = String.valueOf(thisLine.extract(1, 7));
        if (av.equals("")) av = "0";
        if (mv.equals("")) mv = "0";
        if (sv.equals("")) sv = "0";
        String chk = av + mv + sv;
        int a, avf, avt;
        int m, mvf, mvt;
        int s, svf, svt;
        boolean notFound = true;

        if (chk.toLowerCase().contains("n")) {
            if (av.toLowerCase().contains("n")) {
                avf = AVF.get(ivf);
                avt = AVT.get(ivf);
            } else {
                avf = Integer.valueOf(av);
                avt = avf;
            }
            if (mv.toLowerCase().contains("n")) {
                mvf = MVF.get(ivf);
                mvt = MVT.get(ivf);
            } else {
                mvf = Integer.valueOf(mv);
                mvt = mvf;
            }
            if (sv.toLowerCase().contains("n")) {
                svf = SVF.get(ivf);
                svt = SVT.get(ivf);
            } else {
                svf = Integer.valueOf(sv);
                svt = svf;
            }
            String msg = " from <" + avf + "," + mvf + "," + svf + ">   to <" + avt + "," + mvt + "," + svt + ">";
            if (NamedCommon.debugging) uCommons.uSendMessage(uCommons.eMessage + msg);
        } else {
            avf = Integer.valueOf(av);
            avt = avf;
            mvf = Integer.valueOf(mv);
            mvt = mvf;
            svf = Integer.valueOf(sv);
            svt = svf;
        }
        int aval = avf;
        int mval = mvf;
        int sval = svf;
        String tmp;
        UniDynArray temp;

        // ---------------------------------------- //
        // ------ Look in JOIN or X Records ------- //
        // ---------------------------------------- //

        String useMark = NamedCommon.FMark;
        String tmpVal;
        boolean isFirst = true;
        for (int aa = 1; aa < 100; aa++) {
            tmp = String.valueOf(NamedCommon.fmvArray.extract(aa, 1));
            if (tmp.equals("")) break;
            if (tmp.equals(tag)) {
                notFound = false;
                for (int bb = 2; bb < 1000; bb++) {
                    tmp = String.valueOf(NamedCommon.fmvArray.extract(aa, bb));
                    if (tmp.equals("")) break;

                    if (!result.equals("")) result += useMark;
                    isFirst = true;

                    temp = uCommons.SQL2UVRec(tmp);
                    if (temp != null) {
                        if (av.toLowerCase().contains("n")) avt = temp.dcount();
                        for (a = avf; a <= avt; a++) {
                            if (aval == 0) {
                                tmpVal = tmp.split(NamedCommon.IMark)[0];
                                result += tmpVal;
                                aval++;
                                if (aval > avt) aval = avt;
                            } else {
                                if (mv.toLowerCase().contains("n")) mvt = temp.dcount(a);
                                for (m = mvf; m <= mvt; m++) {
                                    if (sv.toLowerCase().contains("n")) svt = temp.dcount(a, m);
                                    if (svt < svf) svt = svf;
                                    for (s = svf; s <= svt; s++) {
                                        tmpVal = String.valueOf(temp.extract(a, m, s));
                                        if (!isFirst) {
                                            result = result + NamedCommon.VMark + tmpVal;
                                        } else {
                                            result += tmpVal;
                                            isFirst = false;
                                        }
                                    }
                                    SVF.set(ivf, svt);
                                }
                                MVF.set(ivf, mvt);
                                aval++;
                                if (aval > avt) aval = avt;
                            }
                        }
                        AVF.set(ivf, avt);
                    }
                }
                return result;
            }
        }
        if (ivf == 0) {
            avf = 1;
            avt = maxInt;
            mvf = 1;
            mvt = maxInt;
            svf = 1;
            svt = maxInt;
        } else {
            if (!av.toLowerCase().contains("n")) {
                avf = Integer.valueOf(av);
                avt = avf;
            } else {
                avt = maxInt;
                while (av.toLowerCase().contains("n")) {
                    av = av.toLowerCase().replace("n", "");
                }
                if (av.equals("")) av = "1";
                avf = Integer.valueOf(av);
            }
            if (!mv.toLowerCase().contains("n")) {
                mvf = Integer.valueOf(mv);
                mvt = mvf;
            } else {
                mvt = maxInt;
                while (mv.toLowerCase().contains("n")) {
                    mv = mv.toLowerCase().replace("n", "");
                }
                if (mv.equals("")) mv = "1";
                mvf = Integer.valueOf(mv);
            }
            if (!sv.toLowerCase().contains("n")) {
                svf = Integer.valueOf(sv);
                svt = svf;
            } else {
                svt = maxInt;
                while (sv.toLowerCase().contains("n")) {
                    sv = sv.toLowerCase().replace("n", "");
                }
                if (sv.equals("")) sv = "1";
                svf = Integer.valueOf(sv);
            }
        }

        // ---------------------------------------- //
        // -------- Look =assoc variables  -------- //
        // ---------------------------------------- //

        if (notFound) {
            String datum = "", field = "";
            String attribute = "", multivalue = "", subvalue = "";
            int fnd = NamedCommon.SubsList.indexOf(tag);
            if (fnd > -1) {
                if (tagsMatched.indexOf(tag) < 0) MatchTagValues(tag);
                field = NamedCommon.DataList.get(fnd);
                if (avt == maxInt) avt = u2Commons.sDcount(field, "A");
                for (a = avf; a <= avt; a++) {
                    attribute = u2Commons.sExtract(field, a, 0, 0);
                    if (mvt == maxInt) mvt = u2Commons.sDcount(attribute, "M");
                    for (m = mvf; m <= mvt; m++) {
                        multivalue = u2Commons.sExtract(attribute, 1, m, 0);
                        if (svt == maxInt) svt = u2Commons.sDcount(multivalue, "S");
                        for (s = svf; s <= svt; s++) {
                            datum = u2Commons.sExtract(field, a, m, s);
                            result += datum;
                            if (s < svt) result += NamedCommon.SMark;
                        }
                        if (m < mvt) result += NamedCommon.VMark;
                    }
                    if (a < avt) result += NamedCommon.FMark;
                }
            }
        }
        return result;
    }

    private static void RemoveThisRunData() {
        // It has the LoadDte, dbhost, account, srcFile and the sqlTable
        // NamedCommon.SqlDatabase, NamedCommon.SqlSchema, NamedCommon.sqlTarget
        boolean inclHost = NamedCommon.rawCols.toLowerCase().contains("dbhost");
        boolean inclAcct = NamedCommon.rawCols.toLowerCase().contains("account");
        boolean inclFile = NamedCommon.rawCols.toLowerCase().contains("file");
        String dbTable = "", dropRows = "", selRows="";
        String sch = NamedCommon.SqlSchema;
        if (NamedCommon.uniBase) sch = "uni";
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                dbTable = "["+NamedCommon.SqlDatabase+"].["+sch+"].["+NamedCommon.sqlTarget+"]";
                break;
            default:
                return;
        }
        dropRows = "DELETE FROM " + dbTable + " where ";
        if (inclHost) dropRows += "DBHost = '" + NamedCommon.dbhost     + "' and ";
        if (inclAcct) dropRows += "Account = '" + NamedCommon.datAct    + "' and ";
        if (inclFile) dropRows += "srcFile = '" + NamedCommon.u2Source  + "' and ";
        dropRows += "LoadDte = '" + NamedCommon.BatchID + "'";
        dropRows = dropRows.replace("\n", "");

        selRows = dropRows.replace("DELETE FROM", "SELECT '1' FROM");

        DDL.clear();
        DDL.add(dropRows);
        SqlCommands.ExecuteSQL(DDL);
        int ctr;
        Statement stmt;
        ResultSet rs;
        try {
            stmt = NamedCommon.uCon.createStatement();
            ctr=99;
            while (ctr > 0) {
                uCommons.Sleep(2);
                rs = stmt.executeQuery(selRows);
                ctr=0;
                while (rs.next()) { ctr++; }
                rs.close();
                rs = null;
                uCommons.uSendMessage("   .) " + uCommons.oconvM(String.valueOf(ctr), "MD0~") + "  rows remaining");
                DDL.clear();
                DDL.add(dropRows);
                SqlCommands.ExecuteSQL(DDL);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        uCommons.Sleep(10);
    }

    private static void OutputDataRow(ArrayList<String> val, String ams) {
        int nbrFlds = val.size();
        StringBuilder sbTemp = new StringBuilder();
        for (int i=0; i < nbrFlds; i++) { sbTemp.append(val.get(i)); }
        if (sbTemp.toString().equals("") && !NamedCommon.emptyrows) return;
        sbTemp = null;
        // *********************************************************** //
        // ** val is an array of data to go into ONE row only !!    ** //
        // *********************************************************** //
        //
        // NB: a single quote is required by SQL Server to envelope fields.
        //     a double quote is required by ADMP at a field level.
        //     They serve very different purposed !!!
        //
        String stats,  dVal, uID, smd5;
        String admpStats="", admpStrings="", Dquote = "\"";
        String newAms = "",admpAMS="";
        if (NamedCommon.AMS.toLowerCase().equals("split")) {
            aAMS = uCommons.FieldOf(ams, "\\.", 1);
            mAMS = uCommons.FieldOf(ams, "\\.", 2);
            sAMS = uCommons.FieldOf(ams, "\\.", 3);
            if (aAMS.equals("0")) aAMS = "1";
            if (mAMS.equals("0")) mAMS = "1";
            if (sAMS.equals("0")) sAMS = "1";
            if (NamedCommon.ADMP) {
                admpAMS = Dquote + aAMS + Dquote + Komma +
                        Dquote + mAMS + Dquote + Komma +
                        Dquote + sAMS + Dquote;
                admpStrings = admpAMS;
            }
            newAms = quote + aAMS + quote + Komma +
                    quote + mAMS + quote + Komma +
                    quote + sAMS + quote;
            aAMS = "";
            mAMS = "";
            sAMS = "";
        } else {
            newAms = ams;
        }

        String valStrings = newAms;

        String dataSource="", admpDataSource="";
        boolean inclHost = NamedCommon.rawCols.toLowerCase().contains("dbhost");
        boolean inclAcct = NamedCommon.rawCols.toLowerCase().contains("account");
        boolean inclFile = NamedCommon.rawCols.toLowerCase().contains("file");
        if (!inclHost) {
            inclAcct=false;
            inclFile=false;
        }
        if (NamedCommon.ADMP) {
            admpDataSource = dataSource;
            if (inclHost) admpDataSource = Dquote +NamedCommon.dbhost + Dquote +Komma;
            if (inclAcct) admpDataSource+= Dquote +NamedCommon.datAct + Dquote +Komma;
        }
        if (inclHost) dataSource = quote + NamedCommon.dbhost + quote + Komma;      // from rFuel.properties
        if (inclAcct) dataSource+= quote + NamedCommon.datAct + quote + Komma;      // from message
        if (inclFile) dataSource+= quote + NamedCommon.u2Source + quote + Komma;    // from map

        String cols = NamedCommon.tblCols;
        cols = cols.replaceAll("\\-", "");
        cols = cols.replaceAll("\\.", "");
        cols = cols.replaceAll("\\*", "");
        String tmpCmd = sqlStmt + cols + ") VALUES (";

        for (int x = 0; x < nbrFlds; x++) {
            dVal = val.get(x);
            if (dVal.contains(quote)) dVal = dVal.replace(quote, quote + quote);
            if (!dVal.startsWith(quote)) dVal = quote + dVal + quote;
            valStrings += Komma + dVal;
            if (NamedCommon.ADMP) admpStrings += Komma + Dquote + dVal + Dquote;
        }
        admpStats = Dquote +NamedCommon.rowID + Dquote + Komma;
        stats = NamedCommon.rowID + Komma;
        uID = String.valueOf(NamedCommon.uID);

        if (!uID.startsWith(quote)) uID = quote + uID;
        if (!uID.endsWith(quote)) uID += quote;

        if (NamedCommon.isNRT) {
            smd5 = uCommons.GetMD5(uID + Komma + NamedCommon.BatchID + Komma + valStrings);
        } else {
            // excludes LoadDte (BachID) because it duplicates Transactions
            // BEWARE : in rFuel.properties - tblCols: LoadDte should have a "." NOT a "-"
            //          the "." takes it out of the primary index
            //          the "-" puts it into the primary index and causes dups based on LoadDte.
            smd5 = uCommons.GetMD5(uID + Komma + valStrings);
        }

        if (NamedCommon.ADMP) {
            admpStats += uID + Komma;
            admpStats += Dquote + NamedCommon.BatchID + Dquote + Komma;
            admpStats += Dquote + smd5 + Dquote + Komma;
            admpStats += Dquote + pNum + Dquote + Komma;
        }
        stats += uID + Komma;
        stats += quote + NamedCommon.BatchID + quote + Komma;
        stats += quote + smd5 + quote + Komma;
        stats += pNum + Komma;

        if (inclHost) {
            stats += dataSource;
            admpStats += admpDataSource;
        }

        pNum++;
        if (pNum > NamedCommon.MaxProc) pNum = 1;
        if ((stats + valStrings).length() > 8060) {
            HoldOverSized(stats + valStrings);
            bigRows++;
        } else {
            if (!NamedCommon.ZERROR) {
                if (!(stats + valStrings).equals(lastRow)) {
                    if (NamedCommon.BulkLoad && NamedCommon.DatSize > 0) {
                        valStrings = stats + valStrings;
                        if ((thisSize + valStrings.length()) > NamedCommon.DatSize) {
                            uCommons.SQLDump(sqlCmds);
                            thisSize=0;
                            sqlCmds.clear();
                        }
                        sqlCmds.add(valStrings);
                        thisSize += valStrings.length();
                        lastRow = valStrings;
                        // ---------------------------
                        datRows++;
                        rowCtr++;
                        NamedCommon.rowID++;
                    } else {
                        tmpCmd = tmpCmd + stats + valStrings + ")";
                        lastRow = stats + valStrings;
                        sqlCmds.add(tmpCmd);
                        // --------- ADMP ------------
                        if (NamedCommon.ADMP) {
                            tmpCmd = admpStats + admpStrings;
                            admpDat.add(tmpCmd);
                        }
                        datRows++;
                        rowCtr++;
                        NamedCommon.rowID++;
                        // ---------------------------
                        if (rowCtr >= cmdBlockSize && !NamedCommon.isNRT) {
                            uCommons.SQLDump(sqlCmds);
                            sqlCmds.clear();
                            if (NamedCommon.ZERROR) uCommons.uSendMessage("Data not dumped");
                            if (NamedCommon.ADMP) {
                                admpCnt = uCommons.ADMPdump(admpCnt, admpDat);
                                admpDat.clear();
                                admpCnt++;
                            }
                            rowCtr = 0;
                            GarbageCollector.CleanUp();
                        }
                    }
                }
            } else {
                uCommons.uSendMessage("Row not added to dat file");
            }
        }
    }

    private static void HoldOverSized(String bigrow) {
        try {
            File bigdata = new File(MessageProtocol.data + NamedCommon.u2FileRef + ".2big");
            if (!bigdata.exists()) bigdata.createNewFile();
            oversize = new FileWriter(bigdata.getAbsolutePath(), true);
            BufferedWriter bwBig = new BufferedWriter(oversize);
            bwBig.write(bigrow);
            bwBig.close();
            oversize.close();
        } catch (IOException e) {
            uCommons.uSendMessage("   .) --------------------------------------------------------------------");
            uCommons.uSendMessage("   .) Cannot create " + MessageProtocol.data + NamedCommon.u2FileRef + ".2big");
            uCommons.uSendMessage("      " + e.getMessage());
            uCommons.uSendMessage("   .) This row will NOT be added to SQL Table");
            uCommons.uSendMessage("   .) " + bigrow);
            uCommons.uSendMessage("   .) --------------------------------------------------------------------");
        }
    }

    private static String[] UnPackMultiValuedField(String inVar, UniDynArray inRec) {
        // inVar e.g.    Sn   S3-n  M1n   M6-n
        inVar = inVar.trim().replaceAll("\\-", "");
        String AMS = inVar.substring(0, 1);
        String val = inVar.substring(1, inVar.length()).toLowerCase();

        if (val.equals("n")) val = "1";
        val = uStrings.gReplace(val, "n", "");

        int FromPos = 1;
        try {
            FromPos = Integer.valueOf(val);
        } catch (NumberFormatException nfe) {
            FromPos = 1;
        }
        int ToPos = FromPos;
        switch (AMS) {
            case "A":
                if (inRec == null) {
                    ToPos = 1;
                } else {
                    ToPos = inRec.dcount();
                }
                break;
            case "M":
                if (inRec == null) {
                    ToPos = 1;
                } else {
                    ToPos = inRec.dcount(thisAv);
                }
                break;
            case "S":
                if (inRec == null) {
                    ToPos = 1;
                } else {
                    ToPos = inRec.dcount(thisAv, thisMv);
                }
                break;
        }
        return new String[]{String.valueOf(FromPos), String.valueOf(ToPos)};
    }

    private static int FlipValues(int nbrItems) {
        int DoneFlag = 0, thisDone, tst3;
        if (SvDone == 0) {
            /* ------------------ flip sv's ------------------ */
            for (int ii = 1; ii <= nbrItems; ii++) {
                tst3 = AllDone.get(3).get(ii);
                if (tst3 < 1) {
                    thisDone = DoneFlag;
                    DoneFlag = FlipThisValue(ii, DoneFlag, SVF, SVT);
                    if ((DoneFlag - thisDone) > 0) AllDone.get(3).set(ii, 1);
                } else {
                    DoneFlag++;
                }
            }
            if (DoneFlag == nbrItems) SvDone = 1;
        }
        DoneFlag = 0;
        if (SvDone == 1 && MvDone != 1) {
            MvDone = 0;
            /* ------------------ flip mv's ------------------ */
            for (int ii = 1; ii <= nbrItems; ii++) {
                tst3 = AllDone.get(2).get(ii);
                if (tst3 < 1) {
                    thisDone = DoneFlag;
                    DoneFlag = FlipThisValue(ii, DoneFlag, MVF, MVT);
                    if ((DoneFlag - thisDone) > 0) AllDone.get(2).set(ii, 1);
                } else {
                    DoneFlag++;
                }
            }
            if (DoneFlag == nbrItems) {
                MvDone = 1;
                SvDone = 0;
                for (int r = 0; r <= nbrItems; r++) { SvDone += AllDone.get(3).get(r); }
                if (SvDone == nbrItems) SvDone = 1;
            } else {
                for (int r = 0; r <= nbrItems; r++) { AllDone.get(3).set(r, AllDone.get(2).get(r)); }
            }
        }
        DoneFlag = 0;
        if (SvDone == 1 && MvDone == 1 && AvDone != 1) {
            /* ------------------ flip av's ------------------ */
            for (int ii = 1; ii <= nbrItems; ii++) {
                tst3 = AllDone.get(1).get(ii);
                if (tst3 < 1) {
                    thisDone = DoneFlag;
                    DoneFlag = FlipThisValue(ii, DoneFlag, AVF, AVT);
                    if ((DoneFlag - thisDone) > 0) AllDone.get(1).set(ii, 1);
                } else {
                    DoneFlag++;
                }
            }
            if (DoneFlag == nbrItems) {
                AvDone = 1;
                MvDone = 0;
                SvDone = 0;
                for (int r = 0; r <= nbrItems; r++) { SvDone += AllDone.get(3).get(r);MvDone += AllDone.get(2).get(r); }
                if (SvDone == nbrItems) SvDone = 1;
                if (MvDone == nbrItems) MvDone = 1;
            } else {
                for (int r = 0; r <= nbrItems; r++) { AllDone.get(2).set(r, AllDone.get(1).get(r)); }
            }
        }
        return (AvDone + MvDone + SvDone);
    }

    private static int FlipThisValue(int ii, int doneFlag, ArrayList<Integer> inFrom, ArrayList<Integer> inTo) {
        if ((inFrom.get(ii) + 1) > inTo.get(ii)) {
            inFrom.set(ii, inTo.get(ii));
            doneFlag++;
        } else {
            int tst3 = inFrom.get(ii);
            tst3++;
            if (tst3 > inTo.get(ii)) tst3 = inTo.get(ii);
            inFrom.set(ii, tst3);
        }
        return doneFlag;
    }

    public static String ConvRawData(String cnv, String Base) {
        String result = "";
        String cc;
        int ccPart = 1;
        // ------------------------------------------------------
        int aDC = u2Commons.sDcount(Base, "A"), mDC = 0, sDC = 0;
        int iDC = u2Commons.sDcount(Base, "I");
        if (iDC > aDC) aDC = iDC;

        String line, useMark, ccChk;
        boolean skipLine = false;

        ccPart = 1;
        cc = uCommons.FieldOf(cnv, "\\}", ccPart);
        cc = cc.replaceAll("~", ",");
        ccChk = uCommons.FieldOf(cc, "\\|", 1).toLowerCase();
        while (cc.length() >= 1) {
            switch (ccChk) {
                case "=drange":
                    if (!uCommons.inDrange(cc, Base)) return skipStr;
                    break;
                case "=assoc":
                    result = TransformData(cc, Base);
                    transformed = true;
                    return result;
                case "=cat":
                    result = TransformData(cc, Base);
                    transformed = true;
                    return result;
            }
            ccPart++;
            cc = uCommons.FieldOf(cnv, "\\}", ccPart);
        }

        transformed = false;
        for (int a = 1; a <= aDC; a++) {
            line = u2Commons.sExtract(Base, a, 0, 0);
            mDC = u2Commons.sDcount(line, "M");
            useMark = NamedCommon.FMark;
            for (int m = 1; m <= mDC; m++) {
                line = u2Commons.sExtract(Base, a, m, 0);
                sDC = u2Commons.sDcount(line, "S");
                useMark = NamedCommon.VMark;
                for (int s = 1; s <= sDC; s++) {
                    line = u2Commons.sExtract(Base, a, m, s);
                    ccPart = 1;
                    cc = uCommons.FieldOf(cnv, "\\}", ccPart);
                    cc = cc.replaceAll("~", ",");
                    ccChk = uCommons.FieldOf(cc, "\\|", 1).toLowerCase();
                    while (cc.length() >= 1) {
                        switch (ccChk) {
                            case "=assoc":
                                break;
                            case "=cat":
                                break;
                            default:
                                line = TransformData(cc, line);
                                transformed = true;
                                break;
                        }
                        ccPart++;
                        cc = uCommons.FieldOf(cnv, "\\}", ccPart);
                    }
                    if (s > 1) useMark = NamedCommon.SMark;
                    if (!line.equals("")) result += useMark + line;
                }
            }
        }
        while (result.startsWith(NamedCommon.FMark))
            result = result.substring(NamedCommon.FMark.length(), result.length());
        while (result.startsWith(NamedCommon.VMark))
            result = result.substring(NamedCommon.VMark.length(), result.length());
        while (result.startsWith(NamedCommon.SMark))
            result = result.substring(NamedCommon.SMark.length(), result.length());
        return result;
    }

    private static String TransformData(String cc, String Base) {
        String chk = cc.substring(0, 1).toUpperCase();
        String result = Base;
        String[] BaseArr = result.split(NamedCommon.FMark);
        int nbrLoops = BaseArr.length;
        String useBase = "", thisCmd = "", sID, sRow;
        UniString rID;
        UniString xRow;
        switch (chk) {
            case "=":
                result = uCommons.UplFunction(cc, Base);
                break;
            case "[":
                result = uCommons.StringExtract(cc, Base);
                break;
            case "D":
                if (Base.startsWith("F<")) {
                    result = Base;
                } else {
                    String ans = "";
                    if (!Base.contains(NamedCommon.VMark)) {
                        ans = uCommons.oconvD(Base, cc);
                    } else {
                        String svar = Base;
                        String[] stmp = svar.split(NamedCommon.VMark);
                        int nn = stmp.length;
                        for (int n = 0; n < nn; n++) {
                            String thisPart = stmp[n];
                            if (n > 0) ans += NamedCommon.VMark;
                            ans += uCommons.oconvD(thisPart, cc);
                        }
                        while (ans.substring(0, 1).equals(NamedCommon.VMark) && ans.length() > 0) {
                            ans = ans.substring(1, ans.length());
                        }
                    }
                    result = ans;
                }
                break;
            case "M":
                if (Base.startsWith("F<")) {
                    result = Base;
                } else {
                    String ans = "";
                    cc = cc.toUpperCase();
                    if (!Base.contains(NamedCommon.VMark)) {
                        if (cc.startsWith("MD") || cc.startsWith("MT")) {
                            ans = uCommons.oconvM(Base, cc);
                        } else {
                            ans = uCommons.oconvT(Base, cc);
                        }
                    } else {
                        String[] stmp = Base.split(NamedCommon.VMark);
                        int nn = stmp.length;
                        for (int n = 0; n < nn; n++) {
                            String thisPart = stmp[n];
                            if (n > 0) ans += NamedCommon.VMark;
                            ans += uCommons.oconvM(thisPart, cc);
                        }
                        while (ans.substring(0, 1).equals(NamedCommon.VMark) && ans.length() > 0) {
                            ans = ans.substring(1, ans.length());
                        }
                    }
                    result = ans;
                }
                break;
            case "!":
                result = uCommons.StringToMask(cc, Base);
                break;
            case "F":
                if (Base.startsWith("F<")) {
                    result = Base;
                } else {
                    String useMark = "";
                    if (Base.contains(NamedCommon.IMark)) useMark = NamedCommon.IMark;
                    if (Base.contains(NamedCommon.FMark)) useMark = NamedCommon.FMark;
                    if (Base.contains(NamedCommon.VMark)) useMark = NamedCommon.VMark;
                    if (Base.contains(NamedCommon.SMark)) useMark = NamedCommon.SMark;
                    if (!useMark.equals("")) {
                        String ans = "";
                        String[] parts = Base.split(useMark);
                        int nbrParts = parts.length;
                        for (int pp = 0; pp < nbrParts; pp++) {
                            String thispart = parts[pp];
                            if (ans.equals("")) {
                                ans = uCommons.StringFieldOf(cc, thispart);
                            } else {
                                ans += useMark + uCommons.StringFieldOf(cc, thispart);
                            }
                        }
                        result = ans;
                        if (result.startsWith(useMark))
                            result = result.substring((useMark.length() + 1), result.length());
                    } else {
                        result = uCommons.StringFieldOf(cc, Base);
                    }
                }
                break;
            case "T":
                result = TransformString(cc, Base);
                break;
            case "X":
                String[] tmpcmd = cc.split("\\|");
                uCommons.uSendMessage("    > Execute X-late, loading data into " + tmpcmd[2]);
                uCommons.uSendMessage("      >> x-late from " + tmpcmd[1]);
                int ffnd = NamedCommon.fTagNames.indexOf(tmpcmd[2]);    //  the variable name
                if (ffnd == -1) {
                    if (u2Commons.uOpenFile(tmpcmd[1], "2")) {  /* e.g. CLIENT          */
                        NamedCommon.fHandles.add(NamedCommon.U2File);   /* handle to CLIENT     */
                        NamedCommon.fTagNames.add(tmpcmd[2]);           /* e.g. $CLIENT         */
                        ffnd = NamedCommon.fTagNames.indexOf(tmpcmd[2]);
                    } else {
                        uCommons.uSendMessage("ERROR with file open of " + tmpcmd[1]);
                        break;
                    }
                } else {
                    NamedCommon.U2File = NamedCommon.fHandles.get(ffnd);
                }

                BB = 1;
                if (nbrLoops == 1) BaseArr = Base.split(NamedCommon.FMark);
                for (int bs = 0; bs < nbrLoops; bs++) {
                    useBase = BaseArr[bs];
                    rID = new UniString(useBase);
                    if (NamedCommon.protocol.equals("real")) {
                        sRow = u2Commons.rRead(tmpcmd[1], useBase, "", "", "");
                        xRow = uCommons.SQL2UVRec(sRow);
                        sRow = null;
                        uCommons.eMessage = "      >> Record [" + useBase + "] has been loaded.";
                    } else {
                        while (true) {
                            try {
                                xRow = NamedCommon.U2File.read(rID);
                                uCommons.eMessage = "      >> Record [" + useBase + "] has been loaded.";
                                break;
                            } catch (UniFileException e) {
                                if (!u2Commons.TestAlive()) {
                                    SourceDB.ReconnectService();
                                } else {
                                    uCommons.eMessage = "          Record [" + useBase + "] NOT found on " + tmpcmd[1];
                                    xRow = null;
                                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                                    break;
                                }
                            }
                        }
                    }
                    uCommons.uSendMessage(uCommons.eMessage);
                    AA = ffnd;
                    BB++;
                    NamedCommon.fmvArray.replace(AA, BB, (uCommons.UV2SQLRec(rID, xRow)));
                    rID = null;    // keeps jvm heap space to a minimum
                    xRow = null;    // same
                }
                break;
            case "J":
                tmpcmd = cc.split("\\|");
                String joinFile = tmpcmd[1];
                String joinCmd = tmpcmd[2];
                String joinTag = tmpcmd[3];
                uCommons.uSendMessage("    > Execute JOIN, loading data into " + joinTag);
                if (nbrLoops == 1) {
                    BaseArr = Base.split(NamedCommon.FMark);
                    nbrLoops = BaseArr.length;
                }
                for (int bs = 0; bs < nbrLoops; bs++) {
                    // when we're trying to join by item from the message
                    useBase = BaseArr[bs];
                    while (joinCmd.contains("$$")) { joinCmd = joinCmd.replace("$$", useBase); }
                    uCommons.uSendMessage("      >> " + joinCmd);
                    thisCmd = joinCmd;
                    ffnd = NamedCommon.fTagNames.indexOf(joinTag);
                    if (ffnd == -1) {
                        boolean fOpen = false;
                        switch (NamedCommon.protocol) {
                            case "u2cs":
                                NamedCommon.U2File = u2Commons.uOpen(joinFile);
                                fOpen = (NamedCommon.U2File != null);
                                break;
                            case "real":
                                fOpen = (!rCommons.ReadAnItem("MD", joinFile, "", "", "").equals(""));
                            case "u2mnt":
                                fOpen = (!u2Commons.ReadAnItem("VOC", joinFile, "", "", "").equals(""));
                                break;
                            case "u2sockets":
                                fOpen = (!u2Commons.ReadAnItem("VOC", joinFile, "", "", "").equals(""));
                                break;
                            case "rmi.u2cs":
                                System.out.println("rmi:  is being developed - not ready yet");
                            default:
                                fOpen = u2Commons.uOpenFile(joinFile, "2");
                        }

                        if (fOpen) {
                            useBase = NamedCommon.U2File.getFileName();
                            NamedCommon.fHandles.add(NamedCommon.U2File);   /* handle to uv file  */
                            NamedCommon.fTagNames.add(joinTag);             /* e.g. $records$     */
                            ffnd = NamedCommon.fTagNames.indexOf(joinTag);
                            joinCmd = joinCmd.replace(joinFile, useBase);
                            thisCmd = joinCmd;
                            joinFile = useBase;
                        } else {
                            uCommons.uSendMessage("ERROR with file open of " + tmpcmd[1]);
                            break;
                        }
                    } else {
                        if (!NamedCommon.protocol.equals("real")) {
                            String srcFile = NamedCommon.U2File.getFileName();
                            if (!srcFile.equals(NamedCommon.fHandles.get(ffnd).getFileName())) {
                                while (true) {
                                    try {
                                        NamedCommon.U2File.close();
                                        NamedCommon.U2File = NamedCommon.fHandles.get(ffnd);
                                        break;
                                    } catch (UniFileException e) {
                                        if (!u2Commons.TestAlive()) {
                                            SourceDB.ReconnectService();
                                        } else {
                                            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                                            // not an error - it may not have been set
                                            break;
                                        }
                                    }
                                }
                            } else {
                                thisCmd = thisCmd.replace(joinFile, srcFile);
                                joinFile = NamedCommon.U2File.getFileName();
                            }
                        }
                    }
                    if (NamedCommon.debugging) uCommons.uSendMessage("      " + thisCmd);
                    AA = ffnd;
                    BB = 1;
                    boolean uniFailed = false;
                    switch (NamedCommon.protocol) {
                        case "u2cs":
                            uniFailed = u2Commons.u2cs_SelectAndRead(AA, BB, joinFile, thisCmd);
                            break;
                        case "real":
                            uniFailed = u2Commons.real_SelectAndRead(AA, BB, joinFile, thisCmd);
                            break;
                        case "u2mnt":
                            uniFailed = u2Commons.u2mnt_SelectAndRead(AA, BB, joinFile, thisCmd);
                            break;
                        case "u2sockets":
                            uniFailed = u2Commons.u2sock_SelectAndRead(AA, BB, joinFile, thisCmd);
                            break;
                        case "rmi.u2cs":
                            System.out.println("rmi:  is being developed - not ready yet");
                        default:
                            uniFailed = u2Commons.u2cs_SelectAndRead(AA, BB, joinFile, thisCmd);
                    }

                    // populate NamedCommon.fmvArray here !!

                    if (uniFailed) {
                        uCommons.uSendMessage("================================================");
                        uCommons.uSendMessage("***   Cannot execute command in ProcessRow() ");
                        uCommons.uSendMessage("***   Command is " + thisCmd);
                        uCommons.uSendMessage("***   1.) Does the file exist (spelling / Q-File) in " + NamedCommon.datAct + " account");
                        uCommons.uSendMessage("***   2.) Does the 'EVAL' work (quotes)");
                        uCommons.uSendMessage("***   TEST this command at TCL before restarting !! ");
                        uCommons.uSendMessage("***   ... no action taken ...");
                        uCommons.uSendMessage("================================================");
                        result = Base;
                    }
                }
                break;
            case "S":
                // Call a SUBR as a conversion code ----------------
                // e.g. S|SRTEST
                //      will CALL SRTEST (Answer, Base)
                //      and put Answer in the TagName
                // -------------------------------------------------
                tmpcmd = cc.split("\\|");
                String atSubr = tmpcmd[1];
                ffnd = NamedCommon.fTagNames.indexOf(tmpcmd[2]);    //  the variable name
                if (ffnd == -1) {
                    NamedCommon.fTagNames.add(tmpcmd[2]);               // e.g. $subrVar$
                    ffnd = NamedCommon.fTagNames.indexOf(tmpcmd[2]);
                } else {
                    uCommons.uSendMessage("ERROR: duplicate defintion of " + tmpcmd[2]);
                    break;
                }
                String response = "";
                String[] callArgs = new String[]{response, Base};
                uCommons.uSendMessage("   .) Call subroutine " + atSubr);
                String[] retnArgs = u2Commons.uniCallSub(atSubr, callArgs);  // send Base
                response = retnArgs[0];
                if (response.startsWith("ERR") || NamedCommon.ZERROR) {
                    // handle the error
                    uCommons.uSendMessage("ERROR: " + response);
                    break;
                } else {
                    AA = ffnd;
                    BB = 1;
                    NamedCommon.fmvArray.replace(AA, BB, tmpcmd[2]);
                    BB++;
                    NamedCommon.fmvArray.replace(AA, BB, response);
                    uCommons.uSendMessage("   .) Loaded " + tmpcmd[2]);
                }
                break;
            default:
                result = Base;
        }
        return result;
    }

    public static String TransformString(String tbl, String base) {
        if (DDL == null) DDL = new ArrayList();
        String ans = "", qt = "'";
        int genLength = 10;
        String key = tbl.split("\\-")[1];     // e.g. T-TC = TC {tran code}
        key+= base;                                 // e.g. TC50
        String tQry = "SELECT * FROM [upl].[TBL] where TblKey = '" + key + "'";
        ResultSet rs;
        NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
        if (NamedCommon.uCon == null) {
            NamedCommon.ZERROR = true;
            uCommons.uSendMessage("ERROR: jdbc connection failure.");
            return base;
        }
        Statement stmt = null;
        String oldV="", newV="";
        try {
            stmt = NamedCommon.uCon.createStatement();
            rs = stmt.executeQuery(tQry);
            while (rs.next()) {
                oldV = rs.getString("Old");
                newV = rs.getString("New");
            }
            rs = null;
            stmt = null;
            while (newV.equals("")) {
                // If this is the first time we're seeing the TBL item, generate a random string
                if (base.length() > genLength) genLength = base.length();
                newV = uCommons.GenerateString(genLength);
                tQry = "SELECT * FROM [upl].[TBL] where New = '" + newV + "'";
                stmt = NamedCommon.uCon.createStatement();
                rs = stmt.executeQuery(tQry);
                oldV = "";
                while(rs.next()) {
                    oldV = rs.getString("Old");
                    break;
                }
                if (oldV.equals("")) {
                    // the random string is unique - update it in TBL
                    tQry = "INSERT INTO [upl].[TBL] (TblKey, Old, New) VALUES ("+qt+key+qt+","+qt+base+qt+","+qt+newV+qt+")";
                    DDL.add(tQry);
                    SqlCommands.ExecuteSQL(DDL);
                    DDL.clear();
                } else {
                    newV = "";
                }
            }
            ans = newV;
        } catch (SQLException e) {
            NamedCommon.ZERROR = true;
            uCommons.uSendMessage(tQry);
            uCommons.uSendMessage("ERROR: " + e.getMessage());
            return base;
        }
        rs = null;
        stmt = null;
        return ans;
    }

}
