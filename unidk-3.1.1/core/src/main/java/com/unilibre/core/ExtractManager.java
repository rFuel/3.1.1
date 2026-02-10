package com.unilibre.core;

/**
 * Copyright UniLibre on 2015. ALL RIGHTS RESERVED
 */

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniFileException;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
//import org.springframework.util.StringUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.unilibre.commons.u2Commons.*;
import static java.lang.Math.abs;

public class ExtractManager {

    private static BufferedReader bReader = null;
    private static String pass = "<<PASS>>", fail = "<<FAIL>>";
    public static String dbFocus, thisReply = "", encSeed = "", fetch = "fetch", correl="";
    public static String LoadDte = NamedCommon.BatchID;
    public static String Komma = NamedCommon.Komma;
    public static String quote = NamedCommon.Quote;
    public static UniFile IOFile;
    public static UniDynArray InRow;
    public static List<String> DDL = new ArrayList<>();
    public static int Ctr = 0, procNum = 0, bestLength = 2500000, thisSize = 0;
    public static int TotalInserts = 0;
    private static int maxWait = 2;
    public static int restartCnt = 0;
    private static boolean Resilience = false;
    private static boolean IamRestarting = false;
    private static long sent = 0, got = 0, laps = 0, nanoSecs = 1000000000;

    public static String start(String message) {
        //uCommons.uSendMessage("ExtractManager.start()");
        String reply = "";
        if (NamedCommon.isWhse) {
            if (SqlCommands.ConnectSQL()) {
                uCommons.uSendMessage("TargetDB: Connected");
                String HoldSchema = NamedCommon.SqlSchema;
                NamedCommon.SqlSchema = NamedCommon.rawSC;
                reply = PullOutAllData(message);
                NamedCommon.SqlSchema = HoldSchema;
            }
        } else {
            reply = PullOutAllData(message);
        }
        return reply;
    }

    public static String PullOutAllData(String thisInstr) {
        //uCommons.uSendMessage("ExtractManager.PullOutAllData()");
        boolean proceed = false;
        String reply = "";
        if (!NamedCommon.IsAvailableU2) {
            NamedCommon.ProcSuccess = SourceDB.ConnectSourceDB();
            if (NamedCommon.ProcSuccess.toUpperCase().contains("PASS")) {
                NamedCommon.IsAvailableU2 = true;
                boolean shutdown = false;
                proceed = (u2Commons.CheckU2Controls().toUpperCase().contains("PASS"));
            } else {
                reply = "U2 host is unavailable";
                uCommons.uSendMessage(reply);
                reply = "<<FAIL>> " + reply;
            }

        } else {
            proceed = true;
        }
        if (proceed) {
            reply = ProcessInstruction(thisInstr);
        } else {
            reply = NamedCommon.ProcSuccess;
        }
        return reply;
    }

    private static String ProcessInstruction(String instruction) {
        String reply = ProcessMap(NamedCommon.xMap);
        return reply;
    }

    public static String ProcessMap(String map) {
        String reply = "";
        String fqn = NamedCommon.BaseCamp + "/maps/" + NamedCommon.xMap;
        reply = GetSourceData(fqn);
        return reply;
    }

    public static String GetSourceData(String map) {
        String reply = pass;
        String srcfile = NamedCommon.u2Source;
        boolean fOpen, showOutput = true;
        if (NamedCommon.isWhse && !NamedCommon.isNRT) {
            if (NamedCommon.task.equals("025")) {
                fOpen = true;
            } else {
                PrepareSQLDumper();
                uCommons.uSendMessage("... Open " + srcfile + "  (1)");
                fOpen = u2Commons.uOpenFile(srcfile, "1"); // "1" = open all the files
                if (NamedCommon.ZERROR) reply = NamedCommon.Zmessage;
            }
        } else {
            switch (NamedCommon.protocol) {
                case "u2cs":
                    if (NamedCommon.isKafka) {
                        fOpen = true;
                    } else {
                        fOpen = uOpenFile(srcfile, "2");
                        if (fOpen) {
                            IOFile = NamedCommon.U2File;
                        } else {
                            reply = "<<FAIL>> " + NamedCommon.Zmessage;
                            return reply;
                        }
                    }
                    break;
                case "real":
                    fOpen = (!rCommons.ReadAnItem("MD", srcfile, "", "", "").equals(""));
                    break;
                case "u2mnt":
                    fOpen = (!u2Commons.ReadAnItem("VOC", srcfile, "", "", "").equals(""));
                    break;
                case "u2sockets":
                    fOpen = (!u2Commons.ReadAnItem("VOC", srcfile, "", "", "").equals(""));
                    break;
                case "rmi.u2cs":
                    System.out.println("rmi:  is being developed - not ready yet");
                default:
                    fOpen = u2Commons.uOpenFile(srcfile, "2");
                    if (NamedCommon.ZERROR) reply = NamedCommon.Zmessage;
                    IOFile = NamedCommon.U2File;
            }
        }
        if (fOpen) {
            DDL.clear();
            if (NamedCommon.task.equals("025")) {
                uCommons.uSendMessage("   .) Delta record has been provided.");
                String[] payload = new String[4];
                payload = APImsg.APIget("payload").split(NamedCommon.IMark);
                for (int i = 0; i < 3; i++) {
                    if (payload[i] == null) payload[i] = "";
                }
                // String account = payload[0];
                // String    file = payload[1];
                String item = payload[2];
                String deltaRec = payload[3];
                NamedCommon.dynRec = uCommons.SQL2UVRec(deltaRec);
                NamedCommon.uID = new UniString(item);
                InRow = NamedCommon.dynRec;
                deltaRec = RestExtract(map);
                // RestExtract() creates data in NamedCommon.DataList
            } else {
                if (NamedCommon.isWhse) {
                    boolean okay = U2Getter(NamedCommon.uTake.getFileName());
                    while (!okay) {
                        if (restartCnt < 2) {
                            System.out.println(" ");
                            uCommons.uSendMessage("Restarting the message.");
                            restartCnt++;
                            okay = U2Getter(NamedCommon.uTake.getFileName());
                        } else {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Unrecoverable error: clear message queues, restart rFuel and try again.";
                            return fail;
                        }
                    }
                } else {
                    uCommons.uSendMessage("   .) Read item [" + NamedCommon.item + "] from " + NamedCommon.U2File.getFileName());
                    if (NamedCommon.isWhse) {
                        uCommons.uSendMessage("   .) ERROR: Single record extract in uWhse()");
                        reply = "<<FAIL>> Single record extract in uWhse()";
                    } else {
                        if (NamedCommon.isDocker) {
                            NamedCommon.dynRec = new UniDynArray(u2Commons.uRead(IOFile, NamedCommon.item));
                            NamedCommon.uID = new UniString(NamedCommon.item);
                            InRow = NamedCommon.dynRec;
                        } else {
                            String strRow = u2Commons.ReadAnItem(srcfile, NamedCommon.item, "", "", "");
                            switch (NamedCommon.protocol) {
                                case "u2cs":
                                    UniString ustr = new UniString(NamedCommon.item);
                                    NamedCommon.uID = ustr;
                                    ustr = null;        // heap space friendly !
                                    InRow = NamedCommon.dynRec;
                                    break;
                                case "real":
                                    InRow = uCommons.SQL2UVRec(strRow);
                                    NamedCommon.uID = new UniString(NamedCommon.item);
                                    break;
                                case "u2mnt":
                                    InRow = uCommons.SQL2UVRec(strRow);
                                    break;
                                case "u2sockets":
                                    InRow = uCommons.SQL2UVRec(strRow);
                                    break;
                                case "rmi.u2cs":
                                    System.out.println("rmi:  is being developed - not ready yet");
                                    InRow = null;
                                default:
                                    InRow = NamedCommon.dynRec;
                            }
                        }
                        if (InRow.dcount() < 1) {
                            reply = "<<FAIL>> [" + NamedCommon.item + "] does not exist on " + NamedCommon.U2File.getFileName();
                        } else {
                            // --------------------------------------------------------------------//
                            //  Fills the NamedCommon.DataList array which is used in DataConverter.
                            //  tmpString will have a PASS or FAIL marker, nothing more.
                            String tmpString = RestExtract(map);
                            // -------------------------------------------------------------------//
                            if (NamedCommon.ZERROR) {
                                reply = "<<FAIL>> ";
                                if (NamedCommon.Zmessage.equals("")) {
                                    reply += "See RunERRORS queue for details.";
                                } else {
                                    reply += NamedCommon.Zmessage;
                                }
                            } else {
                                reply = pass + "Reply returned.";
                            }
                        }
                    }
                    Ctr = 0;
                    thisSize = 0;
                    DDL.clear();
                }
                if (!DDL.isEmpty()) {
                    uCommons.SQLDump(DDL);
                    if (NamedCommon.ZERROR) reply = "<<FAIL>> See RunERRORS queue for details.";
                    Ctr = 0;
                    thisSize = 0;
                    DDL.clear();
                }
            }
        } else {
            uCommons.uSendMessage(srcfile + " open errors ... no action");
            reply = "<<FAIL>> Database file open errors - " + srcfile;
        }
        return reply;
    }

    public static String FetchSourceData(String takeFile) {
        IamRestarting = false;
        boolean uniError = U2Getter(takeFile);
        if (!DDL.isEmpty()) uCommons.SQLDump(DDL);
        DDL.clear();
        if (!uniError) return "<<PASS>>";           // returns uniFail : false = everyting okay
        if (IamRestarting) return "RESTART";
        return "<<FAIL>>";
    }

    public static boolean U2Getter(String takeFile) {
        boolean show = true;
        uCommons.uSendMessage("   .) Preparing to extract raw data");
        boolean rfEnc = false, firstprt = false;
        if (NamedCommon.encRaw) {
            if (!NamedCommon.AES) {
                uCommons.uSendMessage("   .) Find or create the encryption seed.");
                rfEnc = true;
                if (NamedCommon.isPrt) {
                    // -------------------------------------------------------------
                    //  ONLY the first prt file should create the encSeed
                    //  otherwise there are too many of them !!
                    // -------------------------------------------------------------
                    if (uCommons.APIGetter("firstpart").toLowerCase().equals("true")) {
                        uCommons.uSendMessage("   .) This is the first part-file. Will create the seed");
                        if (!NamedCommon.cSeed.equals("")) {
                            encSeed = NamedCommon.cSeed;
                        } else {
                            encSeed = GetEncSeed();
                        }
                    } else {
                        uCommons.uSendMessage("   .) waiting on the first part-file ...");
                        // wait for 10 minutes for the first part file
                        if (!NamedCommon.cSeed.equals("")) {
                            encSeed = NamedCommon.cSeed;
                        } else {
                            for (int i = 1; i < 600; i++) {
                                encSeed = GetEncSeed();
                                if (!encSeed.equals("")) break;
                                uCommons.Sleep(5);
                                // wait for firtpart to create it.
                            }
                            if (encSeed.equals("")) uCommons.uSendMessage("   .) not found - will create it !");
                            // if not found after waiting - okay to create it
                            if (!NamedCommon.cSeed.equals("")) {
                                encSeed = GetEncSeed();
                            }
                        }
                    }
                } else {
                    if (!NamedCommon.cSeed.equals("")) {
                        encSeed = NamedCommon.cSeed;
                    } else {
                        encSeed = GetEncSeed();
                    }
                }
                // Catch logic error
                if (encSeed.equals("")) {
                    uCommons.uSendMessage("   .) ERROR: did not create the encryption seed");
                    NamedCommon.ZERROR = true;
                }
            } else {
                // AES will encrypt accord to secret and salt - NOT an encSeed.
                encSeed = "";
                rfEnc = false;
            }
        }

        if (rfEnc) {
            String effort = "Found";
            if (encSeed.equals("") && !NamedCommon.AES) {
                CreateEncSeed();
                effort = "Created";
                String encMD5 = uCommons.GetMD5(NamedCommon.BatchID + "," + encSeed);

                String extraDat = "";
                boolean inclHost = NamedCommon.rawCols.toLowerCase().contains("dbhost");
                boolean inclAcct = NamedCommon.rawCols.toLowerCase().contains("account");
                boolean inclFile = NamedCommon.rawCols.toLowerCase().contains("file");
                if (!inclHost) {
                    inclAcct = false;
                    inclFile = false;
                }
                if (inclHost) extraDat = "'" + NamedCommon.dbhost + "','";
                if (inclAcct) extraDat += NamedCommon.datAct + "',";
                if (inclFile) extraDat += NamedCommon.u2Source + "',";

                // changed to not use NamedCommon.BatchID because it was being created by ALL processes - same datetime !!
                String encDate = uCommons.GetLocaltimeStamp();
                encDate = uCommons.FieldOf(encDate, "\\.", 1);
                encDate = encDate.replace(" ", "");
                encDate = encDate.replaceAll("\\:", "");
                encDate = encDate.replaceAll("\\-", "");
//                String encDate = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());

                String insRec = "'" + encMD5 + "'"
                        + "," + "'" + "encSeed" + "'"
                        + "," + "'" + encDate + "'"
                        + "," + 0 + ","
                        + extraDat
                        + "'" + encSeed + "'";

                String encRow = "INSERT INTO " + dbFocus;
                String cols = NamedCommon.rawCols;
                cols = cols.replaceAll("\\-", "");
                cols = cols.replaceAll("\\.", "");
                cols = cols.replaceAll("\\*", "");
                encRow += " (" + cols + ") VALUES (" + insRec + ")";
                DDL.clear();
                DDL.add(encRow);
                SqlCommands.ExecuteSQL(DDL);
                encMD5 = "";
                insRec = "";
                encRow = "";
                extraDat = "";
            }
            DDL.clear();
            if (!NamedCommon.AES) {
                uCommons.uSendMessage("********************************************************");
                uCommons.uSendMessage("   .) " + effort + " data encryption seed ");
                uCommons.uSendMessage("   .)          in: " + dbFocus);
                uCommons.uSendMessage("   .)         for: " + NamedCommon.BatchID);
                uCommons.uSendMessage("   .)       value: " + encSeed);
                uCommons.uSendMessage("********************************************************");
            }
        }
        int rnd = ThreadLocalRandom.current().nextInt(111, 999);
        NamedCommon.serial = String.valueOf(rnd);
        DDL = new ArrayList<String>();
        String mode = NamedCommon.RunType;
        String sourceFile = takeFile;
        if (mode.equals("REFRESH") || mode.equals("FULL")) {
            //----------------------------------------//
            // Check if the file is in uMASTER        //
            //      add it if not - [datact].[source] //
            //----------------------------------------//
            String cmd = "", rtnString = "";
            boolean clrFiles = true;
            if (NamedCommon.isPrt) {
                clrFiles = false;
                // using firstpart ASSUMES the first part file is processed first - dangerous !!
//                if (firstprt) clrFiles = true;
            }
            if (clrFiles) {
                // open and clear the .TAKE and the .LOADED files
                switch (NamedCommon.protocol) {
                    case "u2cs":
                        while (true) {
                            try {
                                NamedCommon.uLoaded.clearFile();
                                uCommons.uSendMessage("      > Cleared " + NamedCommon.uLoaded.getFileName());
                                break;
                            } catch (UniFileException e) {
                                if (!u2Commons.TestAlive()) {
                                    SourceDB.ReconnectService();
                                } else {
                                    uCommons.uSendMessage("CANNOT clearfile() " + NamedCommon.uLoaded.getFileName());
                                    if (!u2Commons.ClearFile(NamedCommon.uLoaded)) {
                                        uCommons.uSendMessage("ERROR when clearing data file");
                                    }
                                    break;
                                }
                            }
                        }
                        while (true) {
                            try {
                                NamedCommon.uTake.clearFile();
                                uCommons.uSendMessage("      > Cleared " + NamedCommon.uTake.getFileName());
                                break;
                            } catch (UniFileException e) {
                                if (!u2Commons.TestAlive()) {
                                    SourceDB.ReconnectService();
                                } else {
                                    uCommons.uSendMessage("CANNOT clearfile() " + NamedCommon.uTake.getFileName());
                                    if (!u2Commons.ClearFile(NamedCommon.uTake)) {
                                        uCommons.uSendMessage("ERROR when clearing data file");
                                    }
                                    break;
                                }
                            }
                        }
                        break;
                    case "real":
                        String fname = NamedCommon.u2Source + "_" + NamedCommon.datAct + ".LOADED";
                        cmd = "{WRI}{file=" + fname + "}{item=RFUEL.CONTROL}";
                        rtnString = u2Commons.MetaBasic(cmd);
                        if (NamedCommon.ZERROR) {
                            NamedCommon.Zmessage = rtnString;
                            return false;
                        }
                        if (!rtnString.toLowerCase().equals("ok")) {
                            cmd = "{EXE}{{exec=CREATE-FILE " + fname + " 1,1 17,1 ALU";
                            rtnString = u2Commons.MetaBasic(cmd);
                            if (rtnString.toLowerCase().equals("ok")) {
                                uCommons.uSendMessage("      > Created " + fname);
                            } else {
                                NamedCommon.ZERROR = true;
                                NamedCommon.Zmessage = rtnString;
                                return false;
                            }
                        }
                        cmd = "{CLF}{file=" + fname + "}";
                        rtnString = u2Commons.MetaBasic(cmd);
                        if (!rtnString.equals("ok")) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Clear-File failure :: " + cmd;
                        } else {
                            uCommons.uSendMessage("      > Cleared " + fname);
                        }

                        fname = NamedCommon.u2Source + "_" + NamedCommon.datAct + ".TAKE";
                        cmd = "{WRI}{file=" + fname + "}{item=RFUEL.CONTROL}";
                        rtnString = u2Commons.MetaBasic(cmd);
                        if (!rtnString.toLowerCase().equals("ok")) {
                            cmd = "{EXE}{{exec=CREATE-FILE " + fname + " 1,1 17,1 ALU";
                            rtnString = u2Commons.MetaBasic(cmd);
                            if (rtnString.toLowerCase().equals("ok")) {
                                uCommons.uSendMessage("      > Created " + fname);
                            } else {
                                NamedCommon.ZERROR = true;
                                NamedCommon.Zmessage = rtnString;
                                return false;
                            }
                        }
                        cmd = "{CLF}{file=" + fname + "}";
                        rtnString = u2Commons.MetaBasic(cmd);
                        if (!rtnString.equals("ok")) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Clear-File failure :: " + cmd;
                        } else {
                            uCommons.uSendMessage("      > Cleared " + fname);
                        }
                        break;
                    case "u2mnt":
                        cmd = "{CLF}{file=" + NamedCommon.u2Source + ".LOADED}";
                        rtnString = cc_uMount(cmd);
                        if (!rtnString.equals("ok")) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Clear-File failure :: " + cmd;
                        } else {
                            uCommons.uSendMessage("   .) Cleared " + NamedCommon.u2Source + ".LOADED");
                        }
                        cmd = "{CLF}{file=" + NamedCommon.u2Source + ".TAKE}";
                        rtnString = cc_uMount(cmd);
                        if (!rtnString.equals("ok")) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Clear-File failure :: " + cmd;
                        } else {
                            uCommons.uSendMessage("   .) Cleared " + NamedCommon.u2Source + ".TAKE");
                        }
                        break;
                    case "u2sockets":
                        cmd = "{CLF}{file=" + NamedCommon.u2Source + ".LOADED}";
                        rtnString = cc_uSocket("{socket}\n" + cmd);
                        if (!rtnString.equals("ok")) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Clear-File failure :: " + cmd;
                        } else {
                            uCommons.uSendMessage("   .) Cleared " + NamedCommon.u2Source + ".LOADED");
                        }
                        cmd = "{CLF}{file=" + NamedCommon.u2Source + ".TAKE}";
                        rtnString = cc_uSocket("{socket}\n" + cmd);
                        if (!rtnString.equals("ok")) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Clear-File failure :: " + cmd;
                        } else {
                            uCommons.uSendMessage("   .) Cleared " + NamedCommon.u2Source + ".TAKE");
                        }
                        break;
                    case "rmi.u2cs":
                        System.out.println("rmi:  is being developed - not ready yet");
                }
            } else {
                String tfname = "", lfname = "";
                switch (NamedCommon.protocol) {
                    case "u2cs":
                        tfname = NamedCommon.uLoaded.getFileName();
                        lfname = NamedCommon.uTake.getFileName();
                        break;
                    case "rmi.u2cs":
                        System.out.println("rmi:  is being developed - not ready yet");
                    default:
                        tfname = NamedCommon.u2Source + "_" + NamedCommon.datAct + ".TAKE";
                        lfname = NamedCommon.u2Source + "_" + NamedCommon.datAct + ".LOADED";
                }
                uCommons.uSendMessage("**** Part File process *****");
                uCommons.uSendMessage("   .) DID NOT clear " + tfname);
                uCommons.uSendMessage("   .) DID NOT clear " + lfname);
            }
            if (NamedCommon.ZERROR) {
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return false;
            }
            if (mode.equals("REFRESH") && !NamedCommon.isPrt && !NamedCommon.datOnly) {
                // Clear stats from UPLCTL
                String qry;
                uCommons.uSendMessage("   .) Clear stats from UPLCTL");
                qry = "DELETE FROM ";
                switch (NamedCommon.SqlDBJar) {
                    case "MSSQL":
                        qry += "[" + NamedCommon.rawDB + "].";
                        qry += "[upl].[UPLCTL] ";
                        break;
                    case "SNOWFLAKE":
                        qry += "upl.UPLCTL ";
                        break;
                    case "ORACLE":
                        qry += NamedCommon.rawDB + ".";
                        qry += "upl_UPLCTL ";
                        break;
                    case "MYSQL":
                        qry += NamedCommon.rawDB + ".";
                        qry += "upl_UPLCTL ";
                        break;
                    default:
                        uCommons.uSendMessage("[FATAL] Cannot identify DB type !!");
                        return false;
                }
                qry += "where Target = '" + dbFocus + "'";
                DDL.clear();
                DDL.add(qry);
                SqlCommands.ExecuteSQL(DDL);
                if (NamedCommon.ZERROR) return false;
                DDL.clear();
                if (!NamedCommon.datOnly) uCommons.uSendMessage("   .) Cleared UPLCTL where Target ='" + dbFocus + "'");
            }
        }

        Ctr = 0;
        boolean uniFail = false;

        int lenID;
        int diff = 0, rCnt = 0, aFrom = 0, aTo = 0, slowcnt = 0;
        String reply = "", junk = "", uvRec = "", uvID = "", msg = "";
        List<String> tmpList;
        int runtime = 0, dCnt = 0, lapse, recTot = 0, nulCtr = 0, grpTot = 0, noID = 0;

        String correlID = NamedCommon.ListID + "_" + NamedCommon.u2Source + "_" + NamedCommon.pid;

        String sel = NamedCommon.mapSelect, nsel = NamedCommon.mapNselect;
        if (!NamedCommon.item.equals("")) sel = "NO";
        if (!sel.equals("NO")) {
            if (sel.startsWith("#LIST")) {
                String[] selParts = sel.split(" ");
                if (selParts.length > 2) {
                    sel = "FORM.LIST " + selParts[1] + " " + selParts[2];
                } else {
                    sel = selParts[1];
                }
                correlID += "_" + MessageProtocol.offSet; // + "_";  ## DBG
            } else {
                // --------------------------------------------------------------------
                // When sending select statements in through a map - the statement
                // must be complete - i.e. runnable from TCL.  sel and nsel !!!
                // E.g. SELECT ACCOUNT LIKE ...L39
                // --------------------------------------------------------------------
                sel = NamedCommon.mapSelect;
                nsel = NamedCommon.mapNselect;
                if (!nsel.equals("")) sel += "<tm>" + nsel;
                // --------------------------------------------------------------------
                // The user will not know the qfile name but will know the source file.
                // So, replace the source file name with the qfile name.
                // --------------------------------------------------------------------
                String replaceME = NamedCommon.u2Source, qFile = NamedCommon.U2File.getFileName();
                if (replaceME.startsWith("DICT")) {
                    replaceME = replaceME.split("\\ ")[1];
                }
                sel = sel.replace(replaceME, qFile);
            }
        } else {
            sel = "";
            nsel = "";
        }

        if (correlID.length() > 20) {
            uCommons.uSendMessage("   .) WARNING Correlation ID is too long: " + correlID);
            correlID = NamedCommon.task + "_" + NamedCommon.pid + "_u" + ThreadLocalRandom.current().nextInt(11, 999);
            uCommons.uSendMessage("   .) WARNING Changed to " + correlID);
        }

        correl = correlID;

        if (show) uCommons.uSendMessage("   .) Load source data from " + sourceFile);
        if (!NamedCommon.BulkLoad) uCommons.uSendMessage("******** Manual INSERTs will be used ********");
        if (NamedCommon.datOnly) uCommons.uSendMessage("******** Working with DAT files only ********");
        if (String.valueOf(NamedCommon.aStep).endsWith("0")) NamedCommon.aStep--;
        long begun = System.nanoTime();
        /* ---------------------------------------------------------------------- */

        correlID = correlID.replaceAll("\\ ", "_");

        if (!NamedCommon.item.equals("")) {
            uvID = NamedCommon.item;
            uvRec = u2Commons.ReadAnItem(NamedCommon.u2Source, NamedCommon.item, "0", "0", "0");
            uvRec = uCommons.UV2SQLRec(NamedCommon.uID, NamedCommon.dynRec);
            LoadRow(uvID, uvRec);
            msg = "   .) Loading " + NamedCommon.U2File.getFileName() + "  " + uvID;
            uCommons.uSendMessage(msg);
            recTot = 1;
        } else {
            if (NamedCommon.aStep < 0) {
                if (NamedCommon.RunType.equals("REFRESH")) {
                    NamedCommon.aStep = NamedCommon.rStep;
                } else {
                    NamedCommon.aStep = NamedCommon.iStep;
                }
            }
            if (NamedCommon.aStep < 0) NamedCommon.aStep = 499;
            String subr = "2!uFETCH_PREP";
            String[] arglist = new String[20];
            for (int x = 0; x < 20; x++) { arglist[x] = ""; }
            arglist[0] = sel;
            arglist[1] = NamedCommon.RunType;
            arglist[2] = sourceFile;
            arglist[3] = correlID;
            arglist[4] = String.valueOf(NamedCommon.aStep);
            arglist[5] = NamedCommon.datAct;
            arglist[6] = "[<END>]";
            if (show) uCommons.uSendMessage("   .) Building " + correlID);
            String rtnString = "";
            switch (NamedCommon.protocol) {
                case "u2cs":
                    u2cs_CallFetchPrep(subr, arglist);
                    if (NamedCommon.ZERROR) return false;
                    break;
                case "real":
                    String args = "";
                    for (int al = 0; al < 6; al++) {
                        args += "<tm>" + arglist[al];
                    }
                    String cmd = "{MSVC}{subr-x-uFETCH_PREP}{args=" + args.substring(4, args.length()) + "}";
                    rtnString = u2Commons.MetaBasic(cmd);
                    if (NamedCommon.ZERROR) return false;
                    break;
                case "u2mnt":
                    rtnString = u2Commons.u2mnt_CallSrtn(subr, arglist);
                    if (NamedCommon.ZERROR) return false;
                    break;
                case "u2sockets":
                    rtnString = u2Commons.u2sock_CallSrtn(subr, arglist);
                    if (NamedCommon.ZERROR) return false;
                    break;
                case "rmi.u2cs":
                    System.out.println("rmi:  is being developed - not ready yet");
                    return false;
            }
            if (NamedCommon.ZERROR) return false;

            // ------------------------------------------------------------------------- //

            boolean loopSw = true;
            String ffr = "", tto = "";
            rCnt = 0;
            slowcnt = 0;
            if (show) uCommons.uSendMessage("   .) get blocks of data for processing");
            if (show) uCommons.uSendMessage("   .) use LoadDte " + NamedCommon.BatchID);
            if (NamedCommon.encRaw) uCommons.uSendMessage("   .) Encrypt raw is ON - this takes time");
            sent = System.nanoTime();
            boolean flipCnts = true;
            msg = "";
            boolean stopnow = false;
            NamedCommon.SqlSchema = NamedCommon.rawSC;
            subr = "2!uFETCH";
            for (int x = 0; x < 20; x++) { arglist[x] = ""; }

            arglist[1] = sourceFile;
            arglist[4] = String.valueOf(NamedCommon.RQM);
            arglist[5] = correlID;
            arglist[6] = "[<END>]";

            GarbageCollector.setStart(System.nanoTime());
            stopnow = coreCommons.StopNow();
            coreCommons.SlowDown(fetch);
            if (stopnow) uniFail = true;

            uCommons.uSendMessage("   .) ************************* PROCESSING NOW **************************");
            if (NamedCommon.H2Server) Resilience = true;
            
            while (loopSw && !stopnow) {
                if (flipCnts) {
                    if (aFrom == 0) {
                        aFrom = 1;
                        aTo = NamedCommon.aStep;
                    } else {
                        aFrom = aTo + 1;
                        aTo = aFrom + NamedCommon.aStep;
                    }
                }
                if (show && msg.equals("")) {
                    ffr = uCommons.oconvM(String.valueOf(aFrom), "MD0,");
                    ffr = uCommons.RightHash(ffr, 15);
                    msg = "   .) Fetching " + ffr;
                }
                if (NamedCommon.RQM < NamedCommon.aStep) uCommons.Sleep(1);
                arglist[2] = String.valueOf(aFrom);
                arglist[3] = String.valueOf(aTo);
                if (NamedCommon.debugging) uCommons.uSendMessage("   .) Requesting " + correlID + "_" + aFrom);
                if (NamedCommon.debugging || Resilience) uCommons.uSendMessage("      .) ask for " + aFrom + " to " + aTo);

                String[] inLines;
                int totLines = 0;
                coreCommons.SlowDown(fetch);
                reply = "";
                switch (NamedCommon.protocol) {
                    case "u2cs":
                        reply = u2cs_CallFetchData(sourceFile, arglist);
                        if (reply.equals("<<FAIL>>")) IamRestarting=true;
                        break;
                    case "real":
                        String args = "<tm>";
                        for (int al = 0; al < 20; al++) {
                            if (arglist[al].equals("[<END>]")) break;
                            args += "<tm>" + arglist[al];
                        }
                        String cmd = "{MSVC}{subr-x-uFETCH}{args=" + args.substring(4, args.length()) + "}";
                        rtnString = u2Commons.MetaBasic(cmd);
                        if (NamedCommon.ZERROR) return false;
                        break;
                    case "u2mnt":
                        reply = u2mnt_CallSrtn(subr, arglist);
                        break;
                    case "u2sockets":
                        reply = u2sock_CallSrtn(subr, arglist);
                        break;
                    case "rmi.u2cs":
                        System.out.println("rmi:  is being developed - not ready yet");
                }

                if (IamRestarting) {
                    uCommons.uSendMessage("Wait on all 'dat' files to be loaded .......");
                    uCommons.WaitOnFiles();
                    uCommons.uSendMessage("Remove data loaded by this run .............");
                    RemoveThisRunData();
                    uCommons.uSendMessage("Remove SAVEDLIST records for ["+correlID+"] .....");
                    u2Commons.CleanupLists(correlID);
                    return true;        // true = an error has been found
                }

                if (NamedCommon.debugging) uCommons.uSendMessage("      .) reply received ");

                if (reply.length() < 1) {
                    // add to resilience handling - RESTART: do the mesage again.
                    // Should never experience this condition as the ???_CallFetchData should handle it
                    flipCnts = false;
                    if (show || NamedCommon.debugging) {
                        tto = uCommons.oconvM(String.valueOf(aTo), "MD0,");
                        tto = uCommons.RightHash(tto, 15);
                        msg += " to " + tto + " received NOTHING. (1)";
                        uCommons.uSendMessage(msg);
                        msg = "";
                    }
                    slowcnt++;
                    uCommons.eMessage = "   .) SourceDB running slow ... wait 20 seconds"
                            + " (" + slowcnt + " of " + maxWait + ")";
                    uCommons.uSendMessage(uCommons.eMessage);
                    uCommons.uSendMessage("   .) Pausing for 20 seconds");
                    uCommons.Sleep(20);
                    if (slowcnt > maxWait) {
                        loopSw = false;
                        uCommons.eMessage = "SourceDB may be off-line.";
                        uCommons.uSendMessage("   .) " + uCommons.eMessage);
                        return false;
                    }
                } else {
                    flipCnts = true;
                    slowcnt = 0;
                    // ------------------------------------------------------------------------------------------
                    // BufferedReader the reply - PURE SPEED
                    // got this structure from copilot
                    // HandleReply(reply);
                    thisReply = reply;
                    while (!thisReply.isEmpty()) {
                        // ------------------------------------------------------------------------------------------
                        coreCommons.SlowDown(fetch);
                        inLines = GetLines();
                        // ------------------------------------------------------------------------------------------
                        dCnt = inLines.length;
                        totLines += dCnt;
                        stopnow = coreCommons.StopNow();
                        if (stopnow) break;
                        for (int a = 0; a < dCnt; a++) {
//                            if (a % 50 == 0) coreCommons.SlowDown(fetch);
                            uvRec = inLines[a];
                            if (uvRec.equals("[<END>]")) loopSw = false;
                            if (uvRec.startsWith("[<END>]")) loopSw = false;
                            if (loopSw) {
                                tmpList = new ArrayList<>(Arrays.asList(uvRec.split("<im>")));
                                if (tmpList.size() == 2) {
                                    uvID = tmpList.get(0);
                                    uvRec = tmpList.get(1);
                                } else {
                                    uvRec = "";
                                    uvID = "";
                                    if (tmpList.size() == 1) uvID = tmpList.get(0);
                                }
                                lenID = uvID.length();
                                if (lenID > 0) {
                                    if (uvRec.length() > 0) {
                                        recTot++;
                                        grpTot++;
                                    } else {
                                        uvRec += "EmptyRecord";
                                        nulCtr++;
                                    }
                                    inLines[a] = "";
                                    LoadRow(uvID, uvRec);
                                    coreCommons.SlowDown(fetch);
                                } else {
                                    noID++;
                                }
                                tmpList.clear();
                            } else {
                                break;
                            }
                            tmpList.clear();
                        }
                        inLines = null;
                        if (totLines % 5000 == 0) {
                            uCommons.uSendMessage("      .) Processed " + uCommons.RightHash(uCommons.oconvM(String.valueOf(totLines), "MD0,"), 10) + "  lines of data.");
                        }
                    }
                    thisReply = "";

                    diff = abs((aTo - aFrom)) + 1;
                    rCnt = rCnt + diff;
                    if (rCnt >= NamedCommon.showAT || !loopSw) {
                        got = System.nanoTime();
                        laps = (got - sent) / nanoSecs;
                        lapse = Integer.valueOf(String.valueOf(laps));
                        if (show) {
                            tto = uCommons.oconvM(String.valueOf(aTo), "MD0,");
                            tto = uCommons.RightHash(tto, 15);
                            msg += " to " + tto + " processed in "
                                    + uCommons.RightHash(String.valueOf(lapse), 3) + " seconds";
                            uCommons.uSendMessage(msg);
                            msg = "";
                            coreCommons.SlowDown(fetch);
                        }
                        GarbageCollector.CleanUp();
                        sent = got;
                        rCnt = 0;
                        grpTot = 0;
                    }
                    stopnow = coreCommons.StopNow();
                    coreCommons.SlowDown(fetch);
                    inLines = null;
                }
            }
            msg = "   .) Finished collecting last " +
                    uCommons.oconvM(String.valueOf(dCnt), "MD0,") + " row(s) " +
                    "including [<END>] marker";
            uCommons.uSendMessage(msg);
        }

        if (!DDL.isEmpty()) uCommons.SQLDump(DDL);
        DDL.clear();

        /* ---------------------------------------------------------------------- */

        if (!uniFail) {
            long finished = System.nanoTime();
            laps = (finished - begun) / nanoSecs;
            lapse = Integer.valueOf(String.valueOf(laps));
            runtime = runtime + lapse;
            junk = "   .) Fetched " +
                    uCommons.oconvM(String.valueOf(recTot + nulCtr + noID), "MD0,") +
                    " " + NamedCommon.databaseType +
                    " records in " + runtime + " seconds";
            if (show) uCommons.uSendMessage("   .) " + NamedCommon.block);
            if (show) uCommons.uSendMessage(junk);
            if (show) uCommons.uSendMessage("   .) " + NamedCommon.block);
            if (nulCtr > 0) {
                junk = "   .) Including "
                        + uCommons.oconvM(String.valueOf(nulCtr), "MD0,")
                        + " empty record(s) in source data ";
                if (show) uCommons.uSendMessage(junk);
            }
            if (noID > 0) {
                junk = "   .) Including "
                        + uCommons.oconvM(String.valueOf(noID), "MD0,")
                        + " records with a NULL ID in source data ";
                if (show) uCommons.uSendMessage(junk);
            }

            if (DDL.size() > 0) {
                // for small files that don't get logging info shown.
                uCommons.SQLDump(DDL);
                uCommons.uSendMessage("   >> This dat file :: " + Ctr);
                uCommons.uSendMessage("   >> Tot dat lines :: " + TotalInserts);
                TotalInserts += Ctr;
                uCommons.uSendMessage("   >> " +
                        uCommons.oconvM(String.valueOf(TotalInserts), "MD0") +
                        " rows loaded to dat files");
                Ctr = 0;
                thisSize = 0;
                DDL.clear();
                TotalInserts = 0;
            }
            // ## gitlab 921
            if (!NamedCommon.ZERROR) {
                if (!NamedCommon.datOnly) {
//                String source = "[" + NamedCommon.dbhost + "].[" + NamedCommon.datAct + "].[" + NamedCommon.u2FileRef + "]";
                    String source = "[" + NamedCommon.dbhost + "].[" + NamedCommon.datAct + "].[" + NamedCommon.u2Source + "]";
                    String target = dbFocus;
                    MessageProtocol.UplCtl.set(0, NamedCommon.BatchID);
                    MessageProtocol.UplCtl.set(1, NamedCommon.RunType);
                    MessageProtocol.UplCtl.set(2, NamedCommon.serial);
                    MessageProtocol.UplCtl.set(3, NamedCommon.xMap);
                    MessageProtocol.UplCtl.set(4, "012-Fetch");
                    MessageProtocol.UplCtl.set(5, source);
                    MessageProtocol.UplCtl.set(6, sel);
                    MessageProtocol.UplCtl.set(7, target);
                    MessageProtocol.UplCtl.set(8, String.valueOf(recTot + nulCtr + noID));
                    MessageProtocol.UplCtl.set(9, String.valueOf(recTot + nulCtr + noID));
                    MessageProtocol.UplCtl.set(10, String.valueOf(recTot));
                    MessageProtocol.UplCtl.set(11, String.valueOf(nulCtr));
                    MessageProtocol.UplCtl.set(12, String.valueOf(noID));
                    MessageProtocol.UplCtl.set(13, "0");
                    MessageProtocol.UplCtl.set(14, String.valueOf(recTot));
//                    MessageProtocol.UplCtl.set(14, String.valueOf(recTot + nulCtr + noID));

                    String fName;
                    boolean dict = (NamedCommon.u2Source.startsWith("DICT"));
                    if (dict && !NamedCommon.sqlTarget.startsWith("DICT_")) {
                        fName = "DICT_" + NamedCommon.sqlTarget;
                    } else {
                        fName = NamedCommon.sqlTarget;
                    }
                }
            } else {
                uniFail = true;
            }
        }

        if (!(NamedCommon.U2File == null)) {
            String fName = NamedCommon.U2File.getFileName();
            while (true) {
                try {
                    if (!u2Commons.rfuelFiles.contains(fName)) {
                        NamedCommon.VOC.setRecordID(fName);
                        NamedCommon.VOC.deleteRecord();
                        int vocPx = NamedCommon.PointerFiles.indexOf(fName);
                        if (vocPx > -1) NamedCommon.PointerFiles.remove(vocPx);
                        uCommons.uSendMessage("   .) VOC entry for " + fName + " was removed.");
                        vocPx = NamedCommon.OpenFiles.indexOf(fName);
                        if (vocPx > -1) {
                            NamedCommon.u2Handles.get(vocPx).close();
                            NamedCommon.OpenFiles.remove(vocPx);
                            NamedCommon.u2Handles.remove(vocPx);
                        }
                    }
                    break;
                } catch (UniFileException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        break;
                    }
                }
            }
                // ignore it - it will get cleaned up later.
        }

        if (!correl.equals("")) u2Commons.CleanupLists(correlID);

        uCommons.WaitOnFiles();

        return uniFail;
    }

    public static String[] GetLines() {
        String part = "";
        int batchSize = 0;

        ArrayList<String> replyList = new ArrayList<>();
        bReader = new BufferedReader(new StringReader(thisReply));
        try {
            part = bReader.readLine();
            batchSize += part.length();
        } catch (IOException e) {
            uCommons.uSendMessage("BufferedReader ERROR: " + e.getMessage());
            part = null;
        }

        while (part != null) {
            thisReply = thisReply.substring(part.length(), thisReply.length());
            while (!thisReply.isEmpty() && (thisReply.charAt(0) == '\n' || thisReply.charAt(0) == '\r')) {
                thisReply = thisReply.substring(1, thisReply.length());
            }

            replyList.add(part);
            if (thisReply.isEmpty()) {
                break;
            }
            if (batchSize > bestLength) {
                break;
            }
            try {
                part = bReader.readLine();
                batchSize += part.length();
            } catch (IOException e) {
                uCommons.uSendMessage("ERROR: BufferedReader - " + e.getMessage());
                part = null;
            }
        }
        String[] inLines = replyList.toArray(new String[0]);
        try {
            bReader.close();
        } catch (IOException e) {
            uCommons.uSendMessage("ERROR: BufferedReader.close() - " + e.getMessage());
        }
        bReader = null;
        replyList.clear();
        part = "";
        batchSize = 0;
        return inLines;
    }

    public static String GetEncSeed() {
        String eSeed = "";
        if (NamedCommon.AES) return "";
        String[] sqlObj = dbFocus.split("\\.");
//        String encQry = "SELECT max(CAST(REPLACE(LoadDte, '''','') as decimal(25,0))) as LoadDte, RawData FROM " + dbFocus;
        String encQry = "SELECT LoadDte, RawData FROM " + dbFocus;
        //        encQry = "[" + NamedCommon.rawDB + "].[raw]." + sqlObj[sqlObj.length - 1];
        encQry += " where uID = 'encSeed'" +
                " group by LoadDte, RawData" +
                " order by LoadDte, RawData";

//        String delQry = "DELETE FROM " + "[" + NamedCommon.rawDB + "].[raw]." + sqlObj[2] +
        String delQry = "DELETE FROM " + dbFocus +
                " where uID = 'encSeed' and LoadDte = '@'";

        String ldte = "  ";
        NamedCommon.uCon = null;
        if (ConnectionPool.jdbcPool.size() > 0) {
            NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
        }
        if (NamedCommon.uCon == null) {
            if (!NamedCommon.tConnected) SqlCommands.ConnectSQL();
            if (!NamedCommon.tConnected) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
            }
        }
        ResultSet rs = null;
        Statement stmt = null, delStmt;
        String delCmd, delDte, nxtEnc, rawD = "RawData", loadD = "LoadDte";
        if (NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE")) {
            rawD = rawD.toUpperCase();
            loadD = loadD.toUpperCase();
        }

        // the block below finds the encseed and removes OLD encSeed items
        try {
            NamedCommon.uCon.setAutoCommit(true);
            stmt = NamedCommon.uCon.createStatement();
            ArrayList<String> exeBlock = new ArrayList<>();
            rs = stmt.executeQuery(encQry);
            while (rs.next()) {
                eSeed = rs.getString(rawD);
                ldte = rs.getString(loadD);
                while (rs.next()) {
                    nxtEnc = rs.getString(rawD);
                    if (nxtEnc.equals(eSeed)) {
                        delDte = rs.getString(loadD);
                        delCmd = delQry.replaceAll("\\@", delDte);
                        exeBlock.add(delCmd);
                        uCommons.uSendMessage("      .> Remove old seed: " + delCmd);
                    }
                }
                SqlCommands.ExecuteSQL(exeBlock);
                exeBlock = null;
                delCmd = "";
                delDte = "";
                delQry = "";
            }
            if (eSeed.equals("")) uCommons.Sleep(0);
            rs.close();
            rs = null;
            stmt.close();
            stmt = null;
            if (eSeed.equals("")) NamedCommon.mkEnc = true;
        } catch (SQLException e) {
            // Still waiting on EncSeed.
        }
        sqlObj = null;
        encQry = "";
        ldte = "";
        return eSeed;
    }

    private static String RestExtract(String map) {

        String xmlString = "";

        switch (NamedCommon.protocol) {
            case "u2cs":
                xmlString = Extract_Pick();
                break;
            case "real":
                xmlString = Extract_Pick();
                break;
            case "u2sockets":
                xmlString = rExtract_sockets(map);
                break;
            case "u2mnt":
                xmlString = rExtract_mnt(map);
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
            default:
        }

        return xmlString;
    }

    private static String Extract_Pick() {
        uCommons.uSendMessage("   .) unpack the record.");
        rExtract_Pick();
        String XMLreply = NamedCommon.Zmessage;
        return XMLreply;
    }

    private static void rExtract_Pick() {

        UniString uID = NamedCommon.uID;
        NamedCommon.DataList.clear();
        NamedCommon.DataLineage.clear();
        NamedCommon.SubsList.clear();
        NamedCommon.TmplList.clear();
        NamedCommon.Templates.clear();

        UniDynArray LineArray;
        String XMLreply = "ERROR";
        String csvName, content;
        int nbrCsvItems = NamedCommon.csvList.length;
        for (int i = 0; i < nbrCsvItems; i += 1) {
            csvName = NamedCommon.BaseCamp + "/maps/" + NamedCommon.csvList[i];
            uCommons.uSendMessage("   .) view record through " + NamedCommon.csvList[i]);
            ArrayList<String> csvLines = null;
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
                csvLines = new ArrayList<>(Arrays.asList(content.split("\\r?\\n")));
                UniString SaveID = NamedCommon.uID;
                NamedCommon.uID = uID;
                LineArray = uCommons.PrepareCsvDetails(csvLines);
                if (NamedCommon.ZERROR) return;
                BurstSplit.ProcessRow(LineArray, InRow);
                if (NamedCommon.ZERROR) {
                    XMLreply = NamedCommon.Zmessage;
                    return;
                }
                NamedCommon.uID = SaveID;
            } catch (IOException e) {
                NamedCommon.Zmessage = "<<FAIL>> Cannot find " + csvName;
                NamedCommon.ZERROR = true;
                return;
            }
        }
    }

    public static void rExtract_Sql() {

        if (uCommons.APIGetter("jdbcCon").equals("")) return;

        // 2. Get Data from the SQL Database -------------

        String jdbcCon = uCommons.APIGetter("jdbcCon");
        String jdbcUsr = uCommons.APIGetter("jdbcUsr");
        String jdbcPwd = uCommons.APIGetter("jdbcPwd");
        String jdbcCmd = uCommons.APIGetter("jdbcCmd");
        String jdbcList = uCommons.APIGetter("jdbcList");

        Properties mapProps = new Properties();
        if (!jdbcCon.equals("")) {
            jdbcCon = uCommons.GetValue(mapProps, "jdbcCon", jdbcCon);
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Your map is missing the definition for jdbcCon";
            return;
        }
        if (!jdbcUsr.equals("")) {
            jdbcUsr = uCommons.GetValue(mapProps, "jdbcUsr", jdbcUsr);
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Your map is missing the definition for jdbcUsr";
            return;
        }
        if (!jdbcPwd.equals("")) {
            jdbcPwd = uCommons.GetValue(mapProps, "jdbcPwd", jdbcPwd);
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Your map is missing the definition for jdbcPwd";
            return;
        }
        if (!jdbcCmd.equals("")) {
            jdbcCmd = uCommons.GetValue(mapProps, "jdbcCmd", jdbcCmd);
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Your map is missing the definition for jdbcCmd";
            return;
        }
        if (!jdbcList.equals("")) {
            jdbcList = uCommons.GetValue(mapProps, "jdbcCmd", jdbcList);
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Your map is missing the definition for jdbcList";
            return;
        }

        try {
            ConnectionPool.AddToPool(jdbcCon, jdbcUsr, jdbcPwd);
        } catch (SQLException err) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "rExtract_Sql(): " + err.getMessage();
            return;
        }

        ArrayList<String> colNames = new ArrayList<>();
        ArrayList<String> colTags = new ArrayList<>();
        ArrayList<String> colValues = new ArrayList<>();
        ArrayList<String> colTmpls = new ArrayList<>();
        ArrayList<String> listArr = new ArrayList<>(Arrays.asList(jdbcList.split(",")));
        ArrayList<String> lines;
        String content, listName;

        for (int l = 0; l < listArr.size(); l++) {
            listName = listArr.get(l);
            try {
                String fqfn = NamedCommon.BaseCamp + NamedCommon.slash + "maps" + NamedCommon.slash + listName;
                content = new String(Files.readAllBytes(Paths.get(fqfn)));
                if (content.startsWith("ENC(")) {
                    content = uCipher.Decrypt(content);
                } else {
                    if (NamedCommon.KeepSecrets) {
                        uCommons.WriteDiskRecord(fqfn + "_text", content);
                        uCipher.isLic = false;
                        String encCsv = uCipher.Encrypt(content);
                        uCommons.WriteDiskRecord(fqfn, encCsv);
                        uCipher.isLic = true;
                        encCsv = "";
                    }
                }
                lines = new ArrayList<>(Arrays.asList(content.split("\\r?\\n")));
                for (int ln = 0; ln < lines.size(); ln++) {
                    content = lines.get(ln);
                    while (content.contains(" ")) {
                        content = content.replaceAll("\\ ", "");
                    }
                    colNames.add(uCommons.FieldOf(content, ",", 1));
                    colTags.add(uCommons.FieldOf(content, ",", 2));
                    colTmpls.add(uCommons.FieldOf(content, ",", 3));
                    colValues.add("");
                }
                lines.clear();
            } catch (IOException e) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "rExtract_Sql().IOException" + e.getMessage();
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return;
            }
        }
        listArr.clear();

        // allow data replacements in the jdbc Command
        // e.g. SELECT * FROM DB.SCH.TABLE WHERE COLUMN-1 = '$variable-1$' AND COLUMN-2 = '$variable-2$'
        // $variable-1$ and $variable-2$ MUST be in colTags or in NamedCommon.SubsList

        int posx;
        String theTag, datum;
//        int nbrTags = StringUtils.countOccurrencesOf(jdbcCmd, "$");
        int nbrTags = uCommons.NumberOf(jdbcCmd, "$");
        nbrTags = nbrTags / 2;
        if (nbrTags > 0) {
            while (nbrTags > 0) {
                theTag = uCommons.FieldOf(jdbcCmd, "\\$", 2);
                theTag = "$" + theTag + "$";
                jdbcCmd = jdbcCmd.replace(theTag, "@@@@");

//                nbrTags = StringUtils.countOccurrencesOf(jdbcCmd, "$");
                nbrTags = uCommons.NumberOf(jdbcCmd, "$");
                nbrTags = nbrTags / 2;

                posx = colTags.indexOf(theTag);
                if (posx < 0) {
                    posx = NamedCommon.SubsList.indexOf(theTag);
                    if (posx < 0) {
                        uCommons.uSendMessage("   .) Cannot find [" + theTag + "] - jdbcCmd will fail so IGNORING SQL component.");
                        return;
                    }
                    datum = NamedCommon.DataList.get(posx);
                } else {
                    datum = colValues.get(posx);
                }
                NamedCommon.uCon = ConnectionPool.getConnection(jdbcCon);
                if (NamedCommon.uCon == null) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                    return;
                }
                try {
                    Statement stmt = NamedCommon.uCon.createStatement();
                    ResultSet rs = null;
                    String[] parts = datum.split("<fm>");
                    String tmpCmd;
                    int recCnt;
                    for (int p = 0; p < parts.length; p++) {
                        recCnt = 0;
                        tmpCmd = jdbcCmd.replace("@@@@", parts[p]);
                        uCommons.uSendMessage("       >> " + tmpCmd);
                        rs = stmt.executeQuery(tmpCmd);
                        while (rs.next()) {
                            if (recCnt > 1) {
                                uCommons.uSendMessage("Cannot handle more than 1 row per SQL data gather.");
                                continue;
                            }
                            for (int rc = 0; rc < colNames.size(); rc++) {
                                content = rs.getString(colNames.get(rc));
                                colValues.set(rc, content);
                            }
                            recCnt++;
                        }
                        for (int rc = 0; rc < colNames.size(); rc++) {
                            posx = NamedCommon.SubsList.indexOf(colTags.get(rc));
                            if (posx < 0) {
                                NamedCommon.SubsList.add(colTags.get(rc));
                                NamedCommon.DataList.add(colValues.get(rc));
                                NamedCommon.DataLineage.add("SQL-mix-rfuel");
                                NamedCommon.TmplList.add(colTmpls.get(rc));
                                NamedCommon.AsocList.add(false);
                                if (NamedCommon.Templates.indexOf(colTmpls.get(rc)) < 0) {
                                    NamedCommon.Templates.add(colTmpls.get(rc));
                                }
                            } else {
                                content = NamedCommon.DataList.get(posx) + NamedCommon.FMark + colValues.get(rc);
                                NamedCommon.DataList.set(posx, content);
                                content = NamedCommon.DataLineage.get(posx) + NamedCommon.FMark + "SQL-mix-rfuel";
                                NamedCommon.DataLineage.set(posx, content);
                            }
                        }
                        content = "";
                    }
                    rs.close();
                    stmt.close();
                    rs = null;
                    stmt = null;
                } catch (SQLException err) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = err.getMessage();
                    return;
                }
            }
        } else {
            try {
                NamedCommon.uCon = ConnectionPool.getConnection(jdbcCon);
                if (NamedCommon.uCon == null) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                    return;
                }
                Statement stmt = NamedCommon.uCon.createStatement();
                int recCnt = 0;
                uCommons.uSendMessage("       >> " + jdbcCmd);
                ResultSet rs = stmt.executeQuery(jdbcCmd);
                boolean append = false;
                while (rs.next()) {
                    if (recCnt > 0) {
                        append = true;
                    }
                    for (int rc = 0; rc < colNames.size(); rc++) {
                        content = rs.getString(colNames.get(rc));
                        if (append) {
                            content = colValues.get(rc) + NamedCommon.FMark + content;
                        }
                        colValues.set(rc, content);
                    }
                    recCnt++;
                }
                rs.close();
                stmt.close();
                rs = null;
                stmt = null;
                for (int rc = 0; rc < colNames.size(); rc++) {
                    if (NamedCommon.SubsList.indexOf(colTags.get(rc)) < 0) {
                        NamedCommon.SubsList.add(colTags.get(rc));
                        NamedCommon.DataList.add(colValues.get(rc));
                        NamedCommon.Templates.add(colTmpls.get(rc));
                        NamedCommon.DataLineage.add("SQL-mix-rfuel");
                        NamedCommon.TmplList.add(colTmpls.get(rc));
                        NamedCommon.AsocList.add(false);
                    }
                }
            } catch (SQLException err) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = err.getMessage();
                return;
            }
        }
    }

    public static void rExtract_Api() {

    }

    private static ArrayList<String> GetColNames(String list) {
        ArrayList<String> names = new ArrayList<>();
        ArrayList<String> lines = new ArrayList<>();
        return names;
    }

    private static String rExtract_sockets(String map) {
        NamedCommon.uID = new UniString(NamedCommon.item);
        rExtract_Pick();
        return NamedCommon.Zmessage;
    }

    private static String rExtract_mnt(String map) {
        NamedCommon.uID = new UniString(NamedCommon.item);
        rExtract_Pick();
        return NamedCommon.Zmessage;
    }

    public static void PrepareSQLDumper() {
        String fName = NamedCommon.sqlTarget;

        fName = fName.replaceAll("\\.", "_");
        fName = fName.replaceAll("\\,", "_");
        fName = fName.replaceAll("\\ ", "_");

        if (NamedCommon.SqlDBJar.equals("MSSQL")) {
            dbFocus = "[" + NamedCommon.rawDB + "].[raw].[" + fName + "]";
        } else if (NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE")) {
            dbFocus = NamedCommon.rawDB + ".raw." + fName;
        } else {
            dbFocus = NamedCommon.rawDB + "." + "raw_" + fName;
        }

        // uStreams should not try to create the file every bloody time !! It slows it down too much.
        // Save those files it has created in an array and skip doing it more than once !!

        if (NamedCommon.streamedFiles.indexOf(dbFocus) >= 0 && NamedCommon.isNRT) return;

        if (NamedCommon.isPrt && !APImap.APIget("firstpart").toLowerCase().equals("true")) {
            int tries = 0, max = 30;
            while (!SqlCommands.Exists(fName)) {
                uCommons.uSendMessage("   .) Waiting for target table " + fName + " to be created.");
                uCommons.Sleep(2);
                tries++;
                if (tries > max) {
                    uCommons.uSendMessage("   .) The First part-file has not been executed. Cannot proceed.");
                    NamedCommon.ZERROR = true;
                    return;
                }
            }
        }

        if (!NamedCommon.isNRT) uCommons.uSendMessage("   .) Prepare target table " + dbFocus);
        List<String> DDL = new ArrayList<String>();
        String[] tCols = (NamedCommon.rawCols).split(",");

        // DropTable returns "" if prt and not firstpart.

        String dropTable = SqlCommands.DropTable(NamedCommon.rawDB, NamedCommon.rawSC, fName);
        DDL.clear();
        if (!dropTable.equals("")) {
            SqlCommands.SetBatching(false);
            uCommons.uSendMessage("      > Dropping table  " + dbFocus);
            DDL.add(dropTable);
            SqlCommands.ExecuteSQL(DDL);
            SqlCommands.SetBatching(true);
            if (NamedCommon.ZERROR) return;
        } else {
            if (!NamedCommon.isNRT) uCommons.uSendMessage("      > NO drop table   " + dbFocus);
        }

        if (NamedCommon.isPrt && !uCommons.APIGetter("firstpart").toLowerCase().equals("true")) {
            uCommons.uSendMessage("      > Part file processing - no TABLE duties.");
        } else {
            if (!uCommons.TableExists(NamedCommon.sqlTarget)) {
                String createTable = SqlCommands.CreateTable(NamedCommon.rawDB, NamedCommon.rawSC, fName, tCols);
                DDL.clear();
                if (!NamedCommon.isNRT) uCommons.uSendMessage("      > Creating table  " + dbFocus);
                DDL.add(createTable);
                SqlCommands.SetBatching(false);
                SqlCommands.ExecuteSQL(DDL);
                SqlCommands.SetBatching(true);
                if (NamedCommon.ZERROR) return;
                DDL.clear();
                String ctlLine = SqlCommands.CreateIndex(NamedCommon.rawDB, NamedCommon.rawSC, fName, tCols);
                if (!ctlLine.equals("")) {
                    if (!NamedCommon.isNRT) uCommons.uSendMessage("      > Creating Index");
                    DDL.add(ctlLine);
                    SqlCommands.SetBatching(false);
                    SqlCommands.ExecuteSQL(DDL);
                    SqlCommands.SetBatching(true);
                    NamedCommon.ZERROR=false;
                    NamedCommon.Zmessage="";
                    DDL.clear();
                    if (NamedCommon.ZERROR) return;
                    if (!NamedCommon.isNRT) uCommons.uSendMessage("   .) Done");
                }
            } else {
                if (!NamedCommon.isNRT) uCommons.uSendMessage("      > No create table - table exists.");
                if (!NamedCommon.isNRT) uCommons.uSendMessage("      > No create index");
            }
        }

        if (NamedCommon.isNRT) NamedCommon.streamedFiles.add(dbFocus);
    }

    public static void LoadRow(String uID, String inRec) {

        if (uID.length() > 0) {
            if (NamedCommon.Scrub_uID) {
                uID = BurstSplit.TransformString("T-uID", uID);
            }
            LoadDte = NamedCommon.BatchID;
            procNum++;
            if (procNum > NamedCommon.MaxProc) procNum = 1;
            String MD5 = "", dataSource = "";

            boolean inclHost = NamedCommon.rawCols.toLowerCase().contains("dbhost");
            boolean inclAcct = NamedCommon.rawCols.toLowerCase().contains("account");
            boolean inclFile = NamedCommon.rawCols.toLowerCase().contains("file");
            if (!inclHost) {
                inclAcct = false;
                inclFile = false;
            }
            if (inclHost) dataSource = dataSource + NamedCommon.dbhost + NamedCommon.IMark;     // rFuel.properties
            if (inclAcct) dataSource = dataSource + NamedCommon.datAct + NamedCommon.IMark;     // message
            if (inclFile) dataSource = dataSource + NamedCommon.u2Source + NamedCommon.IMark;   // map

            dataSource = dataSource + uID + NamedCommon.IMark + inRec;
            MD5 = uCommons.GetMD5(dataSource);

            // uStreams fails when encSeed is empty ---------------
            if (encSeed.equals("") && NamedCommon.encRaw) {
                if (!NamedCommon.AES) {
                    if (!NamedCommon.cSeed.equals("")) {
                        encSeed = NamedCommon.cSeed;
                    } else {
                        encSeed = GetEncSeed();
                    }
                    if (encSeed.equals("")) {
                        // use the rFuel encryption method
                        uCommons.uSendMessage("      .> Creating encryption seed.");
                        String extraDat = "";
                        if (inclHost) extraDat = "'" + NamedCommon.dbhost + "','" + NamedCommon.datAct + "',";
                        ExtractManager.CreateEncSeed();
                        String encMD5 = uCommons.GetMD5(NamedCommon.BatchID + "," + encSeed);

                        // changed to not use NamedCommon.BatchID because it was bveing created by ALL processes - same datetime !!
                        String encDate = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());

                        String insRec = "'" + encMD5 + "'"
                                + "," + "'" + "encSeed" + "'"
                                + "," + "'" + encDate + "'"
                                + "," + 0 + ","
                                + extraDat
                                + "'" + encSeed + "'";

                        String encRow = "INSERT INTO " + dbFocus;
                        String cols = NamedCommon.rawCols;
                        cols = cols.replaceAll("\\-", "");
                        cols = cols.replaceAll("\\.", "");
                        cols = cols.replaceAll("\\*", "");
                        encRow += " (" + cols + ") VALUES (" + insRec + ")";
                        DDL.clear();
                        DDL.add(encRow);
                        SqlCommands.ExecuteSQL(DDL);
                        DDL.clear();
                        encMD5 = "";
                        insRec = "";
                        encRow = "";
                        cols = "";
                    }
                } else {
                    // AES uses a secret and salt - NOT an encSeed
                }
            }
            // ----------------------------------------------------

            // gitlab #16 -----------------------------------------
            // IF encrypting at source : NamedCommon.encRaw MUST be false !!
            //    NB: encrypting at source is SLOW but much more secure.
            if (NamedCommon.encRaw) inRec = uCipher.v2Scramble(uCipher.keyBoard25, inRec, encSeed);
            // ----------------------------------------------------

            if (!NamedCommon.ZERROR) {
                if (inRec.contains(quote)) inRec = inRec.replace(quote, quote + quote);
                String valString;
                valString = quote + MD5 + quote;
                valString += Komma + quote + uID + quote;
                valString += Komma + quote + LoadDte + quote;
                valString += Komma + quote + procNum + quote;
                if (inclHost) valString += Komma + quote + NamedCommon.dbhost + quote;
                if (inclAcct) valString += Komma + quote + NamedCommon.datAct + quote;
                if (inclFile) valString += Komma + quote + NamedCommon.u2Source + quote;
                valString += Komma + quote + inRec + quote;

                if (NamedCommon.isNRT) {
                    SqlCommands.ExecuteSQL(DDL);
                    if (NamedCommon.debugging) uCommons.uSendMessage("      > Inserting raw record.");
                    DDL.clear();
                } else {
                    if (NamedCommon.BulkLoad && NamedCommon.DatSize > 0) {
                        if ((thisSize + valString.length()) > NamedCommon.DatSize) {
                            uCommons.SQLDump(DDL);
                            thisSize = 0;
                            DDL.clear();
                        }
                        DDL.add(valString);
                        thisSize += valString.length();
                    } else {
                        String createRow = "";
                        createRow = "INSERT INTO " + dbFocus;
                        String cols = NamedCommon.rawCols;
                        cols = cols.replaceAll("\\-", "");
                        cols = cols.replaceAll("\\.", "");
                        cols = cols.replaceAll("\\*", "");
                        createRow += " (" + cols + ") VALUES (";
                        cols = "";
                        createRow += valString + ")";
                        DDL.add(createRow);
                        Ctr++;
                        if (Ctr >= NamedCommon.DatRows) {
                            uCommons.SQLDump(DDL);
                            DDL.clear();
                            if (NamedCommon.ZERROR)
                                uCommons.uSendMessage("<<FAIL>> SQLDump() See RunERRORS queue for details.");
                            TotalInserts += Ctr;
                            Ctr = 0;
                        }
                    }
                }
            }
        }

    }

    public static void CreateEncSeed() {
        encSeed = uCipher.Randomise(uCipher.keyBoard25);
    }

    public static String[] GetAllMaps() {
        String dir = "/maps";
        String ext = "map";
        String[] fnames = ReadFileNames(dir, ext);
        return fnames;
    }

    public static String[] ReadFileNames(String indir, String inext) {
        String lookin = NamedCommon.BaseCamp + indir;
        final String matchStr = inext;
        if (NamedCommon.debugging) uCommons.uSendMessage("Look in " + lookin + "  for " + matchStr);
        File dir = new File(lookin);
        File[] matchFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(matchStr);
            }
        });
        if (matchFiles == null) return null;
        int nbrmatches = matchFiles.length;
        String[] matchingFiles = new String[nbrmatches];
        for (int ff = 0; ff < nbrmatches; ff++) { matchingFiles[ff] = String.valueOf(matchFiles[ff]); }
        return matchingFiles;
    }

    private static void RemoveThisRunData() {
        // It has the LoadDte, dbhost, account, srcFile and the sqlTable
        // NamedCommon.SqlDatabase, NamedCommon.SqlSchema, NamedCommon.sqlTarget
        boolean inclHost = NamedCommon.rawCols.toLowerCase().contains("dbhost");
        boolean inclAcct = NamedCommon.rawCols.toLowerCase().contains("account");
        boolean inclFile = NamedCommon.rawCols.toLowerCase().contains("file");
        String dbTable = "", dropRows = "";

        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                dbTable = "["+NamedCommon.rawDB+"].[raw].["+NamedCommon.sqlTarget+"]";
                break;
            default:
                return;
        }
        dropRows = "DELETE FROM " + dbTable + " where ";
        if (inclHost) dropRows += "DBHost = '" + NamedCommon.dbhost     + "' and ";
        if (inclAcct) dropRows += "Account = '" + NamedCommon.datAct    + "' and ";
        if (inclFile) dropRows += "srcFile = '" + NamedCommon.u2Source  + "' and ";
        dropRows += "LoadDte = '" + NamedCommon.BatchID + "'";
        DDL.clear();
        DDL.add(dropRows);
        uCommons.uSendMessage("   .) "+ dropRows);
        SqlCommands.ExecuteSQL(DDL);
    }

}


