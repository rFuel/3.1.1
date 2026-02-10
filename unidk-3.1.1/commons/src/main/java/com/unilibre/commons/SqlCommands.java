package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniclientlibs.UniDynArray;
import com.unilibre.cipher.uCipher;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SqlCommands {

    private static final String crlf = "\n";
    private static boolean jdbcBatchMode = false;
    private static boolean showError = true;
    private static ArrayList<String> useDDL = new ArrayList<String>();
    private static ArrayList<String> ddlERR = new ArrayList<String>();
    private static ArrayList<String> sqlERR = new ArrayList<String>();

    public static void SetBatching(boolean inval) {
        // this indicates that bulk inserts are NOT used and this process will
        // create batches of insert into table commands.
        // Issue: if jdbcBatchMode is true - EVERY statement terminated by ';' is seen as a batch !
        jdbcBatchMode = inval;
    }

    public static boolean GetBatching() { return jdbcBatchMode;}

    public static void DropCreateTable() {
//        String[] tCols = (NamedCommon.rawCols + NamedCommon.tblCols).split(",");
        String useCols;
        if (NamedCommon.task.equals("012")) {
            useCols = NamedCommon.rawCols;
        } else {
            useCols= NamedCommon.tblCols;
        }
        String[] tCols = useCols.split(",");
        String dropTable = "";
        String createTable = "";
        String createIndex = "";
        String dbObj = "";
        String useSCH = NamedCommon.SqlSchema;

        if (NamedCommon.uniBase && !NamedCommon.SqlSchema.equals(NamedCommon.rawSC)) {
//            tCols = NamedCommon.tblCols.split(",");
            dbObj = "[" + NamedCommon.SqlDatabase + "].[uni";
            useSCH = "uni";
        } else {
//            tCols = NamedCommon.rawCols.split(",");
            dbObj = "[" + NamedCommon.SqlDatabase + "].[" + NamedCommon.SqlSchema;
        }
        dbObj += "].[" + NamedCommon.sqlTarget + "]";

        if (NamedCommon.streamedFiles.indexOf(dbObj) >= 0 && NamedCommon.isNRT) return;

        List<String> DDL = new ArrayList<String>();
        if (!NamedCommon.isNRT)  dropTable = DropTable(NamedCommon.SqlDatabase, useSCH, NamedCommon.sqlTarget);

        if (!dropTable.equals("")) {
            uCommons.uSendMessage("      > Dropping table " + dbObj);
            if (SqlCommands.GetBatching()) SqlCommands.SetBatching(false);
            DDL.add(dropTable);
            ExecuteSQL(DDL);
            DDL.clear();
            if (NamedCommon.ZERROR) return;
        } else {
            if (!NamedCommon.isNRT) uCommons.uSendMessage("      > NO drop table   " + dbObj);
        }

        uCommons.uSendMessage("      > Create table " + dbObj + " if not(exists)");

        createTable = CreateTable(NamedCommon.SqlDatabase, useSCH, NamedCommon.sqlTarget, tCols);
        if (!createTable.equals("")) {
            if (SqlCommands.GetBatching()) SqlCommands.SetBatching(false);
            DDL.add(createTable);
            ExecuteSQL(DDL);
            DDL.clear();
            if (NamedCommon.ZERROR) return;
        }
        if (!NamedCommon.task.equals("014") && !NamedCommon.isNRT) {
            createIndex = CreateIndex(NamedCommon.SqlDatabase, useSCH, NamedCommon.sqlTarget, tCols);
            if (SqlCommands.GetBatching()) SqlCommands.SetBatching(false);
            DDL.clear();
            DDL.add(createIndex);
            ExecuteSQL(DDL);
            DDL.clear();
            if (NamedCommon.ZERROR) return;
        }
        if (NamedCommon.isNRT) NamedCommon.streamedFiles.add(dbObj);
        DDL.clear();
        tCols = null;
        useCols = null;
        dropTable = null;
        createTable = null;
        createIndex = null;
        dbObj = null;
    }

    public static String CreateTable(String DB, String SCH, String TBL, String[] tCols) {
        String command = "";
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                command = uMSSQLCommons.CreateTable(DB, SCH, TBL, tCols);
                break;
            case "SNOWFLAKE":
                command = uSnowflakeCommons.CreateTable(DB, SCH, TBL, tCols);
                break;
            case "Splice":
                command = uSpliceCommons.CreateTable(DB, SCH, TBL, tCols);
                break;
            case "MYSQL":
                command = uSQLCommons.CreateTable(DB, SCH, TBL, tCols);
                break;
            case "ORACLE":
                command = uOracleCommons.CreateTable(DB, SCH, TBL, tCols);
                break;
        }
        return command;
    }

    public static String DropTable(String DB, String SCH, String TBL) {
        String command = "";
        if (NamedCommon.isPrt) {
            boolean isFirst = uCommons.APIGetter("firstpart").toLowerCase().equals("true");
            if (!isFirst) return command;
        }
        if (NamedCommon.isNRT) return command;
        boolean isWorkFile = TBL.toUpperCase().contains("WORKFILE");
        if (!NamedCommon.RunType.equals("REFRESH") && !isWorkFile) return command;
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                command = uMSSQLCommons.DropTable(DB, SCH, TBL);
                break;
            case "SNOWFLAKE":
                command = uSnowflakeCommons.DropTable(DB, SCH, TBL);
                break;
            case "ORACLE":
                if (TBL.startsWith(SCH.toUpperCase())) SCH = "";
                command = uOracleCommons.DropTable(DB, SCH, TBL);
                break;
            case "Splice":
                command = uSpliceCommons.DropTable(DB, SCH, TBL);
                break;
            case "MYSQL":
                command = uSQLCommons.DropTable(DB, SCH, TBL);
                break;
            case "DB2":
                command = uDB2Commons.DropTable(NamedCommon.SqlDatabase, SCH, TBL);
                break;
            default:
                command = uSQLCommons.DropTable(NamedCommon.SqlDatabase, SCH, TBL);
                break;
        }
        return command;
    }

    public static String CreateIndex(String DB, String SCH, String TBL, String[] tCols) {
        String command = "";
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                command = uMSSQLCommons.CreateIndex(DB, SCH, TBL, tCols);
                break;
            case "SNOWFLAKE":
                command = uSnowflakeCommons.CreateIndex(DB, SCH, TBL, tCols);
                break;
            case "ORACLE":
                command = uOracleCommons.CreateIndex(DB, SCH, TBL, tCols);
                break;
            case "Splice":
                command = uSpliceCommons.CreateIndex(DB, SCH, TBL, tCols);
                break;
            case "MYSQL":
                command = uSQLCommons.CreateIndex(DB, SCH, TBL, tCols);
                break;
            case "DB2":
                command = uDB2Commons.CreateIndex(DB, SCH, TBL, tCols);
                break;
            default:
                command = uSQLCommons.CreateIndex(DB, SCH, TBL, tCols);
                break;
        }
        return command;
    }

    public static String CreateView(String DB, String SCH, String TBL, UniDynArray lineArray) {
        String createView="";
        if (DB.equals(""))  DB = NamedCommon.SqlDatabase;
        if (SCH.equals("")) SCH= NamedCommon.SqlSchema;
        if (TBL.equals("")) TBL= NamedCommon.sqlTarget;
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
//                createView = uMSSQLCommons.CreateView(NamedCommon.SqlDatabase, NamedCommon.SqlSchema, NamedCommon.sqlTarget, lineArray);
                createView = uMSSQLCommons.CreateView(DB, SCH, TBL, lineArray);
                break;
            case "SNOWFLAKE":
//                createView = uSnowflakeCommons.CreateView(NamedCommon.SqlDatabase, NamedCommon.SqlSchema, NamedCommon.sqlTarget, lineArray);
                createView = uSnowflakeCommons.CreateView(DB, SCH, TBL, lineArray);
                break;
            case "ORACLE":
//                createView = uOracleCommons.CreateView(NamedCommon.SqlDatabase,NamedCommon.SqlSchema,NamedCommon.sqlTarget, lineArray);
                createView = uOracleCommons.CreateView(DB, SCH, TBL, lineArray);
                break;
            case "Splice":
//                createView = uSpliceCommons.CreateView(NamedCommon.SqlDatabase,NamedCommon.SqlSchema,NamedCommon.sqlTarget);
                break;
            case "MYSQL":
//                createView = uSQLCommons.CreateView(NamedCommon.SqlDatabase,NamedCommon.SqlSchema,NamedCommon.sqlTarget, lineArray);
                createView = uSQLCommons.CreateView(DB, SCH, TBL, lineArray);
                break;
            case "DB2":
//                createView = uDB2Commons.CreateView(NamedCommon.SqlDatabase,NamedCommon.SqlSchema,NamedCommon.sqlTarget);
//                createView = uDB2Commons.CreateView(DB, SCH, TBL, lineArray);
                break;
            default:
//                createView = uSQLCommons.CreateView(NamedCommon.SqlDatabase,NamedCommon.SqlSchema,NamedCommon.sqlTarget, lineArray);
                createView = "";
                break;
        }
        return createView;
    }

    public static String BulkImport(String fname, String Path2Data) {
        if (NamedCommon.jdbcLoader) {
            SqlCommons.JDBCloader(fname, Path2Data);
            return "";
        }
        String BulkImport = "";
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                BulkImport = uMSSQLCommons.BulkImport(fname, Path2Data);
                break;
            case "SNOWFLAKE":
                uCommons.uSendMessage("MoveRaw and MoveData MUST be handled by sfLoadData");
                uCommons.uSendMessage("Email: support@unilibre.com.au");
                NamedCommon.ZERROR=true;
                break;
            case "ORACLE":
                BulkImport = uOracleCommons.BulkImport(fname, Path2Data);
                break;
            case "Splice":
                BulkImport = uSpliceCommons.BulkImport(fname, Path2Data);
                break;
            case "MYSQL":
                BulkImport = uSQLCommons.BulkImport(fname, Path2Data);
                break;
            case "DB2":
                BulkImport = uDB2Commons.BulkImport(fname, Path2Data);
                break;
            default:
                BulkImport = uSQLCommons.BulkImport(fname, Path2Data);
                break;
        }
        return BulkImport;
    }

    public static boolean Exists(String rawTable) {
        boolean exists = false;
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                exists = uMSSQLCommons.Exists(rawTable);
                break;
            case "SNOWFLAKE":
                exists = uSnowflakeCommons.Exists(rawTable);
                break;
            case "MYSQL":
                exists = uMariaDBCommons.Exists(rawTable);
                break;
            case "ORACLE":
                exists = uOracleCommons.Exists(rawTable);
                break;
        }
        return exists;
    }

    public static void ExecuteSQL(List<String> ddl) {
        if (ddl.isEmpty()) return;
        if (NamedCommon.tHostList.size() == 0) NamedCommon.tHostList.add("base-sql");

        if (NamedCommon.tHostList.size() != ConnectionPool.objPool.size()) {
            uCommons.uSendMessage("-----------------------------------------------------------------");
            uCommons.uSendMessage("Unequal ConnectionPool arrays - reestablish JDBC connection pool.");
            uCommons.uSendMessage("        this can be caused when the connection is lost");
            uCommons.uSendMessage(".");

            int max = NamedCommon.tHostList.size();
            if (max < ConnectionPool.objPool.size()) max = ConnectionPool.objPool.size();
            for (int x=0 ; x < max ; x++) {
                if (x > ConnectionPool.objPool.size()) ConnectionPool.objPool.add(x, "");
                if (x > NamedCommon.tHostList.size())  NamedCommon.tHostList.add(x, "");
                uCommons.uSendMessage("   "+x+") "+NamedCommon.tHostList.get(x)+"  : "+ConnectionPool.objPool.get(x));
            }

            uCommons.uSendMessage("-----------------------------------------------------------------");
            SqlCommands.DisconnectSQL();
            NamedCommon.tHostList.clear();
            ConnectionPool.objPool.clear();
            uCommons.uSendMessage("Resetting run-time properties");
            Properties rProps = uCommons.LoadProperties("rFuel.properties");
            if (NamedCommon.ZERROR) System.exit(0);
            uCommons.SetCommons(rProps);
            uCommons.MessageToAPI(NamedCommon.message);         // THIS was missing - could solve the problem.
            // ------------------------------------------------------------------
            // May need more resets - see Message2Properties
            NamedCommon.datAct = APImsg.APIget("dacct");
            // ------------------------------------------------------------------
            uCommons.uSendMessage("Reconnecting...");
            int loopCnt=0;
            while (loopCnt < 10) {
                SqlCommands.ConnectSQL();
                if (NamedCommon.tConnected) break;
                loopCnt++;
            }
            if (!NamedCommon.tConnected) {
                uCommons.uSendMessage("It seems that connectivity to SQL has been perminantly lost.");
                uCommons.uSendMessage("Stopping Now.");
                SourceDB.DisconnectSourceDB();
                String stopfile = NamedCommon.BaseCamp + NamedCommon.slash
                        + "conf" + NamedCommon.slash + "STOP";
                uCommons.WriteDiskRecord(stopfile, "STOP");
                System.exit(1);
            }
        }

        if (!NamedCommon.tConnected || ConnectionPool.jdbcPool.size() == 0) {
            SqlCommands.ConnectSQL();
            if (NamedCommon.ZERROR) return;
        }

        useDDL.clear(); // = new ArrayList<String>();
        String[] sqlpart;
        String thisCmd, thisCon="", thisHost, thisDB, thisSC, thisOBJ;
        Statement exeStmt = null;
        boolean okay;
        boolean getDB, getSC;
//        boolean batching = (NamedCommon.tHostList.size() > 1);
//        boolean batching = (NamedCommon.tHostList.size() > 1 || NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE"));
        boolean okay2delete = true;

        for (int th = 0; th < ConnectionPool.jdbcPool.size(); th++) {
            if (!okay2delete) continue;
            thisCon = ConnectionPool.ConString.get(th);
            for (int ds = 0; ds < ConnectionPool.objPool.size(); ds++) {
                if (!okay2delete) continue;
                if (!ConnectionPool.objPool.get(ds).startsWith(thisCon + "+")) continue;
                try {
                    thisHost = NamedCommon.tHostList.get(ds);
                } catch (IndexOutOfBoundsException e) {
                    thisHost = "";
                    thisSC   = "";
                    uCommons.uSendMessage("Unequal ConnectionPool arrays ("+ds+") - MUST reset.");
                    SqlCommands.DisconnectSQL();
                    SqlCommands.ReconnectService();
                    ds --;
                    if (ds < 0) System.exit(1);
                }
                if (!NamedCommon.SqlSchema.equals(NamedCommon.rawSC)) {
                    thisDB = uCommons.FieldOf(ConnectionPool.objPool.get(ds), "\\+", 2);
                    thisSC = uCommons.FieldOf(ConnectionPool.objPool.get(ds), "\\+", 3);
                } else {
                    thisDB = NamedCommon.SqlDatabase;
                    thisSC = NamedCommon.SqlSchema;
                }

                if (thisDB.equals("")) thisDB = APImsg.APIget("sqldb:" + NamedCommon.tHostList.get(ds));
                if (thisSC.equals("")) thisSC = APImsg.APIget("schema:" + NamedCommon.tHostList.get(ds));

                if (!thisDB.equals(APImsg.APIget("sqldb"))  && !APImsg.APIget("sqldb").equals(""))  thisDB = APImsg.APIget("sqldb");
                if (!thisSC.equals(APImsg.APIget("schema")) && !APImsg.APIget("schema").equals("")) thisSC = APImsg.APIget("schema");

                NamedCommon.uCon = ConnectionPool.getConnection(thisCon);
                getDB = (NamedCommon.SqlDatabase.startsWith("$") || NamedCommon.SqlDatabase.startsWith("~"));
                getSC = (NamedCommon.SqlSchema.startsWith("$") || NamedCommon.SqlSchema.startsWith("~"));

                if (getDB || getSC) {
                    uCommons.GetThostDetails(thisHost);

                    if (getDB) NamedCommon.SqlDatabase = APImsg.APIget("sqldb:" + thisHost);
                    if (NamedCommon.SqlDatabase.equals("$DB$")) NamedCommon.SqlDatabase = "";

                    if (getSC) NamedCommon.SqlSchema = APImsg.APIget("schema:" + thisHost);
                    if (NamedCommon.SqlSchema.equals("$SC$")) NamedCommon.SqlSchema = "";

                    if (NamedCommon.SqlDatabase.equals("")) NamedCommon.SqlDatabase = APImsg.APIget("sqldb");
                    if (NamedCommon.SqlSchema.equals("")) NamedCommon.SqlSchema = APImsg.APIget("schema");
                    if (NamedCommon.SqlDatabase.equals("")) NamedCommon.SqlDatabase = NamedCommon.rawDB;
                    if (NamedCommon.SqlSchema.equals("")) NamedCommon.SqlSchema = NamedCommon.rawSC;

                    if (NamedCommon.SqlSchema.equals(NamedCommon.rawSC)) NamedCommon.SqlDatabase = NamedCommon.rawDB;

                    thisDB = NamedCommon.SqlDatabase;
                    thisSC = NamedCommon.SqlSchema;
                }

                if (thisDB.equals("")) thisDB = NamedCommon.rawDB;
                if (thisSC.equals("")) thisSC = NamedCommon.rawSC;
                thisOBJ = thisDB + "." + thisSC;

                while (true) {
                    try {
                        if (exeStmt != null) exeStmt.close();
                        break;
                    } catch (SQLException e) {
                        uCommons.uSendMessage(e.getMessage());
                        ReconnectService();
                        return;
                    }
                }
                exeStmt = null;
                if (!jdbcBatchMode) {
                    // not jdbcBatchMode because we're about to do a single BULK INSERT command
                    okay = false;
                    int tries = 0;
                    while (!okay) {
                        NamedCommon.ZERROR = false;
                        try {
                            exeStmt = NamedCommon.uCon.createStatement();
                            okay = true;
                        } catch (SQLException e) {
                            uCommons.uSendMessage(e.getMessage());
                            ReconnectService();
                        }
                    }

                    if (NamedCommon.ZERROR) {
                        uCommons.uSendMessage("<<FAIL>> TargetDB is unavailable");
                        return;
                    }

                    String cmdPart;
                    for (String incmd : ddl) {
                        // commented out 19-12-2025 as being unnecessary - watch for CREATE / DROP / TRUNCATE commands created by rFuel.
                        // sqlpart = incmd.split("\\r?\\n");
                        // this is replacement code: --------------------------------
                        sqlpart = new String[1];
                        sqlpart[0] = incmd;
                        // ----------------------------------------------------------
                        okay2delete = true;
                        //
                        // batching is confusing:
                        // originally, it was intended to make the execute method do the same thing to MANY target hosts.
                        // then along cam SNOWFLAKE and I tagged it onto the batching - not sure why. Need to test again.
                        // EVEN if there is more than 1 target, I don't remember how to multiplex the commands here.
                        // it seems strange to handle it this low down in the code. SNOWFLAKE may only need jdbcBatchMode
                        // ------------------------------------------------------------------------------------------------
                        boolean batching = (NamedCommon.tHostList.size() > 1 || NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE"));
                        int resp=0;
                        useDDL.clear();
                        for (int pp = 0; pp < sqlpart.length; pp++) {
                            if (okay2delete) {
                                thisCmd = sqlpart[pp];
                                if (!thisCmd.trim().equals("") && okay2delete) {
                                    useDDL.add(thisCmd);
                                    if (!batching) {
                                        while (true) {
                                            try {
                                                NamedCommon.SqlReply = "";
                                                NamedCommon.uCon.setAutoCommit(false);
                                                if (NamedCommon.debugging) System.out.println("-DBG- exec: " + thisCmd);
                                                resp = exeStmt.executeUpdate(thisCmd);
                                                if (NamedCommon.debugging) System.out.println("-DBG- exec: done");
                                                NamedCommon.uCon.commit();
                                                if (NamedCommon.debugging) System.out.println("-DBG- exec: committed");
                                                NamedCommon.SqlReply = String.valueOf(NamedCommon.uCon.getWarnings());
                                                if (NamedCommon.SqlReply != "null") {
                                                    System.out.println(thisCmd);
                                                    System.out.println(NamedCommon.SqlReply);
                                                }
                                                break;
                                            } catch (SQLException e) {
                                                int errCode = e.getErrorCode();
                                                if (errCode == 1205) {
                                                    // deadlock handler ------
                                                    uCommons.uSendMessage("Deadlock manager invoked. Retrying command.");
                                                    uCommons.Sleep(2);
                                                    pp--;              // keep re-sending the same command until no deadlock
                                                    break;
                                                } else {
                                                    if (String.valueOf(errCode).startsWith("08")) {
                                                        try {
                                                            ReconnectService();
                                                            NamedCommon.uCon = ConnectionPool.getConnection(NamedCommon.jdbcCon);
                                                            exeStmt = NamedCommon.uCon.createStatement();
                                                        } catch (SQLException ee) {
                                                            uCommons.uSendMessage("ERROR: Unrecoverable SQL error: " + ee.getMessage());
                                                            System.exit(1);
                                                        }
                                                    } else {
                                                        ddlERR.add(thisCmd);
                                                        sqlERR.add(e.getMessage());
                                                        okay2delete = false;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (exeStmt != null) {
                    while (true) {
                        try {
                            exeStmt.close();
                            break;
                        } catch (SQLException e) {
                            uCommons.uSendMessage("SQL ERR:: " + e.getMessage());
                            if (e.getSQLState().startsWith("08")) SqlCommands.ReconnectService();
                        }
                    }
                    exeStmt = null;
                }

                if (okay2delete && jdbcBatchMode) {
                    while (true) {
                        try {
//                            if (NamedCommon.SqlDBJar.equals("ORACLE")) {
                                NamedCommon.uCon.setAutoCommit(false);
//                            } else {
//                                NamedCommon.uCon.setAutoCommit(true);
//                            }

                            SQLWarning SQLerrors;
                            exeStmt = NamedCommon.uCon.createStatement();
                            int cmdCtr = 0;
                            // load the batch
                            for (String incmd : ddl) {
                                // is there more than 1 command in the line?
                                incmd = incmd.replaceAll("\r", "");
                                if (incmd.indexOf(crlf) > 0) {
                                    sqlpart = incmd.split(crlf);
                                    for (String cmd : sqlpart) {
                                        exeStmt.addBatch(cmd);
                                        cmdCtr++;
                                    }
                                } else {
                                    exeStmt.addBatch(incmd);
                                    cmdCtr++;
                                }
                                // execute commands in batches of 200 commands.
                                if (cmdCtr > 200) {
                                    exeStmt.executeBatch();
                                    NamedCommon.uCon.commit();
                                    exeStmt.clearBatch();
                                    cmdCtr = 0;
                                }
                            }
                            //
                            // execute remainer of the batch.
                            //
                            if (cmdCtr > 0) {
                                exeStmt.executeBatch();
                                NamedCommon.uCon.commit();
                                exeStmt.clearBatch();
                                cmdCtr=0;
                            }
                            SQLerrors = exeStmt.getWarnings();
                            exeStmt.close();
                            exeStmt = null;
                            if (SQLerrors != null && !SQLerrors.getMessage().contains("Changed")) {
                                String emsg = String.valueOf(SQLerrors.getMessage()); // .getNextWarning());
                                NamedCommon.SqlReply += "\n " + emsg;
                                if (NamedCommon.BulkLoad) okay2delete = false;
                            }
                            useDDL.clear();
                            break;
                        } catch (SQLException e) {
                            uCommons.uSendMessage("SQL ERROR: " + e.getMessage());
                            if (e.getSQLState().startsWith("08")) {
                                SqlCommands.ReconnectService();
                                NamedCommon.uCon = ConnectionPool.getConnection(thisCon);
                            }
                        }
                        useDDL.clear();
                    }
                }

                if (getDB) NamedCommon.SqlDatabase = "$DB$";
                if (getSC) NamedCommon.SqlSchema = "$SC$";
            }
        }
        sqlpart = null;

        if (!okay2delete || !ddlERR.isEmpty()) {
            if (NamedCommon.BulkLoad) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "<<FAIL>> " + NamedCommon.SqlReply;
            }
            System.out.println("..........................................................");
            System.out.println("Using " + thisCon);
            if (!ddlERR.isEmpty()) {
                System.out.println("SQL Commands Executed ::");
                int cmdCt = 1;
                for (int e=0 ; e < useDDL.size(); e++) {
                    System.out.println(cmdCt + ">  " + ddlERR.get(e));
                    System.out.println(cmdCt + ".. " + sqlERR.get(e));
                    cmdCt++;
                }
                System.out.println("..........................................................");
            }
            if (NamedCommon.BulkLoad) okay2delete = false;
            NamedCommon.StopNow = "<<FAIL>>";
        } else {
            NamedCommon.StopNow = "<<PASS>>";
        }
        if (!NamedCommon.ZERROR) {
            if (okay2delete) {
                NamedCommon.ZERROR = false;
                NamedCommon.Zmessage = "";
            } else {
                NamedCommon.StopNow = "<<FAIL>>";
            }
        } else {
            NamedCommon.StopNow = "<<FAIL>>";
        }
    }

    public static String SelectWorkfiles() {
        String cmd = "";
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                cmd = "";
                if (!NamedCommon.SqlDatabase.equals(NamedCommon.rawDB)) cmd += "use " + NamedCommon.rawDB + ";\n ";
                cmd += "select t.name as TBL FROM  sys.tables  t \n"
                        + "   JOIN  sys.schemas s ON t.[schema_id] = s.[schema_id] \n"
                        + "   WHERE t.type = 'U' and s.[name]='upl' \n"
                        + "     AND t.name like '%workfile%'";
                break;
            case "SNOWFLAKE":
                cmd = "SELECT TABLE_NAME FROM INFORMATION.SCHEMA.TABLES " +
                        " WHERE TABLE_SCHEMA = '" + NamedCommon.rawDB.toUpperCase() + "'" +
                        " AND TABLE_NAME LIKE '%WORKFILE'\n";
                break;
            case "ORACLE":
                cmd = "select TABLE_NAME AS TBL from USER_TABLES where TABLE_NAME LIKE '%WORKFILE%'";
                break;
            case "Splice":
                break;
            case "MYSQL":
                break;
            case "DB2":
                break;
            default:
                break;
        }
        if (cmd.equals("")) {
            uCommons.uSendMessage("***********************************");
            uCommons.uSendMessage("Select and drop workfiles manually.");
            uCommons.uSendMessage("***********************************");
        }
        return cmd;
    }

    public static boolean RemoveWorkfiles() {
        boolean success = true;
        if (!NamedCommon.tConnected) {
            SqlCommands.ConnectSQL();
            if (!NamedCommon.tConnected) uCommons.uSendMessage("NOT Connected !!");
            if (NamedCommon.uCon == null) {
                String url = NamedCommon.jdbcCon.split("\\;")[0];
                String jdbcDBI = APImsg.APIget("jdbcDbi:base-sql");
                if (!jdbcDBI.equals("")) {
                    if (!url.endsWith(";")) url += ";";
                    url += "databaseName=" + jdbcDBI;
                    if (!url.endsWith(";")) url += ";";     // in case Dbi is just a name - common
                }
                if (!NamedCommon.jdbcAdd.equals("")) {
                    if (!url.endsWith(NamedCommon.jdbcSep)) url += NamedCommon.jdbcSep;
                    url += NamedCommon.jdbcAdd;
                    if (!url.endsWith(NamedCommon.jdbcSep)) url += NamedCommon.jdbcSep;
                }
                NamedCommon.uCon = ConnectionPool.getConnection(url);
                if (NamedCommon.uCon == null) {
                    uCommons.uSendMessage("* --------------------------------- ");
                    uCommons.uSendMessage("CANNOT find the jdbc connection for "+url);
                    uCommons.uSendMessage("* --------------------------------- ");
                    uCommons.uSendMessage("Looked in: ");
                    for (int c=0 ; c < ConnectionPool.ConString.size(); c++) {
                        uCommons.uSendMessage("   >. " + ConnectionPool.ConString.get(c));
                    }
                    uCommons.uSendMessage("* --------------------------------- ");
                    success = false;
                }
            }
            if (NamedCommon.uCon != null && !NamedCommon.rawDB.equals("")) {
                try {
                    Statement stmt = NamedCommon.uCon.createStatement();
                    String db = NamedCommon.rawDB.trim();
                    if (!db.equals("") && !db.startsWith("$")) {
                        String tmp = SqlCommands.SelectWorkfiles();
                        String dcmd = "";
                        ResultSet rs = stmt.executeQuery(tmp);

                        ArrayList<String> DDL = new ArrayList<String>();
                        DDL.add(" ");
                        DDL.clear();
                        String store = NamedCommon.RunType;
                        NamedCommon.RunType = "REFRESH";
                        NamedCommon.isNRT = false;
                        NamedCommon.isPrt = false;
                        while (rs.next()) {
                            tmp = rs.getString("TBL");
                            if (tmp.toLowerCase().contains("workfile")) {
                                dcmd = SqlCommands.DropTable(NamedCommon.rawDB, "upl", tmp);
                                DDL.add(dcmd);
                            }
                        }
                        rs.close();
                        rs = null;
                        NamedCommon.RunType = store;
                        tmp = "";
                        if (!DDL.isEmpty()) {
                            SqlCommands.ExecuteSQL(DDL);
                            if (NamedCommon.ZERROR) return false;
                        }
                    } else {
                        uCommons.uSendMessage("Unkown TargetDB \"raw\" connector - cannot cleanup workfiles");
                    }
                    stmt.close();
                    stmt = null;
                } catch (SQLException e) {
                    uCommons.uSendMessage("Cannot create Statement() " + e.getMessage());
                    success = false;
                }
            }
        }
        return success;
    }

    public static String[] ResetCommand(String[] inCmds, String thisHost) {
        String[] outCmds = new String[inCmds.length];
        String cLine, cDB, cSch, thisCMD, thisDB, thisSC;
        int modLvl = 0;
        for (int c = 0; c < inCmds.length; c++) {
            cLine = inCmds[c];
            modLvl = 0;
            if (cLine.contains(".[") && cLine.contains("].")) modLvl = 1;
            if (cLine.contains("$DB$") || cLine.contains("$SC$")) modLvl = 2;
            if (modLvl < 1) {
                outCmds[c] = cLine;
                continue;
            }

            switch (modLvl) {
                case 1:
                    cDB = uCommons.FieldOf(cLine, "\\[", 2);
                    cDB = uCommons.FieldOf(cDB, "\\]", 1);
                    cSch = uCommons.FieldOf(cLine, "\\[", 3);
                    cSch = uCommons.FieldOf(cSch, "\\]", 1);
                    if (NamedCommon.tHostList.size() > 1) {
                        cLine = cLine.replace(cDB, "$DB$");
                        cLine = cLine.replace(cSch, "$SC$");
                    }
                    break;
                case 2:
                    cDB = NamedCommon.SqlDatabase;
                    cSch = NamedCommon.SqlSchema;
                    break;
                default:
                    return inCmds;
            }

//            thisDB = APImsg.APIget("sqldb:" + thisHost);
//            if (thisDB.equals("")) thisDB = cDB;
            thisDB = cDB;
//            if (cSch.equals("upl") || cSch.equals(NamedCommon.rawSC)) {
            thisSC = cSch;
//            } else {
//                thisSC = APImsg.APIget("schema:" + thisHost);
//                if (thisSC.equals("")) thisSC = cSch;
//            }

            thisCMD = cLine;
            if (!thisDB.equals("")) {
                while (thisCMD.contains("$DB$")) {
                    thisCMD = thisCMD.replace("$DB$", thisDB);
                }
            }
            if (!thisSC.equals("")) {
                while (thisCMD.contains("$SC$")) {
                    thisCMD = thisCMD.replace("$SC$", thisSC);
                }
            }
            outCmds[c] = thisCMD;
        }
        return outCmds;
    }

    public static void ShowCmd(String cmd) {
        String t1 = new String();
        String t2 = new String();
        int st = 0, f, t;
        int lx = 70;
        if (lx > cmd.length()) {
            lx = cmd.length();
        }
        t1 = cmd.substring(0, lx);
        while (t1.length() >= 1) {
            cmd = cmd.substring(t1.length(), cmd.length());
            if (st > 0) {
                t2 = "     " + t1;
            } else {
                t2 = t1;
            }
            st++;
            f = 0;
            t = (lx - 5);
            if (t > cmd.length()) {
                t = cmd.length();
            }
            t1 = cmd.substring(f, t);
        }
    }

    public static void SetJdbcParts(String[] jdbcParts, boolean doEnc) {
        String line, key, val;
        boolean encVal;
        for (int p = 0; p < jdbcParts.length; p++) {
            encVal = false;
            line = jdbcParts[p];
            if (!NamedCommon.isNRT && !NamedCommon.runSilent && NamedCommon.debugging) uCommons.uSendMessage(">>> [" + p + "]  " + line);
            if (line.contains("?")) {
                String[] sLine = line.split("\\?");
                uCommons.uSendMessage("  > ConnectSQL() is splitting line (?) " + sLine[0]);
                if (sLine[0].startsWith("jdbc:")) APImsg.APIset("jdbccon:base-sql", sLine[0]);
                line = sLine[1];
                sLine = null;
            }
            if (line.contains("=ENC(")) {
                int lx = line.indexOf("=ENC(");
                String tmp = line.substring(lx + 5, (line.length() - 1));
                line = line.substring(0, lx + 1) + uCipher.Decrypt(tmp);
            }
            if (!line.contains("=")) {
                key = "jdbccon";
                val = line;
            } else {
                String[] lparts = line.split("\\=");
                key = lparts[0];
                switch (key) {
                    case "user":
                        key = "jdbcUsr";
                        if (doEnc) encVal = true;
                        break;
                    case "password":
                        key = "jdbcPwd";
                        if (doEnc) encVal = true;
                        break;
                    case "db":
                        key = "sqldb";
                        break;
                    case "databaseName":
                        key = "jdbcDbi";
                        break;
                }
                if (lparts.length > 0) {
                    val = lparts[1];
                } else {
                    val = APImsg.APIget(key);
                }
                if (key.equals("jdbcUsr") && !NamedCommon.jdbcRealm.equals(""))  {
                    if (NamedCommon.jdbcRealm.startsWith("@")) {
                        val = val + NamedCommon.jdbcRealm;
                    } else {
                        val = val + "@" + NamedCommon.jdbcRealm;
                    }
                }
            }
            if (encVal) val = "ENC("+uCipher.Encrypt(val)+")";
            APImsg.APIset(key + ":base-sql", val);
        }
    }

    public static void ReleaseSQL() {
        if (NamedCommon.jdbcCon.equals("")) return;
        try {
            NamedCommon.uCon.close();
        } catch (SQLException e) {
            uCommons.eMessage = "TargetDB: Disconnect failure";
            uCommons.eMessage += "\n" + e.getMessage();
            NamedCommon.Zmessage = uCommons.eMessage;
            uCommons.uSendMessage(uCommons.eMessage);
            NamedCommon.ZERROR = true;
            NamedCommon.tConnected = false;
        }
        NamedCommon.tConnected = false;
    }

    public static void ReconnectService() {
        NamedCommon.cERROR = true;
        System.out.println(" ");
        System.out.println(" ");
        uCommons.uSendMessage("********************************************************************");
        uCommons.uSendMessage("Connection with TargetDB has been lost. Resilience mode: restarting ");
        uCommons.uSendMessage("jdbc:  " + NamedCommon.jdbcCon);

        uCommons.uSendMessage("tHostList: " + NamedCommon.tHostList.size());
        uCommons.uSendMessage("  objPool: " + ConnectionPool.objPool.size());
        uCommons.uSendMessage(" jdbcPool: " + ConnectionPool.jdbcPool.size());

        int backoff = 0;
        NamedCommon.tConnected = false;
        while (true) {
            NamedCommon.ZERROR = false;
            NamedCommon.runSilent = true;
            backoff += 5;
            uCommons.uSendMessage("wait "+uCommons.RightHash(String.valueOf(backoff), 3)+" seconds before re-trying");
            uCommons.Sleep(backoff);
            DisconnectSQL();
            if (!NamedCommon.ZERROR) ConnectSQL();
            if (NamedCommon.tConnected) break;
            if (backoff >= 30) backoff = 0;
        }
        uCommons.uSendMessage("Connection with TargetDB has been re-established.");
        uCommons.uSendMessage("********************************************************************");
        System.out.println(" ");
        System.out.println(" ");
        NamedCommon.cERROR = false;
    }

    public static boolean ConnectSQL() {
        if (NamedCommon.tConnected) return true;
        if (NamedCommon.datOnly) return true;
        if (NamedCommon.isRest) return false;
        if (!NamedCommon.jdbcCon.startsWith("jdbc")) {
            if (!NamedCommon.runSilent) uCommons.uSendMessage(">>> jdbc connection string not ready for use. Rebuilding now:");
            NamedCommon.ZERROR = false;
            Properties runProps = uCommons.LoadProperties("rFuel.properties");
            if (NamedCommon.ZERROR) System.exit(1);
            uCommons.MakeJdbcCon(runProps);
            String jdbcUsr = uCommons.GetValue(runProps, "jdbcUsr", "");
            String jdbcPwd = uCommons.GetValue(runProps, "jdbcPwd", "");
            if (!jdbcUsr.equals("")) {
                if (NamedCommon.jdbcCon.indexOf("user=") < 0) {
                    NamedCommon.jdbcCon += "user=" + jdbcUsr + NamedCommon.jdbcSep;
                }
            }
            if (!jdbcPwd.equals("")) {
                if (NamedCommon.jdbcCon.indexOf("password=") < 0) {
                    NamedCommon.jdbcCon += "password=" + jdbcPwd + NamedCommon.jdbcSep;
                }
            }
            NamedCommon.jdbcCon = NamedCommon.jdbcCon.replace(NamedCommon.jdbcSep+NamedCommon.jdbcSep, NamedCommon.jdbcSep);
        }
        if (!NamedCommon.runSilent) uCommons.uSendMessage("TargetDB: connecting to " + NamedCommon.SqlDBJar);
        if (APImsg.GetMsgSize() < 1) APImsg.instantiate();

        if (!NamedCommon.isNRT && !NamedCommon.runSilent) uCommons.uSendMessage(">>> Using TargetHost: rFuel.properties");
        String[] jdbcParts = NamedCommon.jdbcCon.split(NamedCommon.jdbcSep);
        String url, usr, pwd;

        if (!NamedCommon.isNRT && !NamedCommon.runSilent) uCommons.uSendMessage(">>> De-constructing jdbc components");
        SetJdbcParts(jdbcParts, NamedCommon.EncBaseSql);
        if (!NamedCommon.isNRT && !NamedCommon.runSilent) uCommons.uSendMessage(">>> Done.");

        url = APImsg.APIget("jdbccon:base-sql");
        usr = APImsg.APIget("jdbcUsr:base-sql");
        pwd = APImsg.APIget("jdbcPwd:base-sql");
        String jdbcDBI = APImsg.APIget("jdbcDbi:base-sql");
        String jdbcSCH = APImsg.APIget("schema:base-sql");

        if (!jdbcDBI.equals("")) {
            if (!url.endsWith(";")) url += ";";
            url += "databaseName=" + jdbcDBI;
            if (!url.endsWith(";")) url += ";";     // in case Dbi is just a name - common.
        }

        if (!NamedCommon.jdbcAdd.equals("")) {
            if (!url.endsWith(";")) url += ";";
            url += NamedCommon.jdbcAdd;
            if (!url.endsWith(";")) url += ";";     // just in case
        }

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("URL  " + url);
            uCommons.uSendMessage("USR  " + usr);
            uCommons.uSendMessage("PWD  ***************");
        }

        if (NamedCommon.tHostList.indexOf("base-sql") < 0) {
            try {
                boolean addit=true;
                ConnectionPool.AddToPool(url, usr, pwd);
                if (!NamedCommon.ZERROR) {
                    if (NamedCommon.tHostList.indexOf("base-sql") < 0) NamedCommon.tHostList.add("base-sql");
                    String chk = url + "+" + jdbcDBI + "+" + jdbcSCH;
                    if (ConnectionPool.objPool.indexOf(chk) < 0 ) {
                        if (!jdbcDBI.equals("")) {
                            // first time through, it won't have a DBI or SCH
                            // second time round, it will have so the indexOf will always fail
                            chk = url + "++";
                            if (ConnectionPool.objPool.indexOf(chk) < 0) addit = true; else addit=false;
                        }
                        if (addit) {
                            // this is the first time through
                            ConnectionPool.objPool.add(chk);
                        } else {
                            // this is the next time so reset with what is known.
                            chk = url + "+" + jdbcDBI + "+" + jdbcSCH;
                            ConnectionPool.objPool.set(ConnectionPool.objPool.indexOf(chk), chk);
                        }
                    }
                    NamedCommon.runSilent = false;
                }
            } catch (SQLException e) {
                if (!NamedCommon.runSilent) uCommons.uSendMessage(e.getMessage());
            }
        }

        NamedCommon.tConnected = (!NamedCommon.ZERROR);
        return NamedCommon.tConnected;
    }

    public static void DisconnectSQL() {
        int th = 0;
        while (th < ConnectionPool.ConString.size()) {
            if (NamedCommon.jdbcCon.contains(ConnectionPool.ConString.get(th))) {
//                if (!NamedCommon.runSilent) uCommons.uSendMessage("TargetDB: Disconnecting " + ConnectionPool.ConString.get(th));
                ConnectionPool.releaseConnection(ConnectionPool.ConString.get(th));
                // ConString and jdbcPool are removed in releaseConnection !!!
                NamedCommon.tHostList.remove(th);
                ConnectionPool.objPool.remove(th);
            } else {
                th++;
            }
        }
        NamedCommon.jdbcCon = "";
        NamedCommon.uCon = null;
        NamedCommon.tConnected = false;
    }

    public static String TruncateTable(String DB, String SCH, String file) {
        String command = "";
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                command =  "TRUNCATE TABLE [" + DB + "].[" + SCH + "].[" + file + "]";
                break;
            case "SNOWFLAKE":
                command = "TRUNCATE TABLE IF EXISTS " + SCH + "." + file + ";";
                break;
            case "ORACLE":
                command =  "TRUNCATE TABLE " + DB + "." + SCH + "_" + file;
                break;
            case "Splice":
                command =  "TRUNCATE TABLE " + DB + "." + SCH + "_" + file;
                break;
            case "MYSQL":
                command =  "TRUNCATE TABLE " + DB + "." + SCH + "_" + file;
                break;
            case "DB2":
                command =  "TRUNCATE TABLE " + DB + "." + SCH + "_" + file;
                break;
            default:
                command =  "TRUNCATE TABLE " + DB + "." + SCH + "_" + file;
                break;
        }
        return command;
    }

}
