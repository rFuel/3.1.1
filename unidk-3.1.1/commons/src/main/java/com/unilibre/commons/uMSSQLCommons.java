package com.unilibre.commons;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

// -------------- NOTES ------------------------------------
// If the rFuel customer (kiwibank) uses Kerberos :
//      Kerberos demands a Realm with the username
//      Kerberos will NOT accept the AD domain name
//  eg  username@domain.name.purpose.etc
//
//      SQL Server will NOT accept the Realm as the username for DB login
//      SQL Server demands the AD domain
//  eg  DOMAIN\\username
//
//  ==> MUST keep the 3 components separate and use purposefully.
//      jdbcAuth    : The AD name for authorization
//      jdbcUsr     : simply - the username
//      jdbcRealm   : The Kerberos authentication realm


import asjava.uniclientlibs.UniDynArray;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class uMSSQLCommons {

    public static String CreateTable(String DB, String SCH, String TBL, String[] tCols) {
        boolean useUse = CheckNamedDB();

        tCols = SqlCommons.CheckCols(tCols);
        TBL = SqlCommons.CheckTBL(TBL);
        SCH = SqlCommons.CheckSCH(SCH);

        int nbrcols = tCols.length;
        String[] tcBCK = new String[nbrcols];
        if (SCH.equals("upl") || NamedCommon.isNRT) {
            for (int tc = 0; tc < nbrcols; tc++) { tcBCK[tc] = tCols[tc]; }
        }

        String cmd = "";

        String maxtype = SqlCommons.MaxCol(SCH);
        String inttype = SqlCommons.IntCol(SCH);
        String smltype = SqlCommons.SmlCol(SCH);
        String keytype = SqlCommons.KeyCol(SCH);
        String dattype = SqlCommons.DatCol(SCH);

        String dbFocus;
        dbFocus = "[" + DB + "].[" + SCH + "].[" + TBL + "]";
        String tmpStr = "";

        // ------------------------------------------------------------------
        String jdbcHost = APImsg.APIget("THOST");
        if (jdbcHost.equals("")) jdbcHost = "base-sql";

        String jdbcUser = NamedCommon.jdbcUser;
        if (!NamedCommon.jdbcAuth.equals(""))   jdbcUser = NamedCommon.jdbcAuth + jdbcUser;

        if (useUse) tmpStr += "use " + DB + " ";
        tmpStr += "if not exists (" +
                "SELECT SCHEMA_ID FROM sys.schemas WHERE [name] = N'"
                + SCH + "') begin exec('CREATE SCHEMA ["
                + SCH + "] AUTHORIZATION [" + jdbcUser+ "]') end;\n";

        if (useUse) tmpStr += "use " + DB + " ";
        tmpStr += "if object_id('" + dbFocus + "', 'U') is null ";
        tmpStr += "BEGIN CREATE TABLE " + dbFocus + " ( ";

        String chr, chr2, wrk, wrkSav;
        for (int xx = 0; xx < tCols.length; xx++) {
            if (tCols[xx].length() > 1) {
                /* --------------------------------------------------------- */
                wrk = tCols[xx];
                wrkSav = wrk;
                if (wrk.contains("NestPosition")) {
                    wrk = "NestPosition";
                    wrkSav = wrk;
                }
                chr = wrk.substring(0, 1);
                chr2= wrk.substring(0, 2);
                if (wrk.contains("ID") && !wrk.equals("uID")) chr = "-";
                if (wrk.startsWith(chr)) wrk = wrk.substring(1, (wrk.length()));
                switch (chr) {
                    case "*":
                        tmpStr += " " + wrk + maxtype + ", ";
                        break;
                    case "-":
                        tmpStr += " " + wrk + keytype + ", ";
                        break;
                    case ".":
                        if (chr2.equals("..")) {
                            wrk = wrk.substring(1, wrk.length());
                            tmpStr += " " + wrk +" INT IDENTITY(1,1), ";
                        } else {
                            tmpStr += " " + wrk + smltype + ", ";
                        }
                        break;
                    case "@":
                        tmpStr += " " + wrk + inttype + ", ";
                        break;
                    default:
                        if (!wrkSav.equals("NestPosition")) {
                            tmpStr += " " + wrkSav + dattype + ", ";
                        } else {
                            if (NamedCommon.AMS.toLowerCase().equals("split")) {
                                tmpStr += " CT" + smltype + ", MV" + smltype + ", SV" + smltype + ", ";
                            } else {
                                tmpStr += " " + wrkSav + smltype + ", ";
                            }
                        }
                }
                /* --------------------------------------------------------- */
            }
        }

        while (!tmpStr.endsWith(",")) { tmpStr = tmpStr.substring(0, (tmpStr.length() - 1)); }

        tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        tmpStr += " ); END;";
        if (NamedCommon.PartitionTables) {
            tmpStr += " ON " + NamedCommon.PartScheme +" (ProcNum)";
        }
        tmpStr += "\n";

        cmd = tmpStr;

        // add an rFuel.properties item to allow in flight setting of create index for normal runs.
        if (SCH.equals("upl") || NamedCommon.isNRT) {
            SCH = "*" + SCH;
            cmd += CreateIndex(DB, SCH, TBL, tcBCK);
        }
        tcBCK = null;
        return cmd;
    }

    public static String CreateIndex(String DB, String SCH, String TBL, String[] tCols) {
        boolean useUse = CheckNamedDB();

        if (SCH.equals("upl")) return "";
        if (SCH.startsWith("*")) SCH = SCH.substring(1, SCH.length());
        if (NamedCommon.allowDups) return "";
        TBL = SqlCommons.CheckTBL(TBL);
        String pKeys = "";
        String Komma = "", cmd = "", wrk;
        String dbFocus = "[" + DB + "].[" + SCH + "].[" + TBL + "]";
        String tblidx = "ux_" + DB + "_" + SCH + "_" + TBL;
        if (NamedCommon.AMS.toLowerCase().equals("split")) {
            String[] newTcols = new String[tCols.length+2];
            String pfx = "";
            int lastPos=0;
            for (int xx = 0; xx < tCols.length; xx++) {
                if (tCols[xx].contains("NestPosition")) {
                    pfx = tCols[xx].replace("NestPosition", "");
                    newTcols[lastPos] = pfx+"CT"; lastPos++;
                    newTcols[lastPos] = pfx+"MV"; lastPos++;
                    newTcols[lastPos] = pfx+"SV"; lastPos++;
                } else {
                    newTcols[lastPos] = tCols[xx]; lastPos++;
                }
            }
            tCols = null;
            tCols = new String[newTcols.length];
            for (int xx=0; xx<newTcols.length; xx++) { tCols[xx] = newTcols[xx]; }
            newTcols = null;
        }

        for (int xx = 0; xx < tCols.length; xx++) {
            wrk = tCols[xx];
            if (wrk == null) continue;
            if (wrk.length() > 1) {
                /* --------------------------------------------------------- */
                // Andy test @ Kiwibank - NO indicies if possible.
                // if ((wrk.startsWith("-") || (wrk.contains("ID")) && wrk.contains(TBL))) {
                if (wrk.startsWith("-")) {
                    // column names are in rFuel.properties rawcols and tblcols
                    if (wrk.startsWith("-")) wrk = wrk.substring(1, (wrk.length()));
                    if (!pKeys.contains(wrk)) pKeys += Komma + wrk;
                    Komma = ",";
                }
                /* --------------------------------------------------------- */
            }
        }

        while (pKeys.endsWith(",")) {
            pKeys = pKeys.substring(0, (pKeys.length() - 1));
        }

        if (SCH.equals("upl")) {
            return "";
        } else {
            if (useUse) cmd = "USE "+DB+" ";
            cmd += "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'$$uxName$$' AND object_id = OBJECT_ID(N'$$dbFocus$$')) " +
                    "BEGIN EXEC('" +
                    "CREATE UNIQUE INDEX $$uxName$$ " +
                    "ON $$dbFocus$$ ($$pKeys$$) " +
                    "WITH (IGNORE_DUP_KEY = ON)'); " +
                    "END";
            if (pKeys.equals("")) pKeys = "uID,MD5";
            if (!pKeys.contains("uID")) pKeys = "uID," + pKeys;

            while (cmd.contains("$$uxName$$")) { cmd = cmd.replace("$$uxName$$", tblidx); }
            while (cmd.contains("$$dbFocus$$")) { cmd = cmd.replace("$$dbFocus$$", dbFocus); }
            while (cmd.contains("$$pKeys$$")) { cmd = cmd.replace("$$pKeys$$", pKeys); }
        }

        return cmd;
    }

    public static String DropTable(String DB, String SCH, String TBL) {
        boolean useUse = CheckNamedDB();

        String cmd = "";
        TBL = SqlCommons.CheckTBL(TBL);
        if (NamedCommon.uniBase && NamedCommon.task.equals("014")) {
            if (!SCH.equals("upl")) SCH = "uni";
        }
        String dbfocus = "[" + DB + "].[" + SCH + "].[" + TBL + "]";
        // -------------------------------------------------------------------------
        // if customer sets dropit to false, they can create and partition tables
        // manually. This is the point of ProcNum. So the mustdrop flag needs to
        // not be used. However, if they set dropit to false and do not create
        // the tables, rFuel must check for table existence before trucates.
        // -------------------------------------------------------------------------
        if (NamedCommon.RunType.equals("PART")) NamedCommon.DropIt = false;
        if (NamedCommon.DropIt) {
            String idx = "ux_" + DB + "_" + SCH + "_" + TBL;
            cmd = "";
            if (useUse) cmd += "USE "+DB+" ";
            cmd += "IF EXISTS (select name from sys.indexes where name = N'" + idx + "') ";
            cmd += "DROP INDEX " + idx + " ON " + dbfocus;
            cmd += "\r\n";
            if (useUse) cmd += "USE "+DB;
            cmd += " IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE"
                    + " TABLE_CATALOG = N'" + DB + "' "
                    + " AND TABLE_SCHEMA = N'" + SCH + "' "
                    + " AND TABLE_NAME = N'" + TBL + "' "
                    + " )  ";
            cmd += "DROP TABLE " + dbfocus;
        } else {
            if (NamedCommon.TruncIt) {
                cmd = "IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE"
                        + " TABLE_CATALOG = N'" + DB + "' "
                        + " AND TABLE_SCHEMA = N'" + SCH + "' "
                        + " AND TABLE_NAME = N'" + TBL + "' "
                        + " )  ";
                cmd = cmd + "TRUNCATE TABLE " + dbfocus;
                uCommons.uSendMessage("Truncating table " + dbfocus);
            }
        }
        return cmd;
    }

    public static String CreateView(String DB, String SCH, String TBL, UniDynArray lineArray) {
        boolean useUse = CheckNamedDB();

        if (NamedCommon.AutoVault) { return VaultBuilder(DB, SCH, TBL, lineArray); }

        String cmd = "", tmp;
        String vSCH = SCH;

        if (NamedCommon.uniBase) SCH = "uni";
        TBL = SqlCommons.CheckTBL(TBL);
        String dbFocus = "[" + DB + "].[" + SCH + "].[" + TBL + "]";
        String vwName = NamedCommon.vwPrefix + TBL;

        if (!APImap.APIget("view").equals("")) vwName = APImap.APIget("view");

        String sCol = "", sName = "", cView = "", dView="";

        if (useUse) dView = "use " + DB + " ";
        if (NamedCommon.task.equals("022")) {
            // no time to drop and re-create views.
// //            String ifNotExists = "if not exists(select 1 from sys.views where name = '" + vwName + "' and type='v') ";
//            String ifNotExists = "if not exists(" + SelectView(vwName, NamedCommon.SqlSchema) + ") ";
//            dView += ifNotExists;
        } else {
            String ifExists = "if exists(" + SelectView(vwName, NamedCommon.SqlSchema) + ") ";
//            dView += ifExists + " DROP VIEW [" + vSCH + "].[" + vwName + "]";
            dView = "DROP VIEW IF EXISTS [" + vSCH + "].[" + vwName + "]";
        }
        cView = "CREATE VIEW " + vSCH + ".[" + vwName + "] AS SELECT ";

        boolean inclHost=false, inclAcct=false, inclFile=false;
        String[] tmpArr = NamedCommon.tblCols.split("\\,");
        String hostname="", acctName="", fileName="", colName="", colChk;
        int eot = tmpArr.length;
        for (int t=0 ; t<eot; t++) {
            colChk  = tmpArr[t].toLowerCase();
            colName  = tmpArr[t];
            colName  = colName.replaceAll("\\-", "");
            colName  = colName.replaceAll("\\.", "");
            colName  = colName.replaceAll("\\*", "");
            if(!inclHost) {
                inclHost = colChk.contains("dbhost");
                if (inclHost) hostname = colName;
            }
            if(!inclAcct) {
                inclAcct = colChk.contains("account");
                if (inclAcct) acctName = colName;
            }
            if(!inclFile) {
                inclFile = colChk.contains("file");
                if (inclFile) fileName = colName;
            }
        }
        tmpArr=null;

        if (!inclHost) {
            inclAcct=false;
            inclFile=false;
        }
        if (inclHost) cView += hostname + ", ";
        if (inclAcct)  cView += acctName+ ", ";
        if (inclFile)  cView += fileName+ ", ";

        cView += "uID,    LoadDte";
        if (NamedCommon.AMS.toLowerCase().equals("split")) {
            cView += ",   CT,   MV,   SV";
        } else {
            cView += ",   NestPosition";
        }

        if (!NamedCommon.isNRT) uCommons.uSendMessage("      > Drop / create view " + vSCH + "." + vwName);

        ArrayList<String> ColArr = new ArrayList<>();
        ColArr = BuildColNames(lineArray);
        int nbrItems = ColArr.size();
        String[] colParts = new String[5];
        for (int i = 0; i<nbrItems; i++) {
            colParts = ColArr.get(i).split("\\.");
            if (colParts.length < 2 ) continue;
            sCol = colParts[0].trim();
            sName= colParts[1].trim();
            if (sCol.replaceAll("\\ ", "").equals(""))  continue;
            if (sName.replaceAll("\\ ", "").equals("")) continue;
            if (sName.length() == 0) sName = sCol;
            cView += ",   " + sCol + " as " + sName;
        }
        cView += " FROM " + dbFocus;

        // ------------------------------------------------------------------
        String jdbcHost = APImsg.APIget("THOST");
        String jdbcUser = NamedCommon.jdbcUser;
        if (!NamedCommon.jdbcAuth.equals(""))   jdbcUser = NamedCommon.jdbcAuth + jdbcUser;
        // ------------------------------------------------------------------

        String cSchema = "";
        if (useUse) cSchema += "use " + DB + " ";
        /* ------------------------------------------------------------------- */

        if (!NamedCommon.task.equals("022")) {
            cSchema += "if not exists (" +
                    "SELECT SCHEMA_ID FROM sys.schemas WHERE [name] = N'"
                    + vSCH + "') begin exec('CREATE SCHEMA ["
                    + vSCH + "] AUTHORIZATION [" + jdbcUser + "]') end";

            if (!NamedCommon.sqlLite) {
                cmd += cSchema + "\r\n";                // create schema if not exists
                cmd += dView + "\r\n";                  // drop view     if exists
                cmd += cView;                           // create view
            } else {
                // needs massive amounts of testing !!  WITH SQL Lite !!         The Mutual Bank
                String chkExists = "SELECT name FROM sqlite_master WHERE type = 'view' AND name = '"+vSCH+"_"+vwName+"';";
                boolean exists=false;
                try {
                    Statement stmt = NamedCommon.uCon.createStatement();
                    ResultSet rs = stmt.executeQuery(chkExists);
                    exists=rs.next();
                    rs.close();
                    rs = null;
                    stmt.close();
                    stmt = null;
                } catch (SQLException se) {
                    SqlCommands.ReconnectService();
                    if (!NamedCommon.ZERROR) {
                        NamedCommon.uCon = ConnectionPool.getConnection(NamedCommon.jdbcCon);
                        if (NamedCommon.TranIsolation) {
                            try {
                                NamedCommon.uCon.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                            } catch (SQLException e) {
                                uCommons.uSendMessage("Isolation ERROR: " + e.getMessage());
                                NamedCommon.ZERROR = true;
                                return "";
                            }
                        }
                    } else {
                        return "";
                    }
                }
                if (exists) cmd = "DROP VIEW IF EXISTS " +vSCH+"_"+vwName+";\n";
                String oldName = vSCH+"."+vwName;
                String newName = vSCH+"_"+vwName;
                cView = cView.replace(oldName, newName);
                cmd +=  cView;
            }
        } else {
            // no time for this - do it by hand before running.
        }
        return cmd;
    }

    public static ArrayList<String> BuildColNames(UniDynArray lineArray) {
        ArrayList<String> Answer = new ArrayList<>();
        String sCol, chr, tmp, sName;
        int nbrLines = lineArray.dcount();
        for (int i = 1; i <= nbrLines; i++) {
            sName = String.valueOf(lineArray.extract(i, 6, 1));
            sCol = String.valueOf(lineArray.extract(i, 5, 1));
            chr = sCol.substring(0, 1);
            switch (chr) {
                case "*":
                    sCol = sCol.replace("*", "");
                    break;
                case "-":
                    sCol = sCol.replace("-", "");
                    break;
                case ".":
                    sCol = sCol.replace(".", "");
                    break;
                case "@":
                    sCol = sCol.replace("@", "");
                    break;
            }
            tmp = sCol+"."+sName+" ";
            Answer.add(tmp);
        }
        return Answer;
    }

    public static String BulkImport(String fname, String Path2Data) {
        String Komma = NamedCommon.Komma;
        int rnd = ThreadLocalRandom.current().nextInt(111, 999);
        String DatFile = fname;
        String BaseFle = fname.split("\\.")[0];
        String ErrFile = BaseFle + "_" + rnd + ".log";
        String DB = "";
        String SCH = "";
        String TBL = "";
        DB = uCommons.FieldOf(fname, "\\[", 2);
        DB = uCommons.FieldOf(DB, "\\]", 1);
        SCH = uCommons.FieldOf(fname, "\\[", 3);
        SCH = uCommons.FieldOf(SCH, "\\]", 1);
        if (DB.equals("~DB~")) DB = "$DB$";
        if (SCH.equals("~SC~")) SCH = "$SC$";
        TBL = uCommons.FieldOf(fname, "\\[", 4);
        TBL = uCommons.FieldOf(TBL, "\\]", 1);
        TBL = SqlCommons.CheckTBL(TBL);

        String sqlStmt = "";
        sqlStmt += "BULK INSERT [" + DB + "].[" + SCH + "].[" + TBL + "]";
        sqlStmt += " FROM '" + Path2Data + DatFile + "' WITH (";
        if (!NamedCommon.AZdir.equals("")) {
            sqlStmt += "DATA_SOURCE = '"+NamedCommon.AZdir + "', ";
//        } else {
//            sqlStmt += "TABLOCK, ";  // slows EVERYTHING down
        }
        sqlStmt += "FIELDTERMINATOR='" + Komma + "'";
        sqlStmt += ", ROWTERMINATOR='0x0a'";
        if (NamedCommon.catchErrs) {
            // error logging slows SQL Server down a LOT !!
//            sqlStmt += ", ERRORFILE='" + Path2Data + "logs/" + ErrFile + "'";
            sqlStmt += ", ERRORFILE='" + "C:\\temp\\SQL_Errors\\" + ErrFile + "'";
        }
        sqlStmt += ")";

        return sqlStmt;
    }

    public static boolean Exists(String rawTable) {
        boolean useUse = CheckNamedDB();

        // Notes:
        //      This uses rawDB from rFuel.properties
        //      It does NOT use the database from the message.

        boolean ans = false;
        String cmd = "";
        if (useUse) cmd = "use " + NamedCommon.rawDB + ";";
        cmd += "IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = 'raw'  " +
                "AND  TABLE_NAME = '" + rawTable + "'))  " +
                "BEGIN SELECT 1 AS ANSWER END  " +
                "ELSE BEGIN SELECT 0 AS ANSWER END";

        ResultSet rs;

        if (ConnectionPool.jdbcPool.size() == 0) {
            SqlCommands.DisconnectSQL();
            SqlCommands.ConnectSQL();
        }
        NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);

        if (NamedCommon.uCon == null) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
            return false;
        }

        Statement stmt = null;
        while (true) {
            try {
                stmt = NamedCommon.uCon.createStatement();
                rs = stmt.executeQuery(cmd);
                while (rs.next()) {
                    if (rs.getString("ANSWER").equals("1")) ans = true;
                }
                stmt.close();
                rs.close();
                break;
            } catch (SQLException e) {
                SqlCommands.ReconnectService();
                NamedCommon.uCon = ConnectionPool.getConnection(NamedCommon.jdbcCon);
            }
        }
        rs = null;
        stmt = null;
        cmd = "";
        return ans;
    }

    private static String VaultBuilder(String db, String sch, String tbl, UniDynArray lineArray) {
        boolean useUse = CheckNamedDB();

        String cmd = "", chr;
        String vSCH = sch;
        if (NamedCommon.uniBase) sch = "uni";
        String dbFocus = "[" + db + "].[" + sch + "].[" + tbl + "]";
        String vwName;
        String sCol = "", sName = "", cView = "", dView;
        /* ------------------------------------------------------------------- */
        // 1. Put all columns into an array                                    //
        // 2. Get a list of Hubs, Hub-sats links, link-sats  {H, L, S}         //
        //    a) The list is {column-name}_{entity-name}_{entity-type}         //
        //    b) The entity-name IS the view-name  {e.g. CustomerDetails_H}    //
        // 3. For each entity, keep a list of the column-names                 //
        /* ------------------------------------------------------------------- */
        ArrayList<String> BaseCols = new ArrayList<>();
        ArrayList<String> EntNames = new ArrayList<>();
        ArrayList<String> ColNames = new ArrayList<>();
        ArrayList<String> EntTypes = new ArrayList<>();
        ArrayList<String> vwCommandARR = new ArrayList<>();
        ArrayList<String> vwNamesARR = new ArrayList<>();
        ArrayList<String> ColArr = null;
        ColArr = BuildColNames(lineArray);
        int nbrItems = ColArr.size();
        if (nbrItems < 0) return "";
        int nbrCparts;
        String[] colParts = new String[5];
        String[] entParts = new String[20];
        String tmp1, tmp2, tmp3, tmp4, tmp5;
        for (int i = 0; i<nbrItems; i++) {
            // CollArr can return values like;                                  //
            //      F_3_n_1.SomeColumnIn_SomeDetails_SAT                        //
            //      F_3_n_1.F_3_n_1_SomeDetails_SAT                             //
            colParts = ColArr.get(i).split("\\.");
            try {
                tmp1 = colParts[0].trim();       // e.g. F_3_n_1                //
                tmp2 = colParts[1].trim();       // e.g. F_3_n_1_SomeDetails_SAT//
            } catch (NullPointerException | ArrayIndexOutOfBoundsException je) {
                continue;
            }
            nbrCparts = tmp2.length() - tmp2.replaceAll("\\_","").length();
            if (nbrCparts < 2) {
                uCommons.uSendMessage("******************************************************");
                uCommons.uSendMessage("*  Badly formed column name(s) in VaultBuilder().    *");
                uCommons.uSendMessage("*     3 underscored parts are required.              *");
                uCommons.uSendMessage("*     {column-name}_{entity-name}_{entity-type}      *");
                uCommons.uSendMessage("*     Please refer to CreateViews in documentation.  *");
                uCommons.uSendMessage("******************************************************");
                return "";
            }
            if (nbrCparts < 20) {
                entParts = tmp2.split("\\_");
                tmp5 = "";                                  // e.g. SAT         //
                tmp4 = "";                                  // e.g. SomeDetails //
                tmp3 = "";                                  // e.g. F_3_n_1     //
                int factor=0;
                try {
                    tmp5 = entParts[nbrCparts - factor].trim(); factor++;
                    tmp4 = entParts[nbrCparts - factor].trim(); factor++;
                    tmp3 = entParts[nbrCparts - factor].trim(); factor++;
                } catch (NullPointerException | ArrayIndexOutOfBoundsException je) {
                    continue;
                }
                for (int x=factor; x<=nbrCparts; x++) {
                    try {
                        tmp3 = entParts[nbrCparts - x].trim() + "_" + tmp3;
                    } catch (NullPointerException | ArrayIndexOutOfBoundsException je) {
                        // error trap - use continue if code is added below this
                    }
                }
            } else {
                uCommons.uSendMessage("******************************************************");
                uCommons.uSendMessage("** Too many parts to your underscore column names!  **");
                uCommons.uSendMessage("******************************************************");
                return "";
            }
            BaseCols.add(tmp1);     // e.g. F_3_n_1
            ColNames.add(tmp3);     // e.g. F_3_n_1  or  SomeColumnIn
            EntNames.add(tmp4);     // e.g. SomeDetails
            EntTypes.add(tmp5);     // e.g. SAT
        }
        /* ------------------------------------------------------------------- */
        String inUse, thisType, pfx="";
        int nbrLoops, sPos=0;
        while (EntNames.size() != 0) {
            nbrLoops = EntNames.size();
            try {
                inUse = EntNames.get(sPos);
                thisType = EntTypes.get(sPos);
            } catch (NullPointerException | ArrayIndexOutOfBoundsException je) {
                break;
            }

            vwName = NamedCommon.vwPrefix + inUse + "_" + thisType;

            if (inUse.equals("*")) {
                sPos++;
                if (sPos >= EntNames.size()) break;
                continue;
            }

            cView = "CREATE VIEW " + vSCH + ".[" + vwName + "] AS SELECT ";
            if (!EntTypes.get(sPos).equals("HUB")) {
                cView += "uID as NaturalKey, CT as CTpos, MV as MVpos, SV as SVpos, ";
            } else {
                cView += "DISTINCT ";
            }
            for (int i=0; i< nbrLoops; i++) {
                if (EntNames.get(i).equals(inUse) && EntTypes.get(i).equals(thisType) ) {
                    sCol = BaseCols.get(i).trim();
                    sName= ColNames.get(i).trim();
                    if (sName.length() == 0) sName = sCol;
                    if (sCol.length() > 0) {
                        cView += pfx + sCol + " as " + sName;
                        pfx = ", ";
                    }
                }
            }

            vwNamesARR.add(vwName);
            vwCommandARR.add(cView);

            for (int i=sPos; i<EntNames.size(); i++) {
                try {
                    if (EntNames.get(i).equals(inUse) && EntTypes.get(i).equals(thisType) ) {
                        BaseCols.remove(i);
                        ColNames.remove(i);
                        EntNames.remove(i);
                        EntTypes.remove(i);
                        i--;
                    }
                } catch (NullPointerException | ArrayIndexOutOfBoundsException je) {
                    // error trap - use continue if code is added below this
                }
            }
            sPos = 0;
            pfx="";
            cView = "";
            dView = "";
        }
        for (int i = 0 ; i < vwCommandARR.size() ; i++) {
            dView = "";
            if (useUse) dView = "use " + NamedCommon.SqlDatabase + ";";
            dView+= "if exists(" + SelectView(vwNamesARR.get(i), NamedCommon.SqlSchema) + ") ";

            dView += " DROP VIEW [" + vSCH + "].[" + vwNamesARR.get(i) + "]";
            cView = vwCommandARR.get(i) + " FROM " + dbFocus + "\n\n";
            cmd += dView + "\n" + cView;
            uCommons.uSendMessage("   .) HLS: build drop/create for " + vwNamesARR.get(i));
            cView = "";
            dView = "";
        }
        ColArr   = null;
        colParts = null;
        entParts = null;
        ColNames = null;
        EntNames = null;
        EntTypes = null;
        return cmd;
    }

    private static String SelectView(String vwName, String sch) {
        // NOTE: do Not use \n as it will execute that line on it's own ... WRONG !!!
        String cmd = "select T3.name as 'Schema', T1.name as ViewName " +
                "from sys.views T1, sys.objects T2, sys.schemas T3 " +
                "where T1.object_id = T2.object_id and T1.schema_id = T3.schema_id and " +
                "T1.name = '" + vwName + "' and T1.type='v' and T3.name = '" + sch + "'";
        return cmd;
    }

    private static boolean CheckNamedDB() {
        if (NamedCommon.SqlUseStmt) {
            return true;       // force it to use the "use" statememt
        } else {
            //  bloody mini SQL in Azure is a PAIN !!!!
            //       just not sure about this !!
            //  IF the jdbc string contains the db name, do NOT use sql USE
            //      that means the jdbc string points to a shitty cloud DB
            //  ?? Have I taken the DB name out of the jdbcCon strin ??
            return !NamedCommon.jdbcCon.toLowerCase().contains("databasename");
        }
    }

}
