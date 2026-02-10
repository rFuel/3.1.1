package com.unilibre.commons;

import asjava.uniclientlibs.UniDynArray;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class uSnowflakeCommons {

    public static String initialise = "";
    private static final String excludes = " UPLCTL ";
    private static final String unique = " UNIQUE";

    public static String CreateTable(String DB, String SCH, String TBL, String[] tCols) {

        // ----------------------------------------------------------------------------------------
        //
        // Set snowflake parameters ONCE for the pid run-time.
        // Then, each method can change parameters as / if required.
        //
        if (initialise.equals("")) InitialiseSnowflake();

        // ----------------------------------------------------------------------------------------

        tCols = SqlCommons.CheckCols(tCols);
        TBL = SqlCommons.CheckTBL(TBL);
        SCH = SqlCommons.CheckSCH(SCH);

        String usePfx = "";
        String cmd = "";
        String tmpStr="", tmp1Str="", tmp2Str="";
        String maxtype = SqlCommons.MaxCol(SCH);
        String inttype = SqlCommons.IntCol(SCH);
        String smltype = SqlCommons.SmlCol(SCH);
        String keytype = SqlCommons.KeyCol(SCH);
        String dattype = SqlCommons.DatCol(SCH);

        usePfx += "CREATE SCHEMA IF NOT EXISTS " + SCH + "\n";

        tmp1Str += usePfx + "CREATE TABLE IF NOT EXISTS " + SCH + "." + TBL + " ( ";
        // make TMP tables transient to save costs
        //      - can add the pid to keep it unique
        //      - make sure to drop them at the end of processing !!
        tmp2Str = "CREATE TRANSIENT TABLE IF NOT EXISTS " + SCH + ".TMP" + TBL + " ( ";

        String chr = "", wrk, wrkSav, extension = "";
        String specialCHrs = "*-.@";
        for (int xx = 0; xx < tCols.length; xx++) {
            if (tCols[xx].length() > 1) {
                /* --------------------------------------------------------- */
                wrk = tCols[xx];
                chr = wrk.substring(0, 1);
                // ENFORCE a not equals that strips the "F" out of colmn names
                if (!specialCHrs.contains(chr)) chr = "\n";
                if (wrk.contains("ID") && !wrk.equals("uID")) chr = "-";

                if (wrk.startsWith(chr)) wrk = wrk.substring(1, (wrk.length()));

                if (chr.equals("-")) {
                    extension = unique;
                } else if (wrk.contains("ID")) {
                    extension = unique;
                } else if (wrk.contains(TBL)) {
                    extension = unique;
                } else if (wrk.toUpperCase().equals("LOADDTE")) {
                    extension = unique;
                } else if (wrk.toUpperCase().equals("DBHOST")) {
                    extension = unique;
                } else if (wrk.toUpperCase().equals("ACCOUNT")) {
                    extension = unique;
                } else {
                    extension = "";
                }

                wrkSav = wrk;
                if (wrk.contains("NestPosition")) {
                    wrk = "NestPosition";
                    wrkSav = wrk;
                }
                switch (chr) {
                    case "*":
                        tmpStr += " " + wrk + maxtype + extension + ", ";
                        break;
                    case "-":
                        tmpStr += " " + wrk + keytype + extension + ", ";
                        break;
                    case ".":
                        tmpStr += " " + wrk + smltype + extension + ", ";
                        break;
                    case "@":
                        tmpStr += " " + wrk + inttype + extension + ", ";
                        break;
                    default:
                        if (!wrkSav.equals("NestPosition")) {
                            tmpStr += " " + wrkSav + dattype + extension + ", ";
                        } else {
                            if (NamedCommon.AMS.toLowerCase().equals("split")) {
                                tmpStr += " CT" + smltype + extension +
                                        ", MV" + smltype + extension +
                                        ", SV" + smltype + extension + ", ";
                            } else {
                                tmpStr += " " + wrkSav + smltype + extension + ", ";
                            }
                        }
                }
                /* --------------------------------------------------------- */
            }
        }
        tmpStr.trim();
        while (!tmpStr.endsWith(",")) {
            tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        }
        tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        tmpStr += "  )" + "\n";
        tmp1Str+= tmpStr;
        tmp2Str+= tmpStr;

        if (!excludes.contains(TBL)) {
            if (NamedCommon.RunType.equals("INCR")) tmp1Str="";
            cmd = tmp1Str + "\n" + tmp2Str;
        } else {
            cmd = tmp1Str;
        }
        return cmd;
    }

    private static void InitialiseSnowflake() {
        if (initialise.equals("")) {
            if (NamedCommon.ZERROR) return;
            // 1. Attach to role, database, warehouse and schema
            initialise = "CREATE SCHEMA IF NOT EXISTS UPL;\n" +
                    "USE ROLE " + NamedCommon.SqlRole + "\n" +
                    "USE DATABASE " + NamedCommon.SqlDatabase + "\n" +
                    "USE WAREHOUSE " + NamedCommon.SqlWarehouse + "\n" +
                    "USE SCHEMA UPL\n" +
                    "CREATE FILE FORMAT IF NOT EXISTS UPL.CSVFORMATTER" +
                    " TYPE = 'CSV'" +
                    " FIELD_DELIMITER = '" + NamedCommon.Komma + "'" +
                    " FIELD_OPTIONALLY_ENCLOSED_BY = '";
            if (NamedCommon.Quote.equals("'")) {
                // 4 ' will make it a single ' in Snowflake.
                initialise += NamedCommon.Quote + NamedCommon.Quote + NamedCommon.Quote;
            } else {
                initialise += NamedCommon.Quote + "'";
            }

            ExecCMD(initialise);

            // Need to execute sql directly because of line-feeds in dataLoader.

            String dataLoader = uCommons.ReadDiskRecord(NamedCommon.BaseCamp+NamedCommon.slash+"conf"+NamedCommon.slash+"sfLOADER.sp");
            Statement stmt = null;
            try {
                stmt = NamedCommon.uCon.createStatement();
                stmt.execute(dataLoader);
                stmt.close();
            } catch (SQLException e) {
                uCommons.uSendMessage(e.getMessage());
                NamedCommon.ZERROR = true;
                return;
            }
            stmt = null;
            uCommons.uSendMessage("Snowflake initialised -------------------------------");
        }
    }

    private static void ExecCMD(String initialise) {
        ArrayList<String> internalDDL = new ArrayList<>();
        internalDDL.add(initialise);
        SqlCommands.ExecuteSQL(internalDDL);
        internalDDL.clear();
    }

    public static String CreateIndex(String DB, String SCH, String TBL, String[] tCols) {
        return "";
    }

    public static String DropTable(String DB, String SCH, String TBL) {

        // ----------------------------------------------------------------------------------------
        //
        // Set snowflake parameters ONCE for the pid run-time.
        // Then, each method can change parameters as / if required.
        //
        if (initialise.equals("")) InitialiseSnowflake();

        // ----------------------------------------------------------------------------------------

        SCH = SCH.toUpperCase();
        TBL = TBL.toUpperCase();

        String vSCH = SCH;

        if (NamedCommon.uniBase && NamedCommon.task.equals("014")) if (!SCH.equals("upl")) SCH = "UNI";

        // -------------------------------------------------------------------------
        // if customer sets dropit to false, they can create and partition tables
        // manually. This is the point of ProcNum. So the mustdrop flag needs to
        // NOT be used. However, if they do set dropit to false and do not create
        // the tables, rFuel must check for table existence before truncates.
        // -------------------------------------------------------------------------

        if (NamedCommon.RunType.equals("PART")) NamedCommon.DropIt = false;
        String cmd = "";
        String tmp = "";
        String tvw = "";
        if (NamedCommon.DropIt) {
            cmd += "DROP ";
            if (NamedCommon.task.equals("014")) {
                tvw = "DROP VIEW IF EXISTS " + NamedCommon.SqlDatabase+"."+vSCH+"."+NamedCommon.vwPrefix+TBL + "\n";
                System.out.println("===============================================");
                System.out.println(tvw);
                System.out.println("===============================================");
            }
        } else {
            cmd = "TRUNCATE ";
            uCommons.uSendMessage("Truncating table " + TBL);
        }
        tmp = cmd + "TABLE IF EXISTS " + SCH + ".TMP" + TBL + "\n";
        cmd += "TABLE IF EXISTS " + SCH + "." + TBL + "\n";
        if (!excludes.contains(TBL)) {
            cmd += tmp + tvw;
            if (NamedCommon.RunType.equals("INCR")) cmd="";
        }
        return cmd;
    }

    public static String CreateView(String DB, String SCH, String TBL, UniDynArray lineArray) {

        SCH = SCH.toUpperCase();
        TBL = TBL.toUpperCase();

        String dataSCH = SCH;
        if (NamedCommon.uniBase) dataSCH = "UNI";

        String dbFocus = NamedCommon.SqlDatabase+"."+dataSCH + "." + TBL;
        String vwName  = NamedCommon.SqlDatabase+"."+SCH+"."+NamedCommon.vwPrefix+TBL;
        String sCol = "";
        String sName= "";
        ArrayList<String> ddl = new ArrayList<>();
        ddl.add("CREATE SCHEMA IF NOT EXISTS "+SCH+"\n");
        SqlCommands.ExecuteSQL(ddl);
        if (NamedCommon.ZERROR) return "";
        uCommons.Sleep(1);      // Give Snaowflake time to create the schema !!
        ddl = null;

        String cView = "CREATE OR REPLACE VIEW " + vwName + " AS SELECT ";

        boolean inclHost = NamedCommon.rawCols.toLowerCase().contains("dbhost");
        boolean inclAcct = NamedCommon.rawCols.toLowerCase().contains("account");
        if (!inclHost) inclAcct=false;
        if (inclHost) cView += "DBHost,  ";
        if (inclAcct)  cView += "Account,  ";

        cView += "uID,    LoadDte";
        if (NamedCommon.AMS.toLowerCase().equals("split")) {
            cView += ",   CT,   MV,   SV";
        } else {
            cView += ",   NestPosition";
        }

        ArrayList<String> ColArr = BuildColNames(lineArray);
        int nbrItems = ColArr.size();
        String[] colParts;
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
        cView += " FROM " + dbFocus + "\n";

        return cView;
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

    public static void BulkImport(String fname, String Path2Data) {
        // The bulk insert process is entirely handled by sfLoadData
    }

    public static boolean Exists(String rawTable) {
        boolean ans = false;
        String cmd;
        cmd = "SELECT CASE WHEN LEN(TABLE_NAME) > 0 THEN 1 ELSE 0 END " +
                "AS ANSWER FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = 'RAW'" +
                "AND  TABLE_NAME = '" + rawTable + "'";
        ResultSet rs;
        NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
        if (NamedCommon.uCon == null) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
            return false;
        }
        Statement stmt = null;
        try {
            stmt = NamedCommon.uCon.createStatement();
            rs = stmt.executeQuery(cmd);
            while (rs.next()) {
                if (Integer.valueOf(rs.getString("ANSWER")) > 0) ans=true;
            }
            stmt.close();
            rs.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        rs = null;
        stmt = null;
        cmd = "";
        return ans;
    }

    private static String VaultBuilder(String db, String sch, String tbl, UniDynArray lineArray) {

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
            dView = "use " + NamedCommon.SqlDatabase + "; if exists(" + SelectView(vwNamesARR.get(i), NamedCommon.SqlSchema) + ") ";
            dView += " DROP VIEW " + vSCH + "." + vwNamesARR.get(i);
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

}
