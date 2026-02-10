package com.unilibre.commons;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniclientlibs.UniDynArray;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class uOracleCommons {

    private static ResultSet results=null;
    private static Statement stmt = null;

    public static String CreateTable(String DB, String SCH, String TBL, String[] tCols) {
        String cmd = "";
        TBL = SqlCommons.CheckTBL(TBL);
        SCH = SqlCommons.CheckSCH(SCH);
        TBL = (SCH + "_" + TBL).toUpperCase();

        String dbFocus = DB + "." + TBL;

        if (Exists(TBL.toUpperCase())) return cmd;

        cmd = "CREATE TABLE " + dbFocus + " (";
        cmd+= SqlCommons.TblCols(tCols, SCH);
        cmd+= "  ) " + "\n";

        if (!SCH.toLowerCase().equals("raw") && !TBL.toLowerCase().contains("workfile")) {
            String useDB = DB;
            if (DB.startsWith("$")) useDB = NamedCommon.rawDB;
            cmd += "CREATE OR REPLACE TRIGGER T_" + TBL + " " +
                    "AFTER INSERT OR UPDATE ON " + TBL + " " +
                    "REFERENCING NEW AS NEW OLD AS OLD " +
                    "FOR EACH ROW " +
                    "BEGIN " +
                    "INSERT INTO AUD_TRXLOGS (sch, tbl, rid)  VALUES  ('" + useDB + "', '" + TBL + "', :NEW.ROWID); " +
                    "END;";
        }

        return cmd;
    }

    public static String CreateIndex(String DB, String SCH, String TBL, String[] tCols) {
        if (SCH.equals("upl")) return "";
        if (SCH.startsWith("*")) SCH = SCH.substring(1, SCH.length());
        if (NamedCommon.allowDups) return "";

        tCols = SqlCommons.CheckCols(tCols);
        tCols = SqlCommons.SplitCols(tCols);
        TBL = SqlCommons.CheckTBL(TBL);
        SCH = SqlCommons.CheckSCH(SCH);
        TBL = SCH + "_" + TBL;
        String dbFocus = DB + "." + TBL;
        String tblidx = "ux_" + TBL;

        String wrk="", pKeys="", Komma="";
        //
        // Identify Primary Keys and add to index
        //
        for (int xx = 0; xx < tCols.length; xx++) {
            wrk = tCols[xx];
            if (wrk == null) continue;
            if (wrk.length() > 1) {
                /* --------------------------------------------------------- */
                if ((wrk.startsWith("-") || (wrk.contains("ID")) && wrk.contains(TBL))) {
                    if (wrk.startsWith("-")) wrk = wrk.substring(1, (wrk.length()));
                    if (!pKeys.contains(wrk)) pKeys += Komma + wrk;
                    Komma = ",";
                }
            }
        }
        while (pKeys.endsWith(",")) {
            pKeys = pKeys.substring(0, (pKeys.length() - 1));
        }

        String cmd = "CREATE UNIQUE INDEX ";
        cmd += tblidx + " on " + dbFocus + " (" + pKeys + ")";

        return cmd;
    }

    public static String DropTable(String DB, String SCH, String TBL) {
        TBL = SqlCommons.CheckTBL(TBL);
        SCH = SqlCommons.CheckSCH(SCH);
        if (!SCH.equals("")) TBL = SCH + "_" + TBL;
        if (!Exists(TBL.toUpperCase())) return "";

        String cmd = "";
        cmd = "SELECT ai.index_name, ai.table_name, ai.index_type, ai.uniqueness, aic.column_name " +
                "FROM all_indexes ai INNER JOIN all_ind_columns aic " +
                "     ON ai.owner = aic.index_owner " +
                "     AND ai.index_name = aic.index_name " +
                "WHERE ai.owner = '" + NamedCommon.rawDB + "' " +
                "  AND ai.table_name = '" + TBL + "' " +
                "ORDER BY aic.column_position";
        RunCommand(cmd);
        boolean dropit = false;
        while (true) {
            try {
                if (!results.next()) break;
                if (results.getString("INDEX_NAME").equals("UX_" + TBL)) dropit=true;
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        try {
            results.close();
        } catch (SQLException e) {
            uCommons.uSendMessage(e.getMessage());
        }
        results = null;
        cmd = "";
        if (dropit) cmd += "DROP INDEX ux_" + TBL + "\n";

        String dbFocus = DB + "." + TBL;
        cmd += "DROP TABLE " + dbFocus;
        return cmd;
    }

    public static String DropView(String DB, String SCH, String TBL) {
        TBL = SqlCommons.CheckTBL(TBL);
        SCH = SqlCommons.CheckSCH(SCH);
        TBL = SCH + "_" + TBL;
        String dbFocus = DB + "." + TBL;
        String vwName = NamedCommon.vwPrefix + TBL;
        String cmd = "DROP VIEW " + vwName;
        return cmd;
    }

    public static String CreateView(String DB, String SCH, String TBL, UniDynArray lineArray) {
        TBL = SqlCommons.CheckTBL(TBL);
        SCH = SqlCommons.CheckSCH(SCH);
        TBL = SCH + "_" + TBL;
        String dbFocus = DB + "." + TBL;
        String vwName = NamedCommon.vwPrefix + TBL;

        String cmd = "CREATE OR REPLACE VIEW " + vwName + " AS SELECT ";
        cmd += "LoadDte as LoadDte, zID as zID";
        if (NamedCommon.AMS.toLowerCase().equals("split")) {
            cmd += ", CT as CT, MV as MV, SV as SV";
        } else {
            cmd += ", NestPosition as NestPosition";
        }

        ArrayList<String> ColArr = new ArrayList<>();
        ColArr = SqlCommons.BuildColNames(lineArray);
        int nbrItems = ColArr.size();
        String sCol, sName;
        String[] colParts = new String[5];
        for (int i = 0; i<nbrItems; i++) {
            colParts = ColArr.get(i).split("\\.");
            sCol = colParts[0].trim();
            if (sCol.replaceAll("\\ ", "").equals("")) continue;
            sName= colParts[1].trim();
            if (sName.length() == 0) sName = sCol;
            cmd += ", " + sCol + " as " + sName;
        }
        cmd += " FROM " + dbFocus;
        return cmd;
    }

    public static String BulkImport(String fname, String Path2Data) {
        SqlCommons.JDBCloader(fname, Path2Data);
        return "";
    }

    public static boolean Exists(String rawTable) {
        String cmd = "SELECT COUNT(*) AS ANSWER FROM USER_TABLES " +
                "WHERE TABLE_NAME = '" + rawTable.toUpperCase() + "'";

        RunCommand(cmd);
        boolean ans = false;
        while (true) {
            try {
                if (!results.next()) break;
                if (!results.getString("ANSWER").equals("0")) ans=true;
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        try {
            results.close();
            stmt.close();
        } catch (SQLException e) {
            uCommons.uSendMessage(e.getMessage());
        }
        results = null;
        stmt = null;
        cmd = "";
        return ans;
    }

    private static void RunCommand(String cmd) {
        results = null;
        stmt = null;
        if (!NamedCommon.tConnected) SqlCommands.ConnectSQL();
        if (NamedCommon.uCon == null) {
            if (ConnectionPool.jdbcPool.size() >= 0) {
                NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
                if (NamedCommon.uCon == null) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                    return;
                }
            }
        }

        try {
            stmt = NamedCommon.uCon.createStatement();
            results = stmt.executeQuery(cmd);
        } catch (SQLException e) {
            uCommons.uSendMessage(e.getMessage());
        }
        return;
    }

}
