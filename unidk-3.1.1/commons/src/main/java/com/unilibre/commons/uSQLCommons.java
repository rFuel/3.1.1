package com.unilibre.commons;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniclientlibs.UniDynArray;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class uSQLCommons {

    public static String CreateTable(String DB, String SCH, String TBL, String[] tCols) {
        String cmd = "";

        if (NamedCommon.AMS.toLowerCase().equals("split")) tCols = SqlCommons.SplitCols(tCols);

        for (int xx=0; xx< tCols.length; xx++) { if (tCols[xx] == null) tCols[xx] = ""; }

        if (TBL.contains(".")) TBL = SqlCommons.NoDots(TBL);

        if (NamedCommon.uniBase && NamedCommon.task.equals("014")) {
            if (!SCH.equals("upl")) SCH = "uni";
        }

        int nbrcols = tCols.length;
        String[] tcBCK = new String[nbrcols];
        if (SCH.equals("upl") || NamedCommon.isNRT) {
            for (int tc = 0; tc < nbrcols; tc++) { tcBCK[tc] = tCols[tc]; }
        }

        String dbFocus;
        dbFocus = DB + "." + SCH + "_" + TBL;
        String tmpStr = "";

        tmpStr += "CREATE TABLE IF NOT EXISTS " + dbFocus + " ( ";

        tmpStr += SqlCommons.TblCols(tCols, SCH);

        while (!tmpStr.endsWith(",")) {
            tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        }
        tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        tmpStr += "  ) " + "\n";

        cmd = tmpStr;

        if (SCH.equals("upl") || NamedCommon.isNRT) {
            SCH = "*" + SCH;
            cmd += CreateIndex(DB, SCH, TBL, tcBCK);
        }

        return cmd;

    }

    public static String CreateIndex(String DB, String SCH, String TBL, String[] tCols) {
        if (SCH.equals("upl")) return "";
        if (SCH.startsWith("*")) SCH = SCH.substring(1, SCH.length());
        if (NamedCommon.allowDups) return "";
        String pKeys = "", Komma = "", cmd = "", wrk;
        String dbFocus =  DB + "." + SCH + "_" + TBL;
        String tblidx = "ux_" + DB + "_" + SCH + "_" + TBL;
        if (NamedCommon.AMS.toLowerCase().equals("split")) tCols = SqlCommons.SplitCols(tCols);

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
                /* --------------------------------------------------------- */
            }
        }

        while (pKeys.endsWith(",")) {
            pKeys = pKeys.substring(0, (pKeys.length() - 1));
        }

        cmd += "CREATE UNIQUE INDEX IF NOT EXISTS ";
        cmd += tblidx + " on " + dbFocus;

        if (SCH.equals("upl")) {
            if (TBL.equals("UPLCTL")) {
                cmd += " (BatchID,RunType,Map,Task)";
            } else {
                cmd += " (uID)";
            }
        } else {
            if (!pKeys.equals("")) {
                //if (!pKeys.contains("LoadDte")) pKeys = "LoadDte," + pKeys;
                if (!pKeys.contains("uID")) pKeys = "uID," + pKeys;
                cmd += " (" + pKeys + ")";
            } else {
                cmd += " (uID,MD5)";
            }
        }

        return cmd;

    }

    public static String DropTable(String DB, String SCH, String TBL) {
        String cmd = "";
        cmd = "IF EXISTS DROP TABLE " + DB + "." + SCH + "_" + TBL;
        return cmd;
    }

    public static String AlterTable(String DB, String SCH, String TBL) {
        String cmd = "";

        return cmd;
    }

    public static String CreateView(String DB, String SCH, String TBL, UniDynArray lineArray) {
        String cmd = "", chr;
        String vSCH = SCH;
        if (NamedCommon.uniBase) SCH = "uni";
        String dbFocus =  DB + "." + SCH + "_" + TBL;
        String vwName = NamedCommon.vwPrefix + "_" + SCH + "_" + TBL;
        String sCol = "", sName = "", cView = "", dView;
        dView = "DROP VIEW IF EXISTS " + vwName;
        cView = "CREATE VIEW " + DB + "." + vwName + " AS SELECT ";
        cView += "LoadDte as LoadDte,   uID as uID";
        if (NamedCommon.AMS.toLowerCase().equals("split")) {
            cView += ", CT as CT, MV as MV, SV as SV";
        } else {
            cView += ", NestPosition as NestPosition";
        }

        uCommons.uSendMessage("   .) Drop / create view " + DB + "." + vwName);

        ArrayList<String> ColArr = uMSSQLCommons.BuildColNames(lineArray);
        int nbrItems = ColArr.size();
        String[] colParts;
        for (int i = 0; i<nbrItems; i++) {
            colParts = ColArr.get(i).split("\\.");
            sCol = colParts[0].trim();
            if (sCol.replaceAll("\\ ", "").equals("")) continue;
            sName= colParts[1].trim();
            if (sName.length() == 0) sName = sCol;
            cView += ", " + sCol + " as " + sName;
        }
        cView += " FROM " + dbFocus;
        /* ------------------------------------------------------------------- */
        cmd += dView + "\r\n" + cView;

        return cmd;
    }

    public static String BulkImport(String fname, String path) {
        String Komma = NamedCommon.Komma;
        int rnd = ThreadLocalRandom.current().nextInt(111, 999);
        String DatFile = fname;
        String BaseFle = fname.split("\\.")[0];
        String ErrFile = BaseFle + "_" + rnd + ".log";

        String DB = ""; String SCH = ""; String TBL = "";

        DB = uCommons.FieldOf(fname, "\\[", 2);
        DB = uCommons.FieldOf(DB, "\\]", 1);
        SCH = uCommons.FieldOf(fname, "\\[", 3);
        SCH = uCommons.FieldOf(SCH, "\\]", 1);

        if (DB.equals("~DB~")) DB = "$DB$";
        if (SCH.equals("~SC~")) SCH = "$SC$";

        TBL = uCommons.FieldOf(fname, "\\[", 4);
        TBL = uCommons.FieldOf(TBL, "\\]", 1);

        if (TBL.contains(".")) {
            uCommons.eMessage = "Change " + TBL + " to ";
            TBL = TBL.replaceAll("\\.", "_");
            TBL = TBL.replaceAll("\\,", "_");
            TBL = TBL.replaceAll("\\ ", "_");
            uCommons.eMessage += TBL;
        }

        String sqlStmt = "";
        sqlStmt += "LOAD DATA INFILE " + path + fname + " INTO  " + DB + "." + SCH + "_" + TBL + "]";
        sqlStmt += "COLUMNS TERMINATED  BY '" + Komma + "' ";
        sqlStmt += "LINES TERMINATED  BY '0x0a'";

        return sqlStmt;

    }
}
