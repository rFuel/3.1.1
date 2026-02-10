package com.unilibre.commons;

public class ksqlDB {

    public static String CreateTable(String DB, String SC, String TBL, String[] tCols) {
        // "DB" is redundant
        tCols = SqlCommons.CheckCols(tCols);
        String maxtype = SqlCommons.MaxCol(SC);
        String inttype = SqlCommons.IntCol(SC);
        String smltype = SqlCommons.SmlCol(SC);
        String keytype = SqlCommons.KeyCol(SC);
        String dattype = SqlCommons.DatCol(SC);

        String tmpStr = "CREATE TABLE IF NOT EXISTS " + TBL + " ( ";
        String chr = "", wrk, wrkSav;
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
                        tmpStr += " " + wrk + smltype + ", ";
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

        while (!tmpStr.endsWith(",")) {
            tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        }
        tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        tmpStr += "  ) " + "\n";

        return tmpStr;
    }

}
