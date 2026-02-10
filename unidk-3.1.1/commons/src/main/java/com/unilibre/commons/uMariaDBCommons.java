package com.unilibre.commons;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class uMariaDBCommons {

    public static String CreateTable(String DB, String SCH, String TBL, String[] tCols) {
        String cmd = "", maxtype = "";
        String Table = SCH+"_"+TBL;
        if (SCH.equals(NamedCommon.rawSC)) {
            maxtype = " varchar(21844) null";
        } else {
            if (NamedCommon.Sparse) {
                maxtype = " varchar(4000) SPARSE null";
            } else {
                maxtype = " varchar(1000) null DEFAULT ''";
            }
        }

        String inttype = " int not null";
        String smltype = " varchar(50) not null";
        String keytype = " varchar(150) not null";

        cmd = "USE "+DB+";\n";
        cmd += "CREATE TABLE " + Table;
        String tmpStr = " ( ";

        String chr = "", wrk, wrkSav;
        for (int xx = 0; xx < tCols.length; xx++) {
            if (tCols[xx].length() > 1) {
                /* --------------------------------------------------------- */
                wrk = tCols[xx];
                wrkSav = wrk;
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
                        tmpStr += " " + wrkSav + maxtype + ", ";
                }
                /* --------------------------------------------------------- */
            }
        }

        while (!tmpStr.endsWith(",")) {
            tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        }
        tmpStr = tmpStr.substring(0, (tmpStr.length() - 1));
        tmpStr += "  ); " + "\n";

        cmd += tmpStr;

        return cmd;
    }

    public static String DropTable(String DB, String SCH, String TBL) {
        String Table = SCH+"_"+TBL;
        String cmd = "";
        cmd = "USE "+ DB+";\n";
        cmd += "DROP TABLE IF EXISTS " + Table + ";\n";
        return cmd;
    }

    public static String AlterTable(String DB, String SCH, String TBL) {
        String cmd = "";

        return cmd;
    }

    public static String BulkImport() {
        return "";
    }

    public static boolean Exists(String rawTable) {
        boolean ans = false;
        String cmd = "SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                "WHERE TABLE_SCHEMA = '" + NamedCommon.rawDB + "' " +
                "AND TABLE_NAME = 'raw_" + rawTable + "' LIMIT 1";
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
                if (rs.getString("ANSWER").equals("1")) ans=true;
            }
            stmt.close();
            rs.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        rs = null;
        stmt = null;
        return ans;
    }

}
