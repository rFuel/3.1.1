package com.unilibre.commons;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniclientlibs.UniDynArray;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

public class SqlCommons {

    public static String[] CheckCols(String[] tCols) {
        String[] answer = new String[tCols.length];
        String col;
        for (int xx=0; xx< answer.length; xx++) {
            col = tCols[xx];
            if (col == null) col = "";
            if (NamedCommon.SqlDBJar.equals("ORACLE") && col.endsWith("uID")) {
                col = col.replace("uID", "zID");
            }
            answer[xx] = col;
        }
        return answer;
    }

    public static String CheckTBL(String TBL) {
        String origTBL = TBL;
        TBL = TBL.replaceAll("\\.", "_");
        TBL = TBL.replaceAll("\\,", "_");
        TBL = TBL.replaceAll("\\ ", "_");
        if (!TBL.equals(origTBL)) {
            uCommons.eMessage = "Changed " + origTBL + " to " + TBL;
            uCommons.uSendMessage(uCommons.eMessage);
        }
        origTBL = "";
        return TBL;
    }

    public static String CheckSCH(String SCH) {
        if (NamedCommon.uniBase && NamedCommon.task.equals("014") && !SCH.equals("upl")) SCH = "uni";
        return SCH;
    }

    public static String[] SplitCols(String[] tCols) {

        if (!NamedCommon.AMS.toLowerCase().equals("split")) return tCols;

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
        tCols = CheckCols(tCols);
        return tCols;
    }

    public static String NoDots(String TBL) {
        uCommons.eMessage = "Changed " + TBL + " to ";
        TBL = TBL.replaceAll("\\.", "_");
        TBL = TBL.replaceAll("\\,", "_");
        TBL = TBL.replaceAll("\\ ", "_");
        uCommons.eMessage += TBL;
        return TBL;
    }

    public static String MaxCol(String SCH) {
        String maxtype = NamedCommon.maxType;
        if (NamedCommon.maxType.equals("")) {
            if (SCH.equals(NamedCommon.rawSC)) {
                maxtype = " varchar(max) null";
            } else {
                if (NamedCommon.Sparse) {
                    maxtype = " varchar(4000) SPARSE null";
                } else {
                    maxtype = " varchar(1000) null DEFAULT ''";
                }
            }
        }
        return maxtype;
    }

    public static String IntCol(String SCH) {
        String inttype = NamedCommon.intType;
        if (inttype.equals("")) inttype = " int not null";
        return inttype;
    }

    public static String SmlCol(String SCH) {
        String coltype = NamedCommon.smlType;
        if (coltype.equals("")) coltype = " varchar(50) not null";
        return coltype;
    }

    public static String KeyCol(String SCH) {
        String coltype = NamedCommon.keyType;
        if (coltype.equals("")) coltype = " varchar(150) not null";
        return coltype;
    }

    public static String DatCol(String SCH) {
        String coltype = NamedCommon.datType;
        if (coltype.equals("")) coltype = "  varchar(250) null";
        return coltype;
    }

    public static String TblCols(String[] tCols, String SCH) {
        tCols = CheckCols(tCols);
        String chr="", wrk, wrkSav, tmpStr="";
        String maxtype = MaxCol(SCH);
        String inttype = IntCol(SCH);
        String smltype = SmlCol(SCH);
        String keytype = KeyCol(SCH);
        String dattype = DatCol(SCH);
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
        if (tmpStr.endsWith(", ")) tmpStr = tmpStr.substring(0, tmpStr.length()-2);
        return tmpStr;
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

    public static void JDBCloader(String fname, String Path2Data) {
        boolean dov2 = false;
        JDBCLoaderV2(fname, Path2Data);
        if (dov2) return;

        boolean rename = false;

        if (fname.toLowerCase().endsWith(".dat")) {
            rename = uCommons.RenameFile(Path2Data + fname, Path2Data + fname + ".load");
            if (!rename) return;    // being done by another process
            fname += ".load";
        }
        ArrayList<String> lines = new ArrayList<>(Arrays.asList(uCommons.ReadDiskRecord(Path2Data+fname).split("\\r?\\n")));

        if (NamedCommon.ZERROR) {
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage = "";
            return;
        }

        String cols = lines.get(0);
        String chr = "", datFile="", vals="";
        if (!cols.startsWith("#")) return;
        String[] tmp = cols.substring(1, cols.length()).split("\\,");

        tmp = CheckCols(tmp);
        cols = "";

        for (int i=0; i < tmp.length; i++) {
            chr = tmp[i].substring(0,1);
            if (".-*@".contains(chr)) tmp[i] = tmp[i].substring(1, tmp[i].length());
            cols += tmp[i] + ", ";
            vals += "?, ";
        }

        if (cols.endsWith(", ")) {
            cols = cols.substring(0, cols.length()-2);
            vals = vals.substring(0, vals.length()-2);
        }

        datFile = fname.replace("\\", "/");
        int nbrSlash = datFile.length() - datFile.replace("/", "").length();
        datFile = uCommons.FieldOf(datFile, "/", (nbrSlash + 1));

        chr = "_";
        if (datFile.indexOf("]"+chr+"[") < 0) chr = "\\.";

        String DB = uCommons.FieldOf(datFile, chr, 1).replace("[", "").replace("]", "");
        String SC = uCommons.FieldOf(datFile, chr, 2).replace("[", "").replace("]", "");
        String TB = uCommons.FieldOf(datFile, "\\[", 4);
        TB = uCommons.FieldOf(TB, "\\]", 1);

        String dbFocus = DB + "." + SC + "_" + TB;
        String sqlInsert = "INSERT INTO " + dbFocus + " (" + cols + ") VALUES (" + vals + ")";

        if (NamedCommon.uCon == null) {
            if (!NamedCommon.tConnected) SqlCommands.ConnectSQL();
            if (!NamedCommon.tConnected) {
                return;
            }
            NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
        }

        boolean jdbcErr = false;
        String ext = ".done";
        try {
            PreparedStatement ps = NamedCommon.uCon.prepareStatement(sqlInsert);
            String[] lparts = new String[tmp.length];
            int pos=0, maxTrx=500, cntTrx=0;
            boolean doit=false;
            String val="";
            for (int i=1; i < lines.size(); i++) {
                lparts = lines.get(i).split("<ft>");
                cntTrx++;
                for (int j=0; j < tmp.length; j++) {
                    pos = j+1;
                    val = "";
                    if (j < lparts.length) val = lparts[j];
                    ps.setString(pos, val);
                    doit = true;
                }
                ps.addBatch();
                if (cntTrx > maxTrx) {
                    ps.executeBatch();
                    NamedCommon.uCon.commit();
                    ps.clearParameters();
                    doit = false;
                    cntTrx = 0;
                }
            }
            if (doit) {
                ps.executeBatch();
                ps.clearParameters();
                NamedCommon.uCon.commit();
            }
            ps.close();
            uCommons.Sleep(0);
            ps = null;
            lparts = null;
            uCommons.eMessage = "   .) Loaded " + lines.size() + " rows into " + dbFocus;
            uCommons.WriteDiskRecord(Path2Data+fname+".load.done", "");
        } catch (SQLException e) {
            uCommons.uSendMessage("FAIL:  " + e.getMessage());
            jdbcErr = true;
            ext = "failed";
        }
        lines.clear();

        if (rename) rename = uCommons.RenameFile(Path2Data+fname, Path2Data+fname+ext);

        uCommons.uSendMessage("---------------------------------------------------------------------------------.");
    }

    private static void JDBCLoaderV2(String fname, String Path2Data) {

        boolean okay = uCommons.RenameFile(Path2Data + fname, Path2Data + fname + ".load");
        if (!okay) return;

        if (NamedCommon.uCon == null) {
            if (!NamedCommon.tConnected) SqlCommands.ConnectSQL();
            if (!NamedCommon.tConnected) {
                return;
            }
            NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
        }

        String datFile = fname.replace("\\", "/");
        int nbrSlash = datFile.length() - datFile.replace("/", "").length();
        datFile = uCommons.FieldOf(datFile, "/", (nbrSlash + 1));

        String chr = "_";
        if (datFile.indexOf("]"+chr+"[") < 0) chr = "\\.";

        String DB = uCommons.FieldOf(datFile, chr, 1).replace("[", "").replace("]", "");
        String SC = uCommons.FieldOf(datFile, chr, 2).replace("[", "").replace("]", "");
        String TB = uCommons.FieldOf(datFile, "\\[", 4);
        TB = uCommons.FieldOf(TB, "\\]", 1);

        String dbFocus = DB + "." + SC + "_" + TB;

        BufferedReader BRin = null;
        FileReader fr = null;
        File fController = new File(Path2Data + fname + ".load");
        String val = "", vals="", cols = "";
        chr = "";
        try {
            fr = new FileReader(Path2Data+fname+".load");
            BRin = new BufferedReader(fr);

            String line = BRin.readLine().replaceAll("\\r?\\n", "");
            cols = line;
            if (!cols.startsWith("#")) return;
            String[] cTmp = PrepareCols(cols).split("\n");
            cols = cTmp[0];
            vals = cTmp[1];
            cTmp = null;
            String[] colController = cols.substring(1, cols.length()).split("\\,");
            String sqlInsert = "INSERT INTO " + dbFocus + " (" + cols + ") VALUES (" + vals + ")";
            NamedCommon.uCon.setAutoCommit(false);
            PreparedStatement ps = NamedCommon.uCon.prepareStatement(sqlInsert);
            int pos=0, maxTrx=500, cntTrx=0;
            boolean doit=false;

            String[] lparts = new String[cols.substring(1, cols.length()).split("\\,").length];

            line = BRin.readLine();
            while ((line) != null) {
                lparts = line.split("<ft>");
                cntTrx++;
                for (int j=0; j < colController.length; j++) {
                    pos = j+1;
                    val = "";
                    if (j < lparts.length) val = lparts[j];
                    ps.setString(pos, val);
                    doit = true;
                }
                ps.addBatch();
                if (cntTrx > maxTrx) {
                    ps.executeBatch();
                    NamedCommon.uCon.commit();
                    ps.clearParameters();
                    doit = false;
                    cntTrx = 0;
                }
                line = BRin.readLine();
            }
            if (doit) {
                ps.executeBatch();
                ps.clearParameters();
                NamedCommon.uCon.commit();
            }
            uCommons.Sleep(0);
            ps.close();
            ps = null;
            lparts = null;
            BRin.close();
            BRin = null;
            fr.close();
            fr = null;
            line = "";
            sqlInsert = "";
            fController.delete();
            fController = null;
        } catch (FileNotFoundException e) {
            // another process grabbed it first.
            return;
        } catch (IOException e) {
            uCommons.uSendMessage("FAIL: File-IO-Error " + e.getMessage());
            uCommons.RenameFile(Path2Data + fname + ".load", Path2Data + fname + ".fail");
            return;
        } catch (SQLException e) {
            uCommons.uSendMessage("FAIL: JDBC-Error " + e.getMessage());
            uCommons.RenameFile(Path2Data + fname + ".load", Path2Data + fname + ".fail");
            return;
        }
    }

    private static String PrepareCols(String inCols) {

        String[] tmp = inCols.substring(1, inCols.length()).split("\\,");

        tmp = CheckCols(tmp);
        String cols = "", vals = "", chr = "", ans;

        for (int i=0; i < tmp.length; i++) {
            chr = tmp[i].substring(0,1);
            if (".-*@".contains(chr)) tmp[i] = tmp[i].substring(1, tmp[i].length());
            cols += tmp[i] + ", ";
            vals += "?, ";
        }

        if (cols.endsWith(", ")) {
            cols = cols.substring(0, cols.length()-2);
            vals = vals.substring(0, vals.length()-2);
        }

        ans = cols + "\n" + vals;
        return ans;
    }

}
