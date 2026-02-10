package com.unilibre.kafka.dev;

import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SqlCommands;
import com.unilibre.commons.kafkaCommons;
import com.unilibre.commons.uCommons;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.unilibre.commons.kafkaCommons.uSendMessage;
import static com.unilibre.kafka.kConsumer.tCols;

public class Threader {

    private static String rawCols;
    private static String burstCols = ".Seqn,-uID,-LoadDte,-MD5,.ProcNum,-NestPosition";

    public static class CallableTask implements Callable {

        String message, xMap, BaseCamp;
        int procNum=0;

        CallableTask(String instr) {
            message = instr;
            procNum++;
            if (procNum > NamedCommon.MaxProc) procNum = 1;
        }

        public String call() throws InterruptedException {
            String answer="";
            boolean okay=false;
            answer = LoadToRaw(message);
//            if (answer.contains("FAIL")) okay = false;
//            if (okay) answer += LoadToLND(message);
            return answer;
        }

        private String LoadToRaw(String inMsg) {

            String errMsg="<<PASS>>";
//            List<String> tmpList = uStrings.gSplit2List(inMsg, "<im>");
            List<String> tmpList = new ArrayList<>(Arrays.asList(inMsg.split("<im>")));
            String datetime, datAct, u2Source, uvID, uvRec;
            if (tmpList.size() == 5) {
                datetime = tmpList.get(0);
                datAct   = tmpList.get(1);
                u2Source = tmpList.get(2);
                uvID = tmpList.get(3);
                uvRec = tmpList.get(4);
            } else {
                errMsg = "<<FAIL>> Badly formed uStream record in thread. Ignoring.";
                uSendMessage(errMsg);
                return errMsg;
            }

            String sqlTarget = u2Source + "_" + datAct;
            sqlTarget = sqlTarget.replaceAll("\\.", "_");
            sqlTarget = sqlTarget.replaceAll("\\,", "_");
            sqlTarget = sqlTarget.replaceAll("\\ ", "_");

            tsPrepareSQLtable(kafkaCommons.rawDB, "raw", sqlTarget);

            String dbFocus = "[" + kafkaCommons.rawDB + "].[raw].[" + sqlTarget + "]";
            String quote = "'";
            String Komma = ",";

            String MD5 = uCommons.GetMD5(uvID + NamedCommon.IMark + uvRec);

            if (uvRec.contains(quote)) {
                uvRec = uvRec.replace(quote, quote+quote);
            }
            String valString;
            valString = quote + MD5 + quote
                    + Komma + quote + uvID + quote
                    + Komma + quote + datetime + quote
                    + Komma + quote + procNum + quote
                    + Komma + quote + uvRec + quote;

            String createRow;
            createRow = "INSERT INTO " + dbFocus;
            createRow += " (" + NamedCommon.rawCols + ") VALUES (";
            createRow += valString + ")";
            ArrayList<String> DDL = new ArrayList<String>();
            DDL.add(createRow);
            // ------------------- test -------------------
            SqlCommands.ExecuteSQL(DDL);
            if (!NamedCommon.isNRT) uCommons.uSendMessage("      > Inserting raw record.");
            DDL.clear();
            DDL = null;

            return errMsg;
        }

        private void tsPrepareSQLtable(String DB, String SCH, String TBL) {
            String dbFocus = "[" + DB + "].[" + SCH + "].[" + TBL + "]";
            String tblidx = "ux_" + DB + "_" + SCH + "_" + TBL;
            String pKeys = "", wrk, cmd;

            if (TBL.contains(".")) {
                TBL = TBL.replaceAll("\\.", "_");
                TBL = TBL.replaceAll("\\,", "_");
                TBL = TBL.replaceAll("\\ ", "_");
            }
            String maxtype = "", inttype = "", smltype = "", keytype = "", dattype="";
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
            } else {
                maxtype = NamedCommon.maxType;
            }

            inttype = NamedCommon.intType;
            smltype = NamedCommon.smlType;
            keytype = NamedCommon.keyType;
            dattype = NamedCommon.datType;

            if (inttype.equals("")) inttype = " int not null";
            if (smltype.equals("")) smltype = " varchar(50) not null";
            if (keytype.equals("")) keytype = " varchar(150) not null";
            if (dattype.equals("")) dattype = " varchar(250) null";

            for (int xx = 0; xx < tCols.length; xx++) {
                wrk = tCols[xx];
                if (wrk == null) continue;
                if (wrk.length() > 1) {
                    if ((wrk.startsWith("-") || (wrk.contains("ID")) && wrk.contains(TBL))) {
                        if (wrk.startsWith("-")) wrk = wrk.substring(1, (wrk.length()));
                        if (!pKeys.contains(wrk)) pKeys += "," + wrk;
                    }
                }
            }

            while (pKeys.endsWith(","))   { pKeys = pKeys.substring(0, (pKeys.length() - 1)); }
            while (pKeys.startsWith(",")) { pKeys = pKeys.substring(1,pKeys.length()); }

            String tmpStr = "";
            tmpStr += "use " + NamedCommon.SqlDatabase + " if not exists (" +
                    "SELECT SCHEMA_ID FROM sys.schemas WHERE [name] = N'"
                    + SCH + "') begin exec('CREATE SCHEMA ["
                    + SCH + "] AUTHORIZATION [dbo]') end\n";

            tmpStr += "use " + NamedCommon.SqlDatabase + " IF object_id('" + dbFocus + "', 'U') is null ";
            tmpStr += "CREATE TABLE " + dbFocus + " ( ";

            String chr = "", wrkSav;
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

            cmd = tmpStr;

            cmd += "USE "+DB+" IF NOT EXISTS (select name from sys.indexes where name = N'" + tblidx + "') ";
            cmd += "CREATE UNIQUE INDEX ";
            cmd += tblidx + " on " + dbFocus;

            if (SCH.equals("upl")) {
                if (TBL.equals("UPLCTL")) {
                    cmd += " (BatchID,RunType,Map,Task)";
                } else {
                    cmd += " (uID)";
                }
            } else {
                if (!pKeys.equals("")) {
                    if (!pKeys.contains("uID")) pKeys = "uID," + pKeys;
                    cmd += " (" + pKeys + ")";
                } else {
                    cmd += " (uID,MD5)";
                }
            }

            cmd += " WITH (IGNORE_DUP_KEY = ON)";
            ArrayList<String> DDL = new ArrayList<>();
            DDL.add(cmd);
            SqlCommands.ExecuteSQL(DDL);
            DDL.clear();
            DDL = null;
        }

    }
    
    public static void begin(String task, String msg) {
        ExecutorService pool = Executors.newFixedThreadPool(1);
        CallableTask task1 = new CallableTask(msg);
        Future future1 = pool.submit(task1);

//        CallableTask task2 = new CallableTask("player2");
//         Future future2 = pool.submit(task2);

        pool.shutdown();
    }
}
