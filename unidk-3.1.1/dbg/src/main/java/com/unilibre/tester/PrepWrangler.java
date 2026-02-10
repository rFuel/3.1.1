package com.unilibre.tester.tester;

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SourceDB;
import com.unilibre.commons.uCommons;

import java.util.Properties;

public class PrepWrangler {

    public static void main(String[] args) {
        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);
        String dbfHost = "rfuel22";

        if (NamedCommon.upl.equals("")) {
            NamedCommon.upl = System.getProperty("user.dir");
            NamedCommon.BaseCamp = NamedCommon.upl;
        }
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            uCommons.SetMemory("domain", dbfHost);
            NamedCommon.BaseCamp = "\\\\"+dbfHost+"\\all\\upl";
            NamedCommon.upl = NamedCommon.BaseCamp;
            NamedCommon.slash = "/";
        }

        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(runProps);
        SourceDB.ConnectSourceDB();
        String acct = "DATA";
        String file = "CLIENT", command;
        UniString id;
        UniDynArray rec, stats;
        UniFile voc = null, st, uf;
        UniSelectList sl, oldList;
        UniCommand cmd;

        String datum, locn, sep="-", qfile;
        int ac, mc, sc, stA, iQty, cnt=0, pCnt=99, tCnt=0, atSel , slx;
        String sQTY, uvMsg;
        boolean isNum, isMT;

        qfile = "upl_" + acct + "_" + file;
        try {
            voc= NamedCommon.uSession.open("VOC");
            rec= new UniDynArray();
            rec.replace(1, "Q");
            rec.replace(2, acct);
            rec.replace(3, file);
            voc.setRecordID(qfile);
            voc.setRecord(rec);
            voc.write();
        } catch (UniSessionException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (UniFileException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            uf = NamedCommon.uSession.open(qfile);
            st = NamedCommon.uSession.open("uSTRUCTURES");
            cmd = NamedCommon.uSession.command();

            // clear old stats -------------------------------
            System.out.println(" ");
            System.out.println(" ");
            uCommons.uSendMessage("Remove old stats for file: " + file);
            command = "SELECT " + st.getFileName() + " LIKE " + file + "...";
            cmd.setCommand(command);
            cmd.exec();
            uCommons.uSendMessage(cmd.response().replaceAll("\\r?\\n", ""));

            sl = NamedCommon.uSession.selectList(0);
            while (!sl.isLastRecordRead()) {
                id = sl.next();
                if (id == null) continue;
                if (id.toString().equals("")) continue;
                st.setRecordID(id);
                st.deleteRecord();
            }
            uCommons.uSendMessage("Done.");
            //  ---------------------------------------------

            System.out.println(" ");
            System.out.println(" ");
            uCommons.uSendMessage("Build stats for file: " + file);
            cmd.setCommand("SELECT " + qfile);
            cmd.exec();
            uvMsg = cmd.response();
            uCommons.uSendMessage(uvMsg.replaceAll("\\r?\\n", ""));

            sl = NamedCommon.uSession.selectList(0);
            atSel = cmd.getAtSelected();
            slx = String.valueOf(atSel).length();
            //
            while (!sl.isLastRecordRead()) {
                id = sl.next();
                if (!id.equals("")) {

                    try {
                        uf.setRecordID(id);
                        rec = new UniDynArray(uf.read());
                    } catch (UniFileException e) {
                        continue;
                    }

                    cnt++;
                    if (cnt > pCnt) {
                        tCnt = tCnt + cnt;
                        System.out.println(uCommons.RightHash(String.valueOf(tCnt), slx));
                        cnt=0;
                    }

                    ac = rec.dcount();
                    for (int av=1 ; av <= ac ; av++) {
                        mc = rec.dcount(av);
                        for (int mv=1; mv <= mc ; mv++) {
                            sc = rec.dcount(av,mv);
                            for (int sv=1 ; sv <= sc ; sv++) {
                                locn = file + sep + av + sep + mv + sep + sv;
                                datum = rec.extract(av, mv, sv).toString();
                                isMT  = datum.equals("");
                                // [-+]  means it may begin with a - or + symbol
                                // ?\\d* means is may have any amount of digits (d)
                                // .     means it may have a decimal point
                                isNum = datum.matches("[-+]?\\d*\\.?\\d+");

                                try {
                                    st.setRecordID(locn);
                                    stats = new UniDynArray(st.read());
                                    datum = "";
                                } catch (UniFileException e) {
                                    stats = new UniDynArray();
                                }

                                if (isMT) {
                                    // unknown data type
                                    stA = 3;
                                } else if (isNum) {
                                    // numeric data type
                                    stA = 1;
                                } else {
                                    // text or string data type
                                    stA = 2;
                                }

                                if (stats == null) {
                                    sQTY = "0";
                                } else {
                                    sQTY = stats.extract(stA).toString();
                                }
                                try {
                                    iQty = Integer.valueOf(sQTY);
                                } catch (NumberFormatException nfe) {
                                    iQty = 0;
                                }
                                iQty++;
                                try {
                                    stats.replace(stA, 1, 1, iQty);
                                    st.setRecordID(locn);
                                    st.setRecord(stats);
                                    st.write();
                                } catch (UniFileException e) {
                                    e.printStackTrace();
                                    System.exit(1);
                                }
                                stats = null;
                            }
                        }
                    }
                    rec = null;
                }
            }
            tCnt = tCnt + cnt;
        } catch (UniSessionException ex) {
            ex.printStackTrace();
            if (NamedCommon.uSession != null && NamedCommon.uSession.isActive()) {
                try {
                    NamedCommon.uJava.closeSession(NamedCommon.uSession);
                } catch (UniSessionException e) {
                    e.printStackTrace();
                }
                NamedCommon.uSession = null;
            }
            ex.printStackTrace();
            uCommons.uSendMessage(ex.getMessage());
        } catch (UniCommandException e) {
            e.printStackTrace();
        } catch (UniSelectListException e) {
            e.printStackTrace();
        } catch (UniFileException e) {
            // deleting data from uStructures
            e.printStackTrace();
        } finally {
            if (NamedCommon.uSession != null && NamedCommon.uSession.isActive()) {
                try {
                    voc.setRecordID(qfile);
                    voc.deleteRecord();
                } catch (UniFileException e) {
                    //
                }

                uCommons.uSendMessage("");
                uCommons.uSendMessage("============================================================================");
                uCommons.uSendMessage(tCnt + " records done.");
                uCommons.uSendMessage("============================================================================");
                try {
                    NamedCommon.uJava.closeSession(NamedCommon.uSession);
                } catch (UniSessionException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
