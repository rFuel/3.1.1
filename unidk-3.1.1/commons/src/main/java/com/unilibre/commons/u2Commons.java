package com.unilibre.commons;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniclientlibs.UniDataSet;
import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.unilibre.commons.uCommons.*;

public class u2Commons {

    private static UniCommand uniCmd = null;
    public static String rfuelFiles = " BP.UPL OBJ.UPL uLOG uREQUESTS uRESPONSES UPL.INSERTS &SAVEDLISTS& &PH& uDELTA.LOG uLISTS ";
    public static String custFiles = "", uniExecResp = "";
    private static String pingString;
    static UniFile uplBP;
    static UniFile uplOBJ;
    static UniFile uplInserts;
    static UniString uniString;
    static String startFrom = "standard";
    private static boolean stopProc = false;
    private static boolean isManaged = false;

    private static long startM, finishM;
    private static double laps;
    private static final double div = 1000000000.00;

    public static void SetManaged(boolean inval) {isManaged=inval;}

    public static void uFlipRaw() {

        int rnd = ThreadLocalRandom.current().nextInt(111, 999);
        NamedCommon.serial = String.valueOf(rnd);
        String baseFile = NamedCommon.u2Source + "_" + APImsg.APIget("dacct");
        uCommons.uSendMessage("   .) Flipping TAKE to LOADED on " + baseFile);
        NamedCommon.Zmessage = "";
        int seqn = 0;
        UniSubroutine fliploaded;
        while (true) {
            try {
                fliploaded = NamedCommon.uSession.subroutine("uFLIPDATA", 5);
                try {
                    fliploaded.setArg(0, "");
                    fliploaded.setArg(1, baseFile);
                    fliploaded.setArg(2, NamedCommon.serial);
                    fliploaded.setArg(3, seqn);
                    fliploaded.setArg(4, "");
                    fliploaded.call();
                    NamedCommon.Zmessage = fliploaded.getArg(0);
                    break;
                } catch (UniSubroutineException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        uCommons.eMessage = "ERROR " + e.getMessage();
                        uCommons.uSendMessage(uCommons.eMessage);
                        break;
                    }
                }
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    uCommons.eMessage = "Subroutine Call FAILED on uFLIPDATA";
                    uCommons.eMessage += "\n" + e.getMessage();
                    uCommons.uSendMessage(uCommons.eMessage);
                    break;
                }
            }
        }
        if (!NamedCommon.Zmessage.equals("")) {
            uCommons.uSendMessage("ERROR: uFlipRaw: " + NamedCommon.Zmessage);
            NamedCommon.ZERROR = true;
        }
        uCommons.uSendMessage("   .) Finished");
    }

    private static String extract(String inVal, int a, int m, int s) {
        String ans = "";

        a -= 1;
        m -= 1;
        s -= 1;

        int alx = 0, mlx = 0, slx = 0;

        if (a == -1) {
            ans = inVal;
        } else {
            String[] Avars = inVal.split("~~");
            alx = Avars.length;
            if (a > (Avars.length - 1)) {
                ans = "";
            } else {
                if (m == -1) {
                    ans = Avars[a];

                } else {
                    String[] Mvars = Avars[a].split("``");
                    mlx = Mvars.length;
                    if (m > (Mvars.length - 1)) {
                        ans = "";
                    } else {
                        if (s == -1) {
                            ans = Mvars[m];
                        } else {
                            String[] Svars = Mvars[m].split("^^");
                            slx = Svars.length;
                            if (s > (Svars.length - 1)) {
                                ans = "";
                            } else {
                                ans = Svars[s];
                            }
                        }
                    }
                }
            }
        }
        return ans;
    }

    private static int dcount(String inVal, String ams) {
        int ans = 0;
        String xChr = "";
        switch (ams.toUpperCase()) {
            case "A":
                xChr = "~~";
                break;
            case "M":
                xChr = "``";
                break;
            case "S":
                xChr = "^^";
                break;
        }
        ans = inVal.length() - inVal.replace(xChr, "").length();
        ans = (ans / 2) + 1;
        return ans;
    }

    public static int sDcount(String inVal, String iams) {
        String xChr = "";
        switch (iams.toUpperCase()) {
            case "I":
                xChr = NamedCommon.IMark;
                break;
            case "A":
                xChr = NamedCommon.FMark;
                break;
            case "M":
                xChr = NamedCommon.VMark;
                break;
            case "S":
                xChr = NamedCommon.SMark;
                break;
        }

        // Cater for associations with empty strings in them.
        String tmpVal = inVal;
        if (tmpVal.endsWith(xChr)) tmpVal += "~~";
        while (tmpVal.contains(xChr+xChr)) { tmpVal = tmpVal.replace(xChr+xChr, xChr+"~~"+xChr); }

        String[] counter = tmpVal.split(xChr);
        int ans = counter.length;
        tmpVal = "";
        counter = null;
        return ans;
    }

    public static String sExtract(String inVal, int a, int m, int s) {
        String ans = "", thisVar = "";
        a -= 1;
        m -= 1;
        s -= 1;

        if (a == -1) {
            ans = FieldOf(inVal, NamedCommon.IMark, 1);
            if (ans.equals("")) ans = inVal;
        } else {
            String[] Avars = inVal.split(NamedCommon.FMark);
            if (a > (Avars.length - 1)) {
                ans = "";
            } else {
                if (m == -1) {
                    ans = Avars[a];
                } else {
                    thisVar = Avars[a];
                    String[] Mvars = Avars[a].split(NamedCommon.VMark);
                    if (m > (Mvars.length - 1)) {
                        ans = "";
                    } else {
                        if (s == -1) {
                            ans = Mvars[m];
                        } else {
                            thisVar = Mvars[m];
                            String[] Svars = Mvars[m].split(NamedCommon.SMark);
                            if (s > (Svars.length - 1)) {
                                ans = "";
                            } else {
                                thisVar = Svars[s];
                                ans = Svars[s];
                            }
                        }
                    }
                }
            }
        }
        return ans;
    }

    private static String xReplace(String record, int a, int m, int s, String value) {
        if (a == -1) return record + NamedCommon.FMark + value;
        String ans = "";
        if (a == 0) a = 1;
        if (m == 0) m = 1;
        if (s == 0) s = 1;
        // set up data holders ----------------------------
        ArrayList<ArrayList<ArrayList<String>>> uvArray = new ArrayList<ArrayList<ArrayList<String>>>();
        ArrayList<ArrayList<String>> mvArray = new ArrayList<ArrayList<String>>();
        ArrayList<String> svArray = new ArrayList<String>();
        int acnt = 0, mcnt = 0, scnt = 0;
        int amax = 0, mmax = 0, smax = 0;
        // create an empty array --------------------------
        String val = "";
        boolean alldone = false, ReplaceThisValue = false;
        amax = uvArray.size();
        acnt = sDcount(record, "A");
        if (amax < acnt) amax = acnt;
        if (amax < a) amax = a;
        if (amax < 1) amax = 1;
        for (int aa = 0; aa <= amax; aa++) {
            mvArray = new ArrayList<ArrayList<String>>();
            if (aa == 0) {
                svArray.add("");
                mvArray.add(svArray);
                uvArray.add(mvArray);
                continue;
            }
            ReplaceThisValue = (aa == a);
            mcnt = sDcount(sExtract(record, aa, 0, 0), "M");
            mmax = mvArray.size();
            if (mmax < mcnt) mmax = mcnt;
            if (mmax < m && ReplaceThisValue) mmax = m;
            if (mmax < 1) mmax = 1;
            for (int mm = 0; mm <= mmax; mm++) {
                svArray = new ArrayList<String>();
                if (mm == 0) {
                    svArray.add("");
                    mvArray.add(svArray);
                    continue;
                }
                ReplaceThisValue = (aa == a && mm == m);
                smax = svArray.size();
                scnt = sDcount(sExtract(record, aa, mm, 0), "S");
                if (smax < scnt) smax = scnt;
                if (smax < s && ReplaceThisValue) smax = s;
                if (smax < 1) smax = 1;
                for (int ss = 0; ss <= smax; ss++) {
                    if (ss == 0) {
                        svArray.add("");
                        continue;
                    }
                    ReplaceThisValue = (aa == a && mm == m && ss == s);
                    if (ReplaceThisValue) {
                        val = value;
                        alldone = true;
                    } else {
                        val = sExtract(record, aa, mm, ss);
                    }
                    svArray.add(val);
                }
                mvArray.add(svArray);
            }
            uvArray.add(mvArray);
        }
        if (alldone) {
            // repack the data -------------------------------
            for (int aa = 1; aa < uvArray.size(); aa++) {
                mvArray = new ArrayList<ArrayList<String>>(uvArray.get(aa));
                for (int mm = 1; mm < mvArray.size(); mm++) {
                    svArray = new ArrayList<>(uvArray.get(aa).get(mm));
                    for (int ss = 1; ss < svArray.size(); ss++) {
                        ans += uvArray.get(aa).get(mm).get(ss);
                        if (ss + 1 < svArray.size()) ans += NamedCommon.SMark;
                    }
                    if (mm + 1 < mvArray.size()) ans += NamedCommon.VMark;
                }
                if (aa + 1 < uvArray.size()) ans += NamedCommon.FMark;
            }
        } else {
            ans = record;
        }
        return ans;
    }

    public static String sReplace(String record, int a, int m, int s, String value) {
        if (a == -1) return record + NamedCommon.FMark + value;
        UniDynArray tmpDyn = new UniDynArray();
        tmpDyn = SQL2UVRec(record);
        tmpDyn.replace(a, m, s, value);
        String answer = UV2SQLRec(null, tmpDyn);
        if (answer.contains(NamedCommon.IMark)) answer = uCommons.FieldOf(answer, NamedCommon.IMark, 2);
        tmpDyn = null;
        return answer;
    }

    public static int CountValues(String marker, String values) {
        int idx = 0, cnt = 0;
        while (idx > -1) {
            idx = values.indexOf(marker, idx);
            if (idx > -1) {
                cnt++;
                idx++;
            }
        }
        return cnt;
    }

    private static void uCopyTakenALL() {

        String inFile = NamedCommon.uTake.getFileName(), dct = "";
        int ctr = 0, totCtr = 0;
        uCommons.uSendMessage("         Flipping data from " + NamedCommon.uTake.getFileName());
        uSelectFile(NamedCommon.uTake);
        uReadNextID();
        while (!NamedCommon.uSelect.isLastRecordRead()) {
            ctr++;
            totCtr++;
            if (ctr > NamedCommon.showAT) {
                uCommons.uSendMessage("         ... flipped " + ctr + " records");
                ctr = 0;
            }
            FlipLoaded(NamedCommon.uID, inFile);
            uReadNextID();
        }
        if (totCtr > 0) {
            uCommons.eMessage = "         " + totCtr + " Record(s) moved from TAKE";
            uCommons.eMessage = uCommons.eMessage + " to LOADED";
        } else {
            uCommons.eMessage = inFile + " was empty - no records moved!";
        }
        uCommons.uSendMessage(uCommons.eMessage);
    }

    private static void uCopyTakenItem() {
        uCommons.uSendMessage("       Copy Taken ... item");
        FlipLoaded(NamedCommon.uID, NamedCommon.u2Source);
    }

    private static void FlipLoaded(UniString uID, String inFile) {
        // flip one record
        boolean SkipRec = false;
        try {
            NamedCommon.uNewRec = NamedCommon.uTake.read(uID);
        } catch (UniFileException e) {
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            SkipRec = true; // cannot md5 a null unistring !!
        }
        if (!SkipRec) {
            while (true) {
                try {
                    NamedCommon.uLoaded.setRecordID(uID);
                    NamedCommon.uLoaded.setRecord(NamedCommon.uNewRec);
                    NamedCommon.uLoaded.write();
                    NamedCommon.uTake.deleteRecord(uID);
                    break;
                } catch (UniFileException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        uCommons.eMessage = "Write FAILURE to " + inFile + ".LOADED :: @ID " + uID;
                        uCommons.uSendMessage(uCommons.eMessage);
                        uCommons.uSendMessage(e.getMessage());
                        uCommons.eStatus = 1;
                        if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                        NamedCommon.ZERROR = true;
                        break;
                    }
                }
            }
        }
    }

    /**********************************************************************************************/

    public static String CheckU2Controls() {
        String rtnString = ReadAnItem("BP.UPL", "STOP", "", "", "").toLowerCase();
        while (NamedCommon.ZERROR) {
            // probably a lost or stale connection to the source DB
            NamedCommon.ZERROR = false;
            SourceDB.ReConnect();
        }
        if (rtnString.contains("stop")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "<<FAIL>> rFuel STOP switch is set on";
            uCommons.uSendMessage(NamedCommon.Zmessage);
            return NamedCommon.Zmessage;
        } else {
            rtnString = "<<PASS>>";
            NamedCommon.U2File = uClose(NamedCommon.U2File);
        }
        return rtnString;
    }

    public static String bCleanse(String inVal) {
        // brace cleanse : remove { and } from value
        inVal = inVal.replaceAll("\\{", "<bo>");
        inVal = inVal.replaceAll("\\}", "<bc>");
        return  inVal;
    }

    public static boolean DeleteAnItem(String inFile, String inItem) {
        boolean okay = false;
        if (uOpenFile(inFile, "2")) {
            while (true) {
                try {
                    NamedCommon.U2File.setRecordID(inItem);
                    NamedCommon.U2File.deleteRecord();
                    okay = true;
                    break;
                } catch (UniFileException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                        // loop and do again when reconnected
                    } else {
                        uSendMessage("DeleteAnItem ERROR: " + e.getMessage());
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "";
                        break;
                    }
                }
            }
        }
        return okay;
    }

    public static boolean WriteAnItem(String inFile, String inItem, String a, String m, String s, String datum) {
        switch (NamedCommon.protocol) {
            case "u2cs":
                while (datum.contains(NamedCommon.SMark)) { datum = datum.replace(NamedCommon.SMark, UniObjectsTokens.AT_SVM); }
                while (datum.contains(NamedCommon.VMark)) { datum = datum.replace(NamedCommon.VMark, UniObjectsTokens.AT_VM);  }
                while (datum.contains(NamedCommon.FMark)) { datum = datum.replace(NamedCommon.FMark, "\n");  }

                UniFile uplFile = uOpen(inFile);
                if (NamedCommon.ZERROR) return false;

                if (a.equals("") || a.equals("0")) {
                    uWriter(uplFile, inItem, datum);
                } else {
                    int av = Integer.valueOf(a);
                    UniDynArray rec = new UniDynArray(uRead(uplFile, inItem));
                    if (m.equals("") || m.equals("0")) {
                        rec.replace(av, 0, datum);
                    }  else {
                        int mv = Integer.valueOf(m);
                        if (s.equals("") || s.equals("0")) {
                            rec.replace(av, mv, datum);
                        } else {
                            int sv = Integer.valueOf(s);
                            rec.replace(av, mv, sv, datum);
                        }
                    }
                    while (true) {
                        try {
                            uplFile.setRecordID(inItem);
                            uplFile.setRecord(rec);
                            uplFile.write();
                            break;
                        } catch (UniFileException e) {
                            if (!u2Commons.TestAlive()) {
                                SourceDB.ReconnectService();
                            } else {
                                uCommons.uSendMessage("Write failue on " + uplFile.getFileName() + "  " + e.getMessage());
                                NamedCommon.ZERROR = true;
                                NamedCommon.Zmessage = "";
                                return false;
                            }
                        }
                    }
                    rec = null;
                }
                break;
            case "real":
                String file = "{file=" + inFile + "}";
                String item = "{item=" + inItem + "]";
                String data = "{data=" + datum + "}";
                String atr  = "{atr="  + a + "}";
                String mv   = "{mv="   + m + "}";
                String sv   = "{sv="   + s + "}";
                String cStr = "{WRI}" + file + item + data + atr + mv + sv;
                String rtnString = u2Commons.MetaBasic(cStr);
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
                return false;
        }
        return true;
    }

    public static String ReadAnItem(String inFile, String inItem, String a, String m, String s) {
        if (NamedCommon.protocol.equals("real")) if (inFile.equals("VOC")) inFile = "MD";
        String rtnString, request;
        String hush = "{hush=true}";
        String file = "{file=" + bCleanse(inFile) + "}";
        String item = "{item=" + bCleanse(inItem) + "}";
        String data = "{data=}";
        String atr  = "{atr=" + a + "}";
        String mv   = "{mv=" + m + "}";
        String sv   = "{sv=" + s + "}";
        String pr   = "{protocol=" + NamedCommon.protocol + "}";

        request     = "{RDI}" + hush + file + item + data + atr + mv + sv + pr;

        switch (NamedCommon.protocol) {
            case "u2cs":
                rtnString = cc_unics(inFile, inItem, a, m, s);
                break;
            case "real":
                rtnString = u2Commons.MetaBasic(request);
                break;
            case "u2mnt":
                rtnString = cc_uMount("{RDI}" + file + item + atr + mv + sv);
                break;
            case "u2sockets":
                rtnString = cc_uSocket("{socket}\n{RDI}" + file + item + atr + mv + sv);
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
            default:
                rtnString = cc_unics(inFile, inItem, a, m, s);
                break;
        }
        return rtnString;
    }

    private static String cc_unics(String inFile, String inItem, String a, String m, String s) {
        String ans = "";
        boolean okay = uOpenFile(inFile, "2");
        if (okay) {
            UniDynArray rec = null;
            NamedCommon.dynRec = null;
            if (!NamedCommon.ZERROR) {
                rec = new UniDynArray(uRead(NamedCommon.U2File, inItem));
                NamedCommon.dynRec = rec;
                rec = null;
            } else {
                if (NamedCommon.debugging) uCommons.uSendMessage( inItem + " not in file " + inFile);
                rec = null;
                return "";
            }
        }

        if (inFile.equals("BP.UPL") && inItem.equals("STOP")) return uCommons.UV2SQLRec(null, NamedCommon.dynRec);
        if (APImsg.APIget("task").equals("050")) return "";

        int atr, mv, sv;
        if (a.equals("")) {
            UniString ustr = new UniString(inItem);
            ans = uCommons.UV2SQLRec(ustr, NamedCommon.dynRec);
            ustr = null;
        } else {
            UniDynArray uChk = new UniDynArray(NamedCommon.dynRec);
            try {
                atr = Integer.valueOf(a);
            } catch (NumberFormatException nfe) {
                atr = 0;
            }
            try {
                mv = Integer.valueOf(a);
            } catch (NumberFormatException nfe) {
                mv = 0;
            }
            try {
                sv = Integer.valueOf(a);
            } catch (NumberFormatException nfe) {
                sv = 0;
            }
            ans = String.valueOf(uChk.extract(atr, mv, sv));
            uChk = null;
        }
        return ans;
    }

    public static String cc_uMount(String inReq) {
        String msv = inReq.substring(0, inReq.indexOf("}") + 1);
        String ans = "";
        inReq = "{mount}\n" + inReq;
        uMountCommons.timeout = 0;
        if (inReq.indexOf("!") > 0) {
            try {
                uMountCommons.timeout = Integer.valueOf(inReq.substring(0, inReq.indexOf("!")));
            } catch (NumberFormatException nfe) {
                uMountCommons.timeout = 10;
            }
            inReq = inReq.substring(inReq.indexOf("!") + 1, inReq.length());
        }

        if (uMountCommons.mntRequests != null) {
            try {
                uMountCommons.mntRequests.close();
            } catch (IOException e) {
                uCommons.uSendMessage("   .) Cannot close mntRequests - " + e.getMessage());
            }
        }
        if (uMountCommons.mntResponse != null) {
            try {
                uMountCommons.mntResponse.close();
            } catch (IOException e) {
                uCommons.uSendMessage("   .) Cannot close mntResponse - " + e.getMessage());
            }
        }

        String uuid = String.valueOf(UUID.randomUUID());
        uuid = APImsg.APIget("CORRELATIONID") + "-" + msv + "-" + uuid;
        if (NamedCommon.debugging) uCommons.uSendMessage("Sending " + uuid);
        uMountCommons.writeReq(uuid, inReq);
        boolean loopsw = true;
        if (!NamedCommon.ZERROR) {
            ans = uMountCommons.readResp(String.valueOf(uuid));
            if (uMountCommons.mntRequests != null) {
                try {
                    uMountCommons.mntRequests.close();
                    uMountCommons.mntRequests = null;
                } catch (IOException e) {
                    uCommons.uSendMessage("   .) Cannot close mntRequests - " + e.getMessage());
                }
            }
            if (uMountCommons.mntResponse != null) {
                try {
                    uMountCommons.mntResponse.close();
                    uMountCommons.mntResponse = null;
                } catch (IOException e) {
                    uCommons.uSendMessage("   .) Cannot close mntResponse - " + e.getMessage());
                }
            }
        }
        if (ans.substring(0, 1).equals("{")) {
            ans = ans.substring(1, ans.length() - 1);
            int st = ans.indexOf("=");
            if (st == (ans.length() - 1)) {
                ans = "";
            } else {
                ans = ans.substring(st + 1, ans.length());
                if (ans.endsWith("}")) ans = ans.substring(0, ans.length() - 1);
            }
        }
        return ans;
    }

    public static String cc_uSocket(String inReq) {
        String ans = "";
        if (!inReq.substring(0, 8).equals("{socket}")) {
            inReq = "{socket}\n" + inReq;
        }
        uSocketCommons.SocketWriter(uSocketCommons.writerSocket, inReq);
        ans = uSocketCommons.SocketReader(uSocketCommons.readerSocket);
        if (ans.startsWith("{")) {
            ans = ans.substring(1, ans.length());
            if (ans.endsWith("}")) ans = ans.substring(0, ans.length() - 1);
            int st = ans.indexOf("=");
            if (st == (ans.length() - 1)) {
                ans = "";
            } else {
                ans = ans.substring(st + 1, ans.length());
            }
        }
        return ans;
    }

    public static String MetaBasic(String inVal) {
        String outVal = "";
        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        if (NamedCommon.ZERROR) return "";
        switch (NamedCommon.protocol) {
            case "real":
                outVal = rCommons.realSub(inVal);
                break;
            case "u2cs":
                String[] callArgs = new String[]{"", inVal};
                callArgs = uniCallSub("SR.METABASIC", callArgs);
                outVal = callArgs[0];
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
        }
        return outVal;
    }

    public static void u2cs_CallFetchPrep(String subr, String[] args) {
        boolean show = true;
        String sel = args[0];
        String listName = args[3];
        while (true) {
            try {
                if (subr.indexOf("!") > -1) subr = FieldOf(subr, "!", 2);
                UniSubroutine fetchPrep = NamedCommon.uSession.subroutine(subr, 6);
                if (show) {
                    uCommons.uSendMessage("   .) Prepare fetch list");
                    if (!sel.equals("")) {
                        uCommons.uSendMessage("   .) ");
                        uCommons.uSendMessage("   >> Select: " + sel);
                        uCommons.uSendMessage("   .) ");
                    }
                    if (NamedCommon.RunType.equals("INCR")) {
                        uCommons.uSendMessage("     .>> This may take a while");
                    }
                }

                fetchPrep.setArg(0, args[0]);
                fetchPrep.setArg(1, args[1]);
                fetchPrep.setArg(2, args[2]);
                fetchPrep.setArg(3, args[3]);
                fetchPrep.setArg(4, args[4]);
                fetchPrep.setArg(5, args[5]);

                if (NamedCommon.debugging) {
                    String callSub = GetUniSubDets(fetchPrep);
                    uCommons.uSendMessage(callSub);
                }
                fetchPrep.call();
                if (NamedCommon.isWhse || NamedCommon.isPrt) {
                    uCommons.uSendMessage("   .) Give uPREP a 5 second head start");
                    uCommons.Sleep(5);
                }
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Session.subroutine(" + subr + ")  " + e.getMessage();
                    uCommons.uSendMessage("!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-! ");
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    if (show) uCommons.uSendMessage("FETCH subroutines are missing - ERR001");
                    uCommons.uSendMessage("!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-! ");
                    uCommons.uSendMessage(" ");
                    break;
                }
            } catch (UniSubroutineException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    NamedCommon.ZERROR = true;
                    uCommons.uSendMessage("UniSubroutine(" + subr + ") Error: " + e.getMessage());
                    break;
                }
            }
        }
    }

    public static String GetUniSubDets(UniSubroutine uSub) {
        String callSub = "      .> ";
        callSub += "CALL " + uSub.getRoutineName() + " (";
        int nbrArgs = uSub.getNumArgs();
        for (int ss = 0; ss < nbrArgs; ss++) {
            if (ss > 0) callSub += ",";
            while (true) {
                try {
                    callSub += "\"" + uSub.getArg(ss) + "\" ";
                    break;
                } catch (UniSubroutineException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        uCommons.uSendMessage("Error decyphering uniSub " + e.getMessage());
                        break;
                    }
                }
            }
        }
        callSub += ")";
        return callSub;
    }

    public static String u2cs_CallFetchData(String sourceFile, String[] args) {
//        args[0] = empty - response from UV
//        args[1] = NamedCommon.U2File.getFileName();
//        args[2] = String.valueOf(aFrom);
//        args[3] = String.valueOf(aFrom + aTo);
//        args[4] = String.valueOf(NamedCommon.RQM);
//        args[5] = CorrelID;

        String reply = "";

        while (true) {
            try {
                UniSubroutine fetchData = NamedCommon.uSession.subroutine("uFETCH", 6);
                try {
                    boolean loopSw = true;
                    String ffr = "", tto = "";
                    int slowcnt = 0;
                    reply = "";
                    while (loopSw) {
                        if (NamedCommon.RQM < NamedCommon.aStep) uCommons.Sleep(1);
                        fetchData.setArg(0, "");
                        fetchData.setArg(1, args[1]);
                        fetchData.setArg(2, args[2]);
                        fetchData.setArg(3, args[3]);
                        fetchData.setArg(4, args[4]);
                        fetchData.setArg(5, args[5]);
                        if (NamedCommon.debugging) {
                            String callSub = GetUniSubDets(fetchData);
                            uCommons.uSendMessage(callSub);
                        }

                        startM = System.nanoTime();
                        fetchData.call();
                        finishM = System.nanoTime();
                        laps = (finishM - startM);
                        laps = laps / div;

                        reply = fetchData.getArg(0);

                        if (reply.length() < 1) {
                            ffr = oconvM(String.valueOf(args[2]), "MD0,");
                            tto = oconvM(String.valueOf(args[3]), "MD0,");
                            slowcnt++;

                            System.out.println(" ");
                            uCommons.uSendMessage("   .) Fetching  " + ffr + " to " + tto + " for CorrID " + args[5] + " received NOTHING. (2)");
                            uCommons.uSendMessage("   .) Call took " + laps + " second(s)");
                            uCommons.uSendMessage("   .) SourceDB running slow ... wait 10 seconds (STOP switch??)" + " (" + slowcnt + " of 5)");

                            uCommons.Sleep(10);

                            if (slowcnt >= 5) {
                                loopSw = false;
                                System.out.println( " ");
                                uCommons.uSendMessage("Connection with SourceDB may be faulty. Restart procedures invoked ...." );
//                                uCommons.ShutDown();  // don't shutdown - it stops EVERYTHING
                                reply = "<<FAIL>>";
                            }
                        } else {
                            loopSw = false;
                        }
                    }
                    break;
                } catch (UniSubroutineException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        uCommons.uSendMessage("*");
                        uCommons.uSendMessage("!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!");
                        uCommons.uSendMessage("!-");
                        uCommons.uSendMessage("!- DB error " + e.getMessage());
                        uCommons.uSendMessage("!-");
                        uCommons.uSendMessage("!-      Source DB unreachable      - UPL-002         -!");
                        uCommons.uSendMessage("            Will try again in 5 seconds              -!");
                        uCommons.uSendMessage("!-");
                        uCommons.uSendMessage("!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!");
                        uCommons.uSendMessage("*");
                        uCommons.Sleep(5);
                    }
                }
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    uCommons.uSendMessage(" ");
                    uCommons.uSendMessage("!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-! ");
                    uCommons.uSendMessage(e.getMessage());
                    uCommons.uSendMessage("FETCH subroutines are missing - ERR001");
                    uCommons.uSendMessage("!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-!-! ");
                    uCommons.uSendMessage(" ");
                    NamedCommon.ZERROR = true;
                    break;
                }
            }
        }
        return reply;
    }

    public static String u2mnt_CallSrtn(String subr, String[] arglist) {
        String reply = "";
        String args = "";
        String slow = "";
        int iTst = subr.indexOf("!");
        if (subr.indexOf("!") > 0) {
            slow = subr.substring(0, subr.indexOf("!") + 1);
            subr = subr.substring(subr.indexOf("!") + 1, subr.length());
        }
        int nbrArgs = arglist.length;
        args = arglist[0];
        for (int x = 1; x <= nbrArgs; x++) {
            if (arglist[x].equals("[<END>]")) {
                break;
            } else {
                args += "<fm>" + arglist[x];
            }
        }
        reply = cc_uMount(slow + "{SRT}" + "{subr=" + subr + "}{args=" + args + "}");
        return reply;
    }

    public static String u2sock_CallSrtn(String subr, String[] arglist) {
        String reply = "";
        String args = "";
        String slow = "";
        int iTst = subr.indexOf("!");
        if (subr.indexOf("!") > 0) {
            slow = subr.substring(0, subr.indexOf("!") + 1);
            subr = subr.substring(subr.indexOf("!") + 1, subr.length());
        }
        int nbrArgs = arglist.length;
        args = arglist[0];
        for (int x = 1; x <= nbrArgs; x++) {
            if (arglist[x].equals("[<END>]")) {
                break;
            } else {
                args += "<fm>" + arglist[x];
            }
        }
        reply = cc_uSocket(slow + "{socket}\n{SRT}" + "{subr=" + subr + "}{args=" + args + "}");
        return reply;
    }

    public static void CleanupLists(String listname) {
        if (listname.equals("")) return;
        String delcmd = "", selcmd = "", slName = "";
        if (NamedCommon.databaseType.equals("UNIVERSE")) {
            slName = "&SAVEDLISTS&";
        } else {
            slName = "SAVEDLISTS";
        }
        selcmd = "SELECT " + slName + " LIKE ..." + listname + "...";
        String cmd = "{CLF}{file=" + slName + "}{sel=" + selcmd + "}";
        String rtnString = "";
        uCommons.uSendMessage("   .) Clean up saved lists");
        switch (NamedCommon.protocol) {
            case "u2cs":
                String metaSrtn = "{CLF}";
                String metaFile = "{file="+slName+"}";
                String metaSel  = "{sel=SELECT " + slName + " LIKE ..."+ listname +"...}";
                String metaCstr = metaSrtn + metaFile + metaSel;
                String metaRtnStr = MetaBasic(metaCstr);
                metaRtnStr = uCommons.FieldOf(metaRtnStr, "=", 2);
                metaRtnStr = uCommons.FieldOf(metaRtnStr, "}", 1);
                boolean metaPass = metaRtnStr.toUpperCase().equals("OK");
                if (!metaPass) {
                    // This is the slowest way of doing this !!!
                    uCommons.uSendMessage("   .) " + selcmd);
                    UniFile iofile = null;
                    if (uOpenFile(slName, "2")) {
                        iofile = NamedCommon.U2File;
                        delcmd = "DELETE " + slName + " ";
                        boolean success;
                        ArrayList<String> delKeys = new ArrayList<String>();
                        uSelectFile(iofile);
                        uReadNextID();
                        // empty the select list
                        while (uCommons.eStatus == 0 && NamedCommon.uID.length() > 0) {
                            if (String.valueOf(NamedCommon.uID).startsWith(listname))
                                delKeys.add(String.valueOf(NamedCommon.uID));
                            uReadNextID();
                        }
                        int nbrDels = delKeys.size();
                        if (nbrDels > 0) {
                            uCommons.uSendMessage("     .> deleting " + nbrDels + " items");
                            cmd = "";
                            for (int dd = 0; dd < nbrDels; dd++) {
                                cmd = delcmd + delKeys.get(dd);
                                uniExec(cmd);
                            }
                            uCommons.uSendMessage("   .) " + slName + " for " + listname + " cleared");
                        }
                    }
                }
                break;
            case "real":
                break;
            case "u2mnt":
                rtnString = cc_uMount("{socket}\n" + cmd);
                break;
            case "u2sockets":
                rtnString = cc_uSocket("{socket}\n" + cmd);
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
        }
    }

    public static UniString uRead(UniFile inFile, String inID) {
        UniString ans = null;
        while (true) {
            try {
                inFile.setRecordID(inID);
                ans = inFile.read();
                break;
            } catch (UniFileException e) {
                if (isManaged) { ans = null; break; }
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    ans = uCommons.SQL2UVRec("");
                    if (NamedCommon.debugging)
                        uCommons.uSendMessage("   .) " + inID + " not in file " + inFile.getFileName());
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    break;
                }
            }
        }
        return ans;
    }

    public static boolean TestAlive() {
        try {
            NamedCommon.VOC.setRecordID("BP.UPL");
            uniString = NamedCommon.VOC.read();
            uniString = null;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean uOpenFile(String inFile, String OpType) {
        // OpType:      1   Open base file with Take and Loaded     //
        // OpType:      2   Open the file passed in ONLY            //

        if (inFile.equals("VOC")) {
            NamedCommon.U2File = NamedCommon.VOC;
            return true;
        }
        if (NamedCommon.protocol.equals("real")) return true;

        if (NamedCommon.debugging) uCommons.uSendMessage("uOpenFile(" + inFile + "," + OpType + ")");
        if (!NamedCommon.sConnected) {
            uCommons.uSendMessage("Connecting to SourceDB");
            SourceDB.ConnectSourceDB();
        }
        if (NamedCommon.uSession == null) {
            SourceDB.DisconnectSourceDB();
            SourceDB.ConnectSourceDB();
        }
        boolean ans = false;
        String pointer;

        // handle change in Data Acct in Message

        String msgDacct = APImsg.APIget("dacct");
        if (!NamedCommon.datAct.equals(msgDacct)) {
            if (!msgDacct.equals("")) NamedCommon.datAct = APImsg.APIget("dacct");
        }
        msgDacct = "";

        pointer = "_" + NamedCommon.datAct;
        inFile = inFile.replaceAll("\\ ", "_");
        String takeFile = inFile + "_" + NamedCommon.datAct + ".TAKE";
        String loadFile = inFile + "_" + NamedCommon.datAct + ".LOADED";
        if (rfuelFiles.contains(inFile)) {
            pointer = "";
            OpType = "2"; // NEVER need the TAKE or LOADED files for these.
        } else {
            if (!inFile.endsWith(NamedCommon.datAct)) {
                if (NamedCommon.isRest) pointer += "_" + NamedCommon.pid; // NamedCommon.MessageID;
                if (NamedCommon.isWhse) pointer += "_" + NamedCommon.pid;
                inFile += pointer;
            }
        }
        String dat = "", dct = "", orgfile = "", tmp = "";
        int lx = loadFile.length() + 2;
        // Handle Dictionaries
        dat = inFile;
        if (inFile.startsWith("DICT_")) {
            if (inFile.length() < 6) return false;
            uCommons.uSendMessage("   .) [" + inFile + "] - is using the DICTionary as a data source");
            dct = "DICT ";
            lx = lx + 5;
            dat = inFile.substring(5, inFile.length());
        }
        // Handle multi-layout files
        dat = uCommons.FirstPartOf(dat, ";"); // FILE;_DATA -- > FILE
        dat = uCommons.FirstPartOf(dat, ":"); // FILE:_DATA -- > FILE
        dat = uCommons.FirstPartOf(dat, "@"); // FILE@_DATA -- > FILE
        dat = uCommons.FirstPartOf(dat, "!"); // FILE!_DATA -- > FILE
        //
        inFile = dat;
        if (!inFile.endsWith(pointer)) inFile += pointer;
        if (OpType.equals("1")) {
            if (inFile.endsWith(pointer)) {
                orgfile = inFile;
            } else {
                orgfile = inFile + pointer;
            }

            // ## ANDY ##
            if (!rfuelFiles.contains(inFile) && !orgfile.startsWith("upl_")) orgfile = "upl_" + orgfile;

            if (NamedCommon.VOC == null) {
                OpenVOC();
                if (NamedCommon.ZERROR) return false;
            }
            if (!NamedCommon.VOC.isOpen()) {
                OpenVOC();
                if (NamedCommon.ZERROR) return false;
            }
            if (!rfuelFiles.contains(inFile)) {
                uCommons.uSendMessage("   .) Removing " + orgfile + " from VOC");
                while (true) {
                    try {
                        if (!rfuelFiles.contains(orgfile)) {
                            NamedCommon.VOC.setRecordID(orgfile);
                            NamedCommon.VOC.deleteRecord();
                            int vocPx = NamedCommon.PointerFiles.indexOf(orgfile);
                            if (vocPx > -1) NamedCommon.PointerFiles.remove(vocPx);
                            uCommons.uSendMessage("   .) VOC entry for " + orgfile + " was deleted and will now be recreated.");
                        }
                        break;
                    } catch (UniFileException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                            uCommons.uSendMessage("   .) VOC entry for " + orgfile + " will now be created.");
                            break;
                        }
                    }
                }
            }
        } else {
            orgfile = inFile;

            // ## ANDY ##
            if (!rfuelFiles.contains(inFile) && !orgfile.startsWith("upl_")) orgfile = "upl_" + orgfile;

            if (rfuelFiles.contains(orgfile)) {
                NamedCommon.U2File = uOpen(orgfile);
                return !NamedCommon.ZERROR;
            }
            // fall through to the logic below
        }

        ans = false;
        if (NamedCommon.debugging) uCommons.uSendMessage("Trying to open " + orgfile);
        while (true) {
            if (NamedCommon.U2File != null) {
                if (!NamedCommon.U2File.getFileName().equals(orgfile)) NamedCommon.U2File = null;
            }
            if (NamedCommon.U2File != null) {
                if (!NamedCommon.U2File.getFileName().equals(orgfile)) {
                    try {
                        if (dct.equals("")) {
                            if (NamedCommon.OpenFiles.indexOf(orgfile) < 0) {
                                if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + orgfile);
                                NamedCommon.U2File = NamedCommon.uSession.open(orgfile);
                                NamedCommon.OpenFiles.add(orgfile);
                                NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(orgfile), NamedCommon.U2File);
                            } else {
                                NamedCommon.U2File = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(orgfile));
                            }
                        } else {
                            if (NamedCommon.OpenFiles.indexOf(orgfile) < 0) {
                                if (NamedCommon.debugging) uCommons.uSendMessage("Open  DICT " + orgfile);
                                NamedCommon.U2File = NamedCommon.uSession.openDict(orgfile);
                                NamedCommon.OpenFiles.add(orgfile);
                                NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(orgfile), NamedCommon.U2File);
                            } else {
                                NamedCommon.U2File = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(orgfile));
                            }
                        }
//                        int lx2=NamedCommon.U2File.getFileName().length();
//                        if (lx2 > lx) lx = lx2;
                        tmp = uCommons.LeftHash(NamedCommon.U2File.getFileName(), lx);
                        if (!(rfuelFiles).contains(orgfile)) uCommons.uSendMessage("   .) " + tmp + " is open");
                        // the file is already open and ready
                        break;
                    } catch (UniSessionException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            uCommons.uSendMessage("File open error: " + orgfile);
                            ans = false;
                            break;
                        }
                    }
                }
                ans = true;
                break;
            } else {
                while (true) {
                    try {
                        if (!(rfuelFiles).contains(orgfile) || NamedCommon.debugging) {
                            uCommons.uSendMessage("   .) Open " + orgfile);
                        }
                        if (dct.equals("")) {
                            if (NamedCommon.OpenFiles.indexOf(orgfile) < 0) {
//                                if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + orgfile);
                                NamedCommon.U2File = NamedCommon.uSession.open(orgfile);
                                NamedCommon.OpenFiles.add(orgfile);
                                NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(orgfile), NamedCommon.U2File);
                            } else {
                                NamedCommon.U2File = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(orgfile));
                                uCommons.uSendMessage("   .) " + orgfile + " is available from u2Handles(" + NamedCommon.OpenFiles.indexOf(orgfile) + ")");
                            }
                        } else {
                            if (NamedCommon.OpenFiles.indexOf(orgfile) < 0) {
                                if (NamedCommon.debugging) uCommons.uSendMessage("Open  DICT " + orgfile);
                                NamedCommon.U2File = NamedCommon.uSession.openDict(dct + orgfile);
                                NamedCommon.OpenFiles.add(orgfile);
                                NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(orgfile), NamedCommon.U2File);
                            } else {
                                NamedCommon.U2File = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(orgfile));
                                uCommons.uSendMessage("   .) " + orgfile + " is available from u2Handles(" + NamedCommon.OpenFiles.indexOf(orgfile) + ")");
                            }
                        }
                        ans = true;
                        break;
                    } catch (UniSessionException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
//                            if (e.getMessage().contains("RPC")) NamedCommon.ConnectionError = true;
                            if (!rfuelFiles.contains(orgfile) && !NamedCommon.ConnectionError) {
                                uCommons.uSendMessage("   .) File " + orgfile + " not found.");
                                if (!dct.equals("")) {
                                    uCommons.uSendMessage("   .) Set-File to " + NamedCommon.datAct + " " + dat + " as " + orgfile);
                                    uCommons.uSendMessage("   .) Need access to the DICTionary of " + dat);
                                } else {
                                    uCommons.uSendMessage("   .) Set-File to " + NamedCommon.datAct + " " + NamedCommon.u2Source + " as " + orgfile);
                                }
                                if (!SetFile(NamedCommon.u2Source, orgfile)) {
                                    //  changed inFile to orgfile
                                    uCommons.uSendMessage("   .) Set-File failed for " + orgfile);
                                    ans = false;
                                    NamedCommon.ZERROR = true;
                                    return ans;
                                }
//                                int lx2=NamedCommon.U2File.getFileName().length();
//                                if (lx2 > lx) lx = lx2;
                                uCommons.uSendMessage("   .) " + uCommons.LeftHash(dct + orgfile, lx) + " is open");
                                ans = true;
                                break;
                            } else {
                                uCommons.uSendMessage("uOpenFile() WARNING: SourceDB connection may have gone stale. Reconnecting now.");
                            }
                            break;
                        }
                    }
                }
            }
        }

        if (OpType.equals("1") && ans) {
            ans = false;
            while (true) {
                try {
                    if (NamedCommon.OpenFiles.indexOf(takeFile) < 0) {
                        if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + takeFile);
                        NamedCommon.uTake = NamedCommon.uSession.open(takeFile);
                        NamedCommon.OpenFiles.add(takeFile);
                        NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(takeFile), NamedCommon.uTake);
                    } else {
                        NamedCommon.uTake = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(takeFile));
                    }
                    tmp = uCommons.LeftHash(takeFile, lx);
                    uCommons.uSendMessage("   .) " + tmp + " is open");
                    ans = true;
                    break;
                } catch (UniSessionException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        if (!CreateFile(takeFile, NamedCommon.uTake)) {
                            uCommons.uSendMessage("CREATE.FILE failed for " + takeFile);
                            ans = false;
                            break;
                        }
                    }
                }
            }
            if (ans) {
                ans = false;
                while (true) {
                    try {
                        if (NamedCommon.OpenFiles.indexOf(loadFile) < 0) {
                            if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + loadFile);
                            NamedCommon.uLoaded = NamedCommon.uSession.open(loadFile);
                            NamedCommon.OpenFiles.add(loadFile);
                            NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(loadFile), NamedCommon.uLoaded);
                        } else {
                            NamedCommon.uLoaded = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(loadFile));
                        }
                        tmp = uCommons.LeftHash(loadFile, lx);
                        uCommons.uSendMessage("   .) " + tmp + " is open");
                        ans = true;
                        break;
                    } catch (UniSessionException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            if (!CreateFile(loadFile, NamedCommon.uLoaded)) {
                                uCommons.uSendMessage("CREATE.FILE failed for " + loadFile);
                                ans = false;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return ans;
    }

    public static void OpenVOC() {
        if (NamedCommon.protocol.equals("real")) return;
        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        if (NamedCommon.uSession == null) SourceDB.ConnectSourceDB();
        if (NamedCommon.VOC != null) {
            if (NamedCommon.VOC.isOpen()) return;
            while (true) {
                try {
                    NamedCommon.VOC.close();
                    break;
                } catch (UniFileException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                        uCommons.uSendMessage("ISSUE when trying to close(VOC) set to \"null\"");
                        NamedCommon.VOC = null;
                        break;
                    }
                }
            }
        }
        while (true) {
            try {
                if (NamedCommon.debugging) uCommons.uSendMessage("Open  VOC");
                NamedCommon.VOC = NamedCommon.uSession.open("VOC");
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    uCommons.uSendMessage("***********************************************************");
                    uCommons.uSendMessage("FATAL - cannot open VOC");
                    uCommons.uSendMessage("DB Error Type: [" + e.getErrorType() +
                            "]   Code: [" + e.getErrorCode() +
                            "]   Descr: " + e.getMessage());
                    uCommons.uSendMessage("Is rFuel.properties defined correctly? Is rFuel in a proper account?");
                    uCommons.uSendMessage("***********************************************************");
                    uCommons.uSendMessage("Restarting this process now.");
                    if (!NamedCommon.isWebs) {
                        String message = APImsg.BuildMessage();
                        String broker = uCommons.GetNextBkr(NamedCommon.Broker);
                        Hop.start(message, "", broker, NamedCommon.inputQueue, "", NamedCommon.CorrelationID.replaceAll("\\.", "_"));
                        System.exit(1);
                    }
                }
            }
        }
    }

    public static UniFile uOpen(String inFile) {
        if (!NamedCommon.sConnected) {
            if (SourceDB.ConnectSourceDB().contains("<<FAIL>>")) {
                NamedCommon.ZERROR = true;
                return null;
            }
        }
        if (NamedCommon.uSession == null) {
            if (SourceDB.ConnectSourceDB().contains("<<FAIL>>")) {
                NamedCommon.ZERROR = true;
                return null;
            }
        }

        UniFile theFile = null;
        String dict = "";

        if (inFile.equals("VOC")) {
            if (NamedCommon.VOC != null) {
                if (NamedCommon.VOC.isOpen()) return NamedCommon.VOC;
                while (true) {
                    try {
                        NamedCommon.VOC = NamedCommon.uSession.open("VOC");
                        break;
                    } catch (UniSessionException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            uCommons.uSendMessage("Cannot open VOC file !!");
                            NamedCommon.ZERROR = true;
                            break;
                        }
                    }
                }
                return NamedCommon.VOC;
            }
        }
        if ((rfuelFiles).contains(inFile)) {
            // if the inFile is an rFuel file and cannot be opened - it must be a new installation
            // LoadUplBp will OpenCreate these files.
            while (true) {
                try {
                    NamedCommon.U2File = NamedCommon.uSession.open(inFile);
                    break;
                } catch (UniSessionException ee) {
                    if (stopProc) {
                        uCommons.uSendMessage("   This account is not set-up for rFuel.");
                        uCommons.uSendMessage("   Please run the 910 Load-UDE-Programs message.");
                        NamedCommon.U2File = null;
                        NamedCommon.ZERROR = true;
                        break;
                    }
                    if (isManaged) {
                        NamedCommon.U2File = null;
                        break;
                    }
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        uCommons.uSendMessage("   .) " + inFile + " does not exist.");
                        NamedCommon.U2File = null;
                        NamedCommon.ZERROR = true;
                        break;
                    }
                }
            }
            return NamedCommon.U2File;
        }

        String extn = "_" + NamedCommon.datAct + "_" + NamedCommon.pid; // NamedCommon.MessageID;
        String vocName = "upl_" + inFile;
//        String vocName = inFile;
        if (!vocName.endsWith(extn)) vocName += extn;
        String chk = vocName.replace("DICT ", "");
        if (!chk.equals(vocName)) {
            dict = "DICT ";
        }
        if (NamedCommon.U2File != null && dict.equals("")) {
            if (NamedCommon.U2File.getFileName().equals(vocName)) {
                return NamedCommon.U2File;
            }
        }
        if (!SetFile(inFile, vocName)) {
            NamedCommon.ZERROR = true;
            uCommons.uSendMessage("File open error on " + inFile);
            return null;
        }

        while (true) {
            try {
                if (NamedCommon.U2File != null) {
                    if (NamedCommon.U2File.getFileName().equals(chk)) {
                        theFile = NamedCommon.U2File;
                    } else {
                        if (NamedCommon.OpenFiles.indexOf(vocName) < 0) {
                            if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + vocName);
                            theFile = NamedCommon.uSession.open(vocName);
                            NamedCommon.OpenFiles.add(vocName);
                            NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(vocName), theFile);
                        } else {
                            theFile = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(vocName));
                        }
                    }
                } else {
                    if (NamedCommon.OpenFiles.indexOf(vocName) < 0) {
                        if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + vocName);
                        theFile = NamedCommon.uSession.open(vocName);
                        NamedCommon.OpenFiles.add(vocName);
                        NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(vocName), theFile);
                    } else {
                        theFile = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(vocName));
                    }
                }
                if (NamedCommon.debugging)
                    uCommons.uSendMessage("   .) [" + theFile.getFileName() + "] is open as " + inFile);
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    NamedCommon.Zmessage = "uOpen(): " + e.getMessage();
                    NamedCommon.ZERROR = true;
                    uCommons.uSendMessage("File open error on " + inFile + " : " + e.getMessage());
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    break;
                }
            }
        }
        return theFile;
    }

    private static boolean SetFile(String infile, String qfile) {
        //  infile is the ACTUAL file name
        //  qfile is the VOC "name" for the infile
        //  e.g. infile: CLIENT (DatAct: LIVE1)
        //       qfile : CLIENT.LIVE1

        if (!NamedCommon.sConnected) {
            uCommons.uSendMessage("Connecting to SourceDB.");
            if (SourceDB.ConnectSourceDB().contains("<<FAIL>>")) {
                NamedCommon.ZERROR = true;
                return false;
            }
        }
        boolean okay = true;
        boolean dict = false;
        int tries = 0;
        while (okay) {
            if (NamedCommon.VOC == null || !NamedCommon.VOC.isOpen()) {
                u2Commons.OpenVOC();
                if (NamedCommon.ZERROR) okay = false;
            }
            break;
        }

        if (!okay) return okay;

        if (qfile.contains("DICT ")) {
            qfile = qfile.replace("DICT ", "");
            dict = true;
        }
        UniString recID = new UniString(qfile);
        UniString recCK = new UniString();
        tries = 0;
        boolean loopSw = true;
        while (loopSw) {
            while (true) {
                try {
                    recCK = NamedCommon.VOC.read(recID);
                    while (true) {
                        try {
                            NamedCommon.VOC.deleteRecord(recID);
                            break;
                        } catch (UniFileException e) {
                            if (!u2Commons.TestAlive()) {
                                SourceDB.ReconnectService();
                            } else {
                                if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                                uCommons.uSendMessage("Cannot DELETE " + String.valueOf(recID) + " from VOC file.");
                                uCommons.uSendMessage(e.getMessage());
                                break;
                            }
                        }
                    }
                    break;
                } catch (UniFileException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                        recCK = null;
                        break;
                    }
                }
            }
            while (true) {
                try {
                    tries++;
                    String usePointer = NamedCommon.datAct;
                    if (!NamedCommon.fLocn.equals("")) usePointer = NamedCommon.fLocn;
                    if (!usePointer.endsWith(NamedCommon.slash) && usePointer.contains(NamedCommon.slash))
                        usePointer += NamedCommon.slash;

                    UniDynArray record = new UniDynArray();
                    if (NamedCommon.databaseType.equals("UNIDATA") || usePointer.contains(NamedCommon.slash)) {
                        if (infile.startsWith("DICT ")) {
                            infile = infile.replace("DICT ", "");
                            dict = true;
                        }
                        record.replace(1, "F");
                        record.replace(2, usePointer + infile);
                        record.replace(3, usePointer + "D_" + infile);
                    } else {
                        if (infile.startsWith("DICT ")) {
                            infile = infile.replace("DICT ", "");
                            dict = true;
                        }
                        record.replace(1, "Q");
                        record.replace(2, usePointer);
                        record.replace(3, infile);
                    }
                    NamedCommon.VOC.setRecordID(recID);
                    NamedCommon.VOC.setRecord(record);
                    NamedCommon.VOC.write();
                    if (NamedCommon.PointerFiles.indexOf(recID.toString()) < 0)
                        NamedCommon.PointerFiles.add(recID.toString());
                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) Pointer file " + qfile + " created");
                    loopSw = false;
                    record = null;
                    break;
                } catch (UniFileException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                        uCommons.uSendMessage("FATAL: cannot write [" + String.valueOf(recID) + "] to VOC");
                        uCommons.uSendMessage(e.getMessage());
                        break;
                    }
                }
            }
        }

        if (!okay) return okay;

        while (true) {
            try {
                if (dict) {
                    if (NamedCommon.OpenFiles.indexOf(qfile) < 0) {
                        if (NamedCommon.debugging) uCommons.uSendMessage("Open  DICT " + qfile);
                        NamedCommon.U2File = NamedCommon.uSession.openDict(qfile);
                        NamedCommon.OpenFiles.add(qfile);
                        NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(qfile), NamedCommon.U2File);
                    } else {
                        NamedCommon.U2File = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(qfile));
                    }
                } else {
                    if (NamedCommon.OpenFiles.indexOf(qfile) < 0) {
                        if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + qfile);
                        NamedCommon.U2File = NamedCommon.uSession.open(qfile);
                        NamedCommon.OpenFiles.add(qfile);
                        NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(qfile), NamedCommon.U2File);
                    } else {
                        if (NamedCommon.u2Handles == null || NamedCommon.u2Handles.size() == 0) {
                            CloseAllFiles();
                            if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + qfile);
                            NamedCommon.U2File = NamedCommon.uSession.open(qfile);
                            NamedCommon.OpenFiles.add(qfile);
                            NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(qfile), NamedCommon.U2File);
                        }
                        try {
                            NamedCommon.U2File = NamedCommon.u2Handles.get(NamedCommon.OpenFiles.indexOf(qfile));
                        } catch (Exception e) {
                            NamedCommon.Zmessage = "Cannot find " + qfile + " in u2Handles.";
                            uCommons.uSendMessage(NamedCommon.Zmessage);
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage += "  " + e.getMessage();
                            okay = false;
                        }
                    }
                }
                if (NamedCommon.OpenFiles.indexOf(qfile) >= 0) {
                    uCommons.uSendMessage("   .) " + qfile + " stored in u2Handles(" + NamedCommon.OpenFiles.indexOf(qfile) + ")");
                }
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    uCommons.uSendMessage("   .) Open failed for " + qfile + " >> " + e.getMessage());
                    okay = false;
                    while (true) {
                        try {
                            if (!u2Commons.rfuelFiles.contains(recID.toString())) {
                                NamedCommon.VOC.setRecordID(recID);
                                NamedCommon.VOC.deleteRecord();
                                uCommons.uSendMessage(recID + " deleted from VOC");
                                int vocPx = NamedCommon.PointerFiles.indexOf(recID.toString());
                                if (vocPx > -1) NamedCommon.PointerFiles.remove(vocPx);
                            }
                            break;
                        } catch (UniFileException e1) {
                            if (!u2Commons.TestAlive()) {
                                SourceDB.ReconnectService();
                            } else {
                                if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                                uCommons.uSendMessage("Cannot the remove failed file from VOC - DELETE " + recID);
                                okay = false;
                            }
                            break;
                        }
                    }
                    break;
                }
            }
        }

        return okay;
    }

    private static boolean CreateFile(String thefile, UniFile uFile) {
        String cFile, status = "Created " + thefile;
        if (thefile.startsWith("DICT ")) {
            thefile = thefile.split("\\ ")[1];
        }
        if (NamedCommon.databaseType.equals("UNIVERSE")) {
            // MUST be PIopen flavour
            cFile = "CREATE.FILE " + thefile + " 30 64BIT"; // 3 4";
        } else {
            uCommons.uSendMessage("CREATE.FILE for UniData is not ready");
            cFile = "CREATE.FILE " + thefile + " 919,3 DYNAMIC KEYDATA";
            //      CREATE.FILE MULTIFILE thefile.TAKE 8443,7 DYNAMIC KEYDATA
        }
        boolean okay = uniExec(cFile);
        if (!okay) {
            status = "<<FAIL>> - " + thefile + " must be created manually";
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = status;
        } else {
            while (true) {
                try {
                    if (NamedCommon.debugging) uCommons.uSendMessage("Open  " + thefile);
                    uFile = NamedCommon.uSession.open(thefile);
                    NamedCommon.OpenFiles.add(thefile);
                    NamedCommon.u2Handles.add(NamedCommon.OpenFiles.indexOf(thefile), uFile);
                    status = "";
                    break;
                } catch (UniSessionException e) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                        status = "<<FAIL>> " + thefile + " must be created by hand :: " + cFile;
                        uCommons.uSendMessage(e.getMessage());
                        okay = false;
                        break;
                    }
                }
            }
        }
        uCommons.uSendMessage(status);
        return okay;
    }

    public static boolean ClearFile(UniFile FileIO) {
        boolean okay = true;
//        uCommons.uSendMessage("Probably a PartFile - clearing manually. Please wait.");
        uCommons.uSendMessage("Selecting records...");
        uSelectFile(FileIO);
        UniDataSet uDset = new UniDataSet();
        uCommons.uSendMessage("Deleting records...");
        int delblock = 0, deltot = 0, nbritems = 0, lshow = 11;
        uReadNextID();
        while (!NamedCommon.uID.equals("")) {
            String chk = String.valueOf(NamedCommon.uID);
            uDset.append(NamedCommon.uID);
            delblock++;
            deltot++;
            if (delblock > NamedCommon.showAT) {
                while (true) {
                    try {
                        FileIO.deleteRecord(uDset);
                        uDset = new UniDataSet();
                        delblock = 0;
                        uCommons.eMessage = "Deleted "
                                + uCommons.RightHash(oconvM(String.valueOf(deltot), "MD0,"), lshow)
                                + " records ... ";
                        uCommons.uSendMessage(uCommons.eMessage);
                        break;
                    } catch (UniFileException e2) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            if (e2.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                            uCommons.uSendMessage("<<FAIL>> " + e2.getMessage());
                            okay = false;
                            break;
                        }
                    }
                }
            }
            uReadNextID();
        }
        if (delblock > 0) {
            while (true) {
                try {
                    FileIO.deleteRecord(uDset);
                    break;
                } catch (UniFileException e2) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        if (e2.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                        uCommons.uSendMessage("<<FAIL>> " + e2.getMessage());
                        okay = false;
                        break;
                    }
                }
            }
            uDset = new UniDataSet();
            delblock = 0;
        }
        uCommons.eMessage = "Deleted "
                + uCommons.RightHash(oconvM(String.valueOf(deltot), "MD0,"), lshow)
                + " records ... ";
        uCommons.uSendMessage(uCommons.eMessage);
        return okay;
    }

    public static void uSelectFile(UniFile FileIO) {
        while (true) {
            try {
                NamedCommon.uSelect = NamedCommon.uSession.selectList(0);
                while (true) {
                    try {
                        NamedCommon.uSelect.clearList();
                        break;
                    } catch (UniSelectListException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            uCommons.eMessage = "Clear List Error :: " + e.getMessage();
                            break;
                        }
                    }
                }
                while (true) {
                    try {
                        NamedCommon.uSelect.select(FileIO);
                        break;
                    } catch (UniSelectListException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            uCommons.eStatus = 1;
                            uCommons.uSendMessage(e.getMessage());
                            uCommons.eMessage = "Select failed ";
                            break;
                        }
                    }
                }
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    uCommons.eStatus = 1;
                    uCommons.eMessage = "Assign SelList:: failed ";
                }
            }
        }
        if (!(uCommons.eMessage.isEmpty())) {
            uCommons.uSendMessage(uCommons.eMessage);
        }
    }

    public static UniString uReadNextID() {
        NamedCommon.uID = null;
        uCommons.eStatus = 0;
        while (true) {
            try {
                NamedCommon.uID = NamedCommon.uSelect.next();
                break;
            } catch (UniSelectListException e) {
                uCommons.eStatus = 1;
                uCommons.eMessage = e.getMessage();
            } catch (NullPointerException npe) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    uCommons.eStatus = 1;
                    uCommons.eMessage = npe.getMessage();
                    break;
                }
            }
        }
        return NamedCommon.uID;
    }

    public static void uWriter(UniFile inFile, String inID, String inRec) {
        if (inFile == null) {
            NamedCommon.Zmessage = "The file you are trying to write to is not open (null)";
            uCommons.uSendMessage(NamedCommon.Zmessage);
            NamedCommon.ZERROR = true;
            return;
        }
        String filename = inFile.getFileName();
        if (filename == null || filename.equals("")) {
            NamedCommon.Zmessage = "The file you are trying to write to has not been opened.";
            uCommons.uSendMessage(NamedCommon.Zmessage);
            NamedCommon.ZERROR = true;
            return;
        }
        int tries = 0;
        boolean okay = false;
        while (tries < 3) {
            tries++;
            UniDynArray rec = new UniDynArray();
            okay = true;
            if (inRec.contains("\n")) {
                String[] lines = inRec.split("\\r?\\n");
                for (int ln = 0; ln < lines.length; ln++) {
                    rec.replace(ln + 1, lines[ln]);
                }
            } else {
                rec = new UniDynArray(inRec);
            }
            while (true) {
                try {
                    inFile.setRecord(rec);
                    inFile.setRecordID(inID);
                    inFile.write();
                    tries = 4;
                    break;
                } catch (UniFileException ufe) {
                    if (!u2Commons.TestAlive()) {
                        SourceDB.ReconnectService();
                    } else {
                        uCommons.uSendMessage("U2 File Error # (" + tries + ")  " + inFile.getFileName());
                        uCommons.uSendMessage("Database message: " + ufe.getErrorTypeText() + " " + ufe.getMessage());
                        NamedCommon.ZERROR = true;
                        break;
                    }
                }
            }
            rec = null;
        }
        if (!okay) {
            NamedCommon.Zmessage = "uniWriter() ERROR on file "
                    + inFile.getFileName() + " for @ID ["
                    + inFile.getRecordID() + "]  :: ";
            NamedCommon.ZERROR = true;
        } else {
            NamedCommon.ConnectionError = false;
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage = "";
        }
    }

    public static UniFile uClose(UniFile inFile) {
        if (inFile == null) return null;
        if (!inFile.isOpen()) return null;
        String fname = inFile.getFileName();
        if (inFile == NamedCommon.U2File) {
            if (NamedCommon.U2File.getFileName().equals(fname)) {
                try {
                    NamedCommon.U2File.close();
                    NamedCommon.U2File = null;
                    inFile = null;
                } catch (UniFileException e) {
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    if (!NamedCommon.runSilent) uCommons.uSendMessage("WARN: uClose() " + e.getMessage());
                }
                inFile = null;
            }
        }

        if (inFile != null) {
            try {
                inFile.close();
            } catch (UniFileException e) {
                if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                if (NamedCommon.debugging) {
                    uCommons.eMessage = "uClose() ERROR on "
                            + fname + " :: " + e.getMessage();
                    uCommons.uSendMessage(uCommons.eMessage);
                }
            }
            inFile = null;
        }
        int posx = NamedCommon.OpenFiles.indexOf(fname);
        if (posx > -1) {
            try {
                NamedCommon.u2Handles.get(posx).close();
            } catch (UniFileException e) {
                if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                // already closed (above)   //
            }
            NamedCommon.u2Handles.set(posx, null);
            NamedCommon.u2Handles.remove(posx);
            NamedCommon.OpenFiles.remove(posx);
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("Close " + fname);
        return inFile;
    }

    public static boolean SetupU2Controls(boolean shutdown) {
        boolean success = true;
        String setting = "stop";
        if (!shutdown) setting = "";
        stopProc = shutdown;

        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        if (NamedCommon.ZERROR) return false;

        switch (NamedCommon.protocol) {
            case "real":
                ClearVoc("");
                rCommons.Startup(shutdown);
                break;
            case "u2cs":
                uCommons.uSendMessage("Removing old Q-pointers");
                ClearVoc("");
                uCommons.uSendMessage("Resetting STOP switch in BP.UPL");
                if (uplBP != null) {
                    if (!uplBP.getFileName().equals("BP.UPL")) {
                        uplBP = uClose(uplBP);
                        if (!LoadUplBp.OpenCreate("BP.UPL").contains("<<FAIL>>")) {
                            uplBP = LoadUplBp.uplHANDLE;
                            NamedCommon.ZERROR = false;
                        } else {
                            NamedCommon.ZERROR = true;
                        }
                    }
                } else {
                    String ans = LoadUplBp.OpenCreate("BP.UPL");
                    if (!ans.contains("<<FAIL>>")) {
                        uplBP = LoadUplBp.uplHANDLE;
                        NamedCommon.ZERROR = false;
                        NamedCommon.Zmessage = "";
                    } else {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = ans;
                        uCommons.uSendMessage(NamedCommon.Zmessage);
                        return false;
                    }
                }
                uWriter(uplBP, "STOP", setting);
                if (!shutdown) uCommons.uSendMessage("STOP switch has been turned OFF");
                if (shutdown) uCommons.uSendMessage("STOP switch has been turned ON");
                uCommons.Sleep(0);
                // ------------------------------------------------------------------------ //
                // Only Clear Phantoms, lists and logs when starting up - need to read them //
                // ------------------------------------------------------------------------ //
                if (!shutdown) {
                    uCommons.uSendMessage("Clearing Phantoms, SavedLists and Logs");
                    uCommons.Sleep(2);
                    String phFile, slFile;
                    if (NamedCommon.databaseType.equals("UNIVERSE")) {
                        phFile = "&PH&";
                        slFile = "&SAVEDLISTS&";
                    } else {
                        phFile = "_PH_";
                        slFile = "SAVEDLISTS";
                    }
                    String clf;
                    clf = "CLEAR.FILE " + phFile;
                    uniExec(clf);
                    uCommons.uSendMessage("   > " + uniExecResp);
                    clf = "CLEAR.FILE " + slFile;
                    uniExec(clf);
                    uCommons.uSendMessage("   > " + uniExecResp);
                }

                success = true;
                if (NamedCommon.ZERROR) {
                    uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
                    NamedCommon.ZERROR = false;
                }
                if (!shutdown) {
                    UniString usChk = uRead(uplBP, "STARTUP");
                    if (usChk.toString().equals("")) {
                        uCommons.uSendMessage("   .) Adding uHARNESS to STARTUP");
                        uWriter(uplBP, "STARTUP", "PHANTOM uHARNESS");
                        usChk = uRead(uplBP, "STARTUP");
                    }
                    if (usChk.dcount() > 0) {
                        uCommons.uSendMessage("AUTO-Restart of UDE Objects");
                        UniDynArray daChk = new UniDynArray(usChk);
                        String cmd;
                        int nbrPgms = daChk.dcount();
                        for (int pgm = 1; pgm <= nbrPgms; pgm++) {
                            cmd = String.valueOf(daChk.extract(pgm)).replaceAll("\\*", "");
                            if (!cmd.equals("")) {
                                uCommons.uSendMessage("Starting " + cmd);
                                uniExec(cmd);
                            }
                        }
                        daChk = null;
                    }
                }
                if (NamedCommon.ZERROR) return false;
                uWriter(uplBP, "RQM", String.valueOf(NamedCommon.RQM));
                if (NamedCommon.ZERROR) return false;
                if (!shutdown) uCommons.uSendMessage("RQM has been reset");
                if (NamedCommon.ZERROR) {
                    uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
                    NamedCommon.ZERROR = false;
                }
                break;
            case "u2mnt":
                String cmd, file, atr, mv, sv, rtnString;
                String resetExt = uMountCommons.extOut;
                uMountCommons.extOut = ".admin";

                uCommons.uSendMessage("Clearing uLOG");
                cmd = "{CLF}";
                file = "{file=uLOG}{item=}{data=}";
                atr = "{atr=}";
                mv = "{mv=}";
                sv = "{sv=}";
                rtnString = cc_uMount(cmd + file + atr + mv + sv);

                uCommons.uSendMessage("Resetting STOP switch in BP.UPL");
                cmd = "{WRI}";
                file = "{file=BP.UPL}{item=STOP}{data=" + setting + "}";
                atr = "{atr=}";
                mv = "{mv=}";
                sv = "{sv=}";
                rtnString = cc_uMount(cmd + file + atr + mv + sv);

                if (!shutdown) {
                    uCommons.uSendMessage("STOP switch has been turned OFF");
                } else {
                    uCommons.uSendMessage("STOP switch has been turned ON");
                }

                if (!shutdown) {
                    cmd = "{RDI}";
                    file = "{file=BP.UPL}{item=STARTUP}{data=}";
                    atr = "{atr=}";
                    mv = "{mv=}";
                    sv = "{sv=}";
                    rtnString = cc_uMount(cmd + file + atr + mv + sv);
                    if (rtnString.equals("")) {
                        uCommons.uSendMessage("   .) Adding uHARNESS to STARTUP");
                        cmd = "{WRI}";
                        file = "{file=BP.UPL}{item=STARTUP}{data=PHANTOM uHARNESS}";
                        atr = "{atr=}";
                        mv = "{mv=}";
                        sv = "{sv=}";
                        rtnString = cc_uMount(cmd + file + atr + mv + sv);
                    } else {
                        uCommons.uSendMessage("AUTO-Restart of UDE Objects");
                        while (rtnString.contains("<fm>")) {
                            rtnString = rtnString.replace("<fm>", "\t");
                        }
                        String[] pgms = rtnString.split("\\t");
                        String uCmd = "";
                        int nbrPgms = pgms.length;
                        for (int pgm = 0; pgm < nbrPgms; pgm++) {
                            uCmd = pgms[pgm].replaceAll("\\*", "");
                            if (!uCmd.equals("")) {
                                uCommons.uSendMessage("   .) Starting " + uCmd);
                                cmd = "{EXE}{exec=" + uCmd + "}";
                                file = "{file=}";
                                atr = "{atr=}";
                                mv = "{mv=}";
                                sv = "{sv=}";
                                rtnString = cc_uMount(cmd + file + atr + mv + sv);
                            }
                        }
                    }
                }
                if (NamedCommon.ZERROR) return false;
                cmd = "{WRI}";
                file = "{file=BP.UPL}{item=RQM}{data=" + NamedCommon.RQM + "}";
                atr = "{atr=}";
                mv = "{mv=}";
                sv = "{sv=}";
                rtnString = cc_uMount(cmd + file + atr + mv + sv);
                if (NamedCommon.ZERROR) return false;
                if (!shutdown) uCommons.uSendMessage("RQM has been reset");

                uMountCommons.extOut = resetExt;
                break;
            case "u2sockets":
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
            default:
                break;
        }

        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            NamedCommon.ZERROR = false;
        }

        if (!shutdown) {
            // clear all saved lists
            switch (NamedCommon.protocol) {
                case "u2cs":
                    String lstFile = "&SAVEDLISTS&";
                    if (NamedCommon.databaseType.equals("UNIDATA")) lstFile = "SAVEDLISTS";
                    String cmd = "CLEAR.FILE " + lstFile;
                    uniExec(cmd);
                    uCommons.uSendMessage(lstFile + " has been reset");
                    break;
                case "real":
                    break;
                case "u2mnt":
                    break;
                case "rmi.u2cs":
                    System.out.println("rmi:  is being developed - not ready yet");
            }
        }

        uCommons.uSendMessage("Remove workfiles");

        if (startFrom.equals("standard")) {
            success = SqlCommands.RemoveWorkfiles();
//            if (!NamedCommon.tConnected) {
//                SqlCommands.ConnectSQL();
//                if (!NamedCommon.tConnected) uCommons.uSendMessage("NOT Connected !!");
//                if (NamedCommon.uCon == null) {
//                    String url = NamedCommon.jdbcCon.split("\\;")[0];
//                    String jdbcDBI = APImsg.APIget("jdbcDbi:base-sql");
//                    if (!jdbcDBI.equals("")) {
//                        if (!url.endsWith(";")) url += ";";
//                        url += "databaseName=" + jdbcDBI;
//                        if (!url.endsWith(";")) url += ";";     // in case Dbi is just a name - common
//                    }
//                    if (!NamedCommon.jdbcAdd.equals("")) {
//                        if (!url.endsWith(NamedCommon.jdbcSep)) url += NamedCommon.jdbcSep;
//                        url += NamedCommon.jdbcAdd;
//                        if (!url.endsWith(NamedCommon.jdbcSep)) url += NamedCommon.jdbcSep;
//                    }
//                    NamedCommon.uCon = ConnectionPool.getConnection(url);
//                    if (NamedCommon.uCon == null) {
//                        uCommons.uSendMessage("* --------------------------------- ");
//                        uCommons.uSendMessage("CANNOT find the jdbc connection for "+url);
//                        uCommons.uSendMessage("* --------------------------------- ");
//                        uCommons.uSendMessage("Looked in: ");
//                        for (int c=0 ; c < ConnectionPool.ConString.size(); c++) {
//                            uCommons.uSendMessage("   >. " + ConnectionPool.ConString.get(c));
//                        }
//                        uCommons.uSendMessage("* --------------------------------- ");
//                        success = false;
//                    }
//                }
//                if (NamedCommon.uCon != null && !NamedCommon.rawDB.equals("")) {
//                    try {
//                        Statement stmt = NamedCommon.uCon.createStatement();
//                        String db = NamedCommon.rawDB.trim();
//                        if (!db.equals("") && !db.startsWith("$")) {
//                            String tmp = SqlCommands.SelectWorkfiles();
//                            String dcmd = "";
//                            ResultSet rs = stmt.executeQuery(tmp);
//
//                            ArrayList<String> DDL = new ArrayList<String>();
//                            DDL.add(" ");
//                            DDL.clear();
//                            String store = NamedCommon.RunType;
//                            NamedCommon.RunType = "REFRESH";
//                            NamedCommon.isNRT = false;
//                            NamedCommon.isPrt = false;
//                            while (rs.next()) {
//                                tmp = rs.getString("TBL");
//                                if (tmp.toLowerCase().contains("workfile")) {
//                                    dcmd = SqlCommands.DropTable(NamedCommon.rawDB, "upl", tmp);
//                                    DDL.add(dcmd);
//                                }
//                            }
//                            rs.close();
//                            rs = null;
//                            NamedCommon.RunType = store;
//                            tmp = "";
//                            if (!DDL.isEmpty()) {
//                                SqlCommands.ExecuteSQL(DDL);
//                                if (NamedCommon.ZERROR) return false;
//                            }
//                        } else {
//                            uCommons.uSendMessage("Unkown TargetDB \"raw\" connector - cannot cleanup workfiles");
//                        }
//                        stmt.close();
//                        stmt = null;
//                    } catch (SQLException e) {
//                        uCommons.uSendMessage("Cannot create Statement() " + e.getMessage());
//                        success = false;
//                    }
//                }
//            }
        } else {
            uCommons.uSendMessage("    Not applicable when Loading Programs");
            uCommons.uSendMessage("    ****");
        }

        uCommons.uSendMessage("-----------------------------------------------------------------");

        if (shutdown) {
            if (NamedCommon.sConnected) SourceDB.DisconnectSourceDB();
            if (NamedCommon.tConnected) SqlCommands.DisconnectSQL();
        }

        return success;

    }

    public static String rSub (String subr, String[] args) {
        String callStr = "";
        for (int a=0; a < args.length; a++) { callStr += "<tm>" + args[a]; }
        if (callStr.endsWith("<tm>")) {
            callStr = callStr.substring(0, callStr.length()-4);
        }
        String srtn = "{MSVC}";
        String sname= "{call=" + subr + "}";
        String cmd  = "{args=" + callStr + "}";
        String cStr = srtn + sname + cmd;
        String rtnString = MetaBasic(cStr);
        return rtnString;
    }

    public static String rRead (String infile, String itemID, String a, String m, String s) {
        String srtn = "{RDI}";
        String cmd  = "{cmd=}";
        String file = "{file=" + infile + "}";
        String item = "{item=" + itemID + "}";
        String atr  = "{atr=" + a + "}";
        String mv   = "{mv=" + m + "}";
        String sv   = "{sv=" + s +"}";
        String cStr = srtn + cmd + file + item + atr + mv + sv;
        String rtnString = MetaBasic(cStr);
        return rtnString;
    }

    public static boolean real_SelectAndRead(int AA, int BB, String joinFile, String thisCmd) {
        boolean uniFailed = false;
        String selFile="";
        if (thisCmd.toLowerCase().contains("select")) {
            selFile = uCommons.FieldOf(thisCmd," ", 2);
        } else {
            selFile = joinFile; //NamedCommon.u2Source;
        }

        String srtn = "{SAR}";
        String cmd  = "{cmd=" + thisCmd + "}";
        String file = "{file=" + selFile + "}";
        String item = "{item=}";
        String atr = "{atr=}";
        String mv = "{mv=}";
        String sv = "{sv=}";
        String cStr = srtn + cmd + file + item + atr + mv + sv;
        String rtnString = MetaBasic(cStr);
        if (NamedCommon.ZERROR) {
            uniFailed = true;
        } else {
            String junk;
            UniString rID;
            UniString xRow;
            String[] result = rtnString.split("<im>");
            int eor = result.length;
            for (int r = 0; r < eor; r++) {
                junk = result[r];
                rID = new UniString(uCommons.FieldOf(junk, "<km>", 1));
                xRow = new UniString(uCommons.FieldOf(junk, "<km>", 2));
                BB++;
                NamedCommon.fmvArray.replace(AA, BB, (uCommons.UV2SQLRec(rID, xRow)));
                rID = null;
                xRow = null;
            }
        }
        return uniFailed;
    }

    public static boolean generic_SelectAndRead(int AA, int BB, String joinFile, String thisCmd) {
        boolean uniFailed = false;
        String srtn = "{SAR}";
        String cmd  = "{cmd=" + thisCmd + "}";
        String file = "{file=" + joinFile + "}";
        String item = "{item=}";
        String atr = "{atr=}";
        String mv = "{mv=}";
        String sv = "{sv=}";
        String cStr = srtn + cmd + file + item + atr + mv + sv;
        String rtnString = MetaBasic(cStr);
        cStr =""; cmd =""; file =""; item =""; atr =""; mv =""; sv ="";
        if (NamedCommon.ZERROR) {
            uniFailed = true;
        } else {
            String junk;
            String rID, sRow;
            String[] result = rtnString.split("<im>");
            int eor = result.length;
            for (int r = 0; r < eor; r++) {
                junk = result[r];
                rID = uCommons.FieldOf(junk, "<km>", 1);
                sRow = uCommons.FieldOf(junk, "<km>", 2);
                BB++;
                NamedCommon.fmvArray.replace(AA, BB, rID + "<im>" + sRow);
            }
            rID =""; sRow =""; junk =""; rtnString =""; thisCmd =""; joinFile =""; srtn ="";
        }
        return uniFailed;
    }

    public static boolean u2cs_SelectAndRead(int AA, int BB, String joinFile, String thisCmd) {
        boolean uniFailed = false;
        UniString rID;
        UniString xRow;
        int AtSelected = 0;
        if (!NamedCommon.U2File.getFileName().equals(joinFile)) {
            boolean fOpen = (uOpen(joinFile) != null);
            if (!fOpen) return false;
        }

        while (true) {
            try {
                uniCmd = null;
                uniCmd = NamedCommon.uSession.command();
                uniCmd.setCommand(thisCmd);
                while (true) {
                    try {
                        uniCmd.exec();
                        AtSelected = uniCmd.getAtSelected();
                        uCommons.uSendMessage("      >> " + AtSelected + " records selected");
                        UniSelectList selList = NamedCommon.uSession.selectList(0);
                        boolean eof = false;
                        int sCtr = 0;
                        String sRow;
                        while (!eof) {
                            while (true) {
                                try {
                                    rID = selList.next();
                                    if (rID.toString().equals("")) {
                                        eof = true;
                                        continue;
                                    }
                                    while (true) {
                                        try {
                                            xRow = NamedCommon.U2File.read(rID);
                                            if (NamedCommon.debugging)
                                                uCommons.uSendMessage("         StringifyRecord(" + rID + ")");
                                            sRow = UV2SQLRec(rID, xRow);
                                            sCtr++;
                                            BB++;
                                            while (!NamedCommon.fmvArray.extract(AA, BB).equals("")) {
                                                BB++;
                                            }
                                            NamedCommon.fmvArray.replace(AA, BB, sRow);
                                            break;
                                        } catch (UniFileException e) {
                                            if (!u2Commons.TestAlive()) {
                                                SourceDB.ReconnectService();
                                            } else {
                                                uCommons.uSendMessage("      -- Error reading " + rID.toString() + " - skipping");
                                                break;
                                            }
                                        }
                                    }
                                    break;
                                } catch (UniSelectListException e) {
                                    if (!u2Commons.TestAlive()) {
                                        SourceDB.ReconnectService();
                                    } else {
                                        eof = true;
                                        break;
                                    }
                                }
                            }
                            eof = selList.isLastRecordRead();
                        }
                        uCommons.uSendMessage("      >> " + sCtr + " records loaded");
                        break;
                    } catch (UniCommandException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            uniFailed = true;
                            break;
                        }
                    }
                }
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    uniFailed = true;
                    break;
                }
            }
        }
        return uniFailed;
    }

    private static void ShowfmvArray(UniDynArray fmvArray) {
        uCommons.uSendMessage("-------------------------------------------------------");
        int a, m, s, lx;
        String datum = "";
        int eoa = fmvArray.dcount();
        int eom, eos;
        for (a = 1; a <= eoa; a++) {
            datum = String.valueOf(fmvArray.extract(a));
            if (datum.equals("")) continue;
            uCommons.uSendMessage("Tag: " + NamedCommon.fTagNames.get(a));
            eom = fmvArray.dcount(a);
            for (m = 1; m <= eom; m++) {
                datum = String.valueOf(fmvArray.extract(a, m));
                if (datum.equals("")) continue;
                eos = fmvArray.dcount(a,m);
                for (s = 1; s <= eos; s++) {
                    datum = String.valueOf(fmvArray.extract(a, m, s));
                    if (datum.equals("")) continue;
                    lx = datum.length();
                    if (lx > 50) lx = 50;
                    datum = datum.substring(0, lx);
                    uCommons.uSendMessage("<" + a + "," + m + "," + s + "> " + datum);
                }
            }
        }
        uCommons.uSendMessage("-------------------------------------------------------");
    }

    public static String[] uniCallSub(String srtn, String[] args) {
        String[] ans = new String[args.length];
        while (true) {
            try {
                UniSubroutine uSub = NamedCommon.uSession.subroutine(srtn, args.length);
                for (int a = 0; a < args.length; a++) {
                    uSub.setArg(a, args[a]);
                }
                if (NamedCommon.debugging) {
                    String callSub = GetUniSubDets(uSub);
                    uCommons.uSendMessage(callSub);
                }
                uSub.call();
                for (int a = 0; a < args.length; a++) {
                    ans[a] = uSub.getArg(a);
                }
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    NamedCommon.Zmessage = "uniCallSub(): " + e.getMessage();
                    NamedCommon.ZERROR = true;
                    ans[0] = NamedCommon.Zmessage;
                    break;
                }
            } catch (UniSubroutineException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    NamedCommon.Zmessage = "uniCallSub(): " + e.getMessage();
                    NamedCommon.ZERROR = true;
                    ans[0] = NamedCommon.Zmessage;
                    break;
                }
            }
        }
        return ans;
    }

    private static void u2cs_SARwithPaging(int AA, int BB, String joinFile, String thisCmd) {
        String[] arglist = new String[20];
        for (int x = 0; x < 20; x++) { arglist[x] = ""; }
        String subr="", end_of_list="[<END>]";
        String CorrelID = APImsg.APIget("correlationid");
        if (CorrelID.equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "No correlation ID in the message. This is required for query paging.";
            uCommons.uSendMessage(NamedCommon.Zmessage);
            return;
        }

        uCommons.uSendMessage("   .) "+NamedCommon.block);
        boolean initiate = APImsg.APIget("initiate").toLowerCase().equals("true");
        if (initiate) {
            subr = "2!uFETCH_PREP";
            arglist[0] = thisCmd;
            arglist[1] = "uREST";
            arglist[2] = NamedCommon.U2File.getFileName();
            arglist[3] = CorrelID;
            arglist[4] = APImsg.APIget("page-size");
            arglist[5] = NamedCommon.datAct;
            arglist[6] = end_of_list;
            u2cs_CallFetchPrep(subr, arglist);
        } else {
            uCommons.uSendMessage("   .) Getting page # "+APImsg.APIget("page"));
        }
        uCommons.uSendMessage("   .) "+NamedCommon.block);

        int aFrom=1, aTo=1000;
        try {
            aFrom = Integer.valueOf(APImsg.APIget("page"));
        } catch (NumberFormatException nfe) {
            aFrom = 1;
            uCommons.uSendMessage("   .) Number Format Error on page # using '1' instead.");
        }
        try {
            aTo = Integer.valueOf(APImsg.APIget("page-size"));
        } catch (NumberFormatException nfe) {
            aTo = 1000;
            uCommons.uSendMessage("   .) Number Format Error on page-size using '1000' instead.");
        }
        if (aFrom != 1) aFrom = aFrom * aTo;
        subr = "2!uFETCH";
        for (int x = 0; x < 20; x++) { arglist[x] = ""; }
        arglist[0] = "";
        arglist[1] = NamedCommon.U2File.getFileName();
        arglist[2] = String.valueOf(aFrom);
        arglist[3] = String.valueOf(aFrom + aTo);
        arglist[4] = String.valueOf(NamedCommon.RQM);
        arglist[5] = CorrelID;
        String rID, xRow;
        String reply = u2cs_CallFetchData(NamedCommon.u2Source, arglist);
        String[] inLines = reply.split("\\r?\\n");
        int eor, eop = inLines.length;
        for (int r=0 ; r < eop ; r++) {
            reply = inLines[r];
            String[] rows = reply.split(NamedCommon.FMark);
            eor = rows.length;
            for (int rr=0; rr < eor ; rr++) {
                reply = rows[rr];
                if (!reply.equals(end_of_list)) {
                    BB++;
                    NamedCommon.fmvArray.replace(AA, BB, reply);
                }
            }
        }
    }

    public static boolean u2mnt_SelectAndRead(int AA, int BB, String joinFile, String thisCmd) {
        String cmd = "{cmd=" + thisCmd + "}";
        String file = "{file=" + joinFile + "}";
        String atr = "{atr=}";
        String mv = "{mv=}";
        String sv = "{sv=}";
        String rtnString = cc_uMount("{SAR}" + cmd + file + atr + mv + sv);
        return SelectAndRead(rtnString, AA, BB);
    }

    public static boolean u2sock_SelectAndRead(int AA, int BB, String joinFile, String thisCmd) {
        String mgr = "{socket\n}";
        String cmd = "{cmd=" + thisCmd + "}";
        String file = "{file=" + joinFile + "}";
        String atr = "{atr=}";
        String mv = "{mv=}";
        String sv = "{sv=}";
        String rtnString = cc_uSocket(mgr + "{SAR}" + cmd + file + atr + mv + sv);
        return SelectAndRead(rtnString, AA, BB);
    }

    private static boolean SelectAndRead(String rtnString, int AA, int BB) {
        int origBB = BB;
        String id, uvRec, line;
        if (rtnString.length() > 0) {
//            ArrayList<String> uvRows = new ArrayList<>(uStrings.gSplit2List(rtnString, "<im>"));
            ArrayList<String> uvRows = new ArrayList<>(Arrays.asList(rtnString.split("<im>")));
            int sCtr = 0, nbrRows = uvRows.size();
            for (int rr = 0; rr < nbrRows; rr++) {
                line = uvRows.get(rr);
                String[] parts = new String[]{};
                parts = uStrings.gSplit2Array(line, "<km>");
                if (parts.length > 1) {
                    id = parts[0];
                    uvRec = parts[1];
                    sCtr++;
                    BB++;
                    NamedCommon.fmvArray.replace(AA, BB, uvRec); //uCommons.UV2SQLRec(id, xRow));
                }
            }
        }
        // RETURN true or false.
        // true = there has been an error
        // false= no error, all is good.
        return (BB == origBB);
    }

    public static String GetExtractFile(String takeFile) {
        String stripIT = "_" + NamedCommon.MessageID;
        if (!takeFile.contains(stripIT)) return takeFile;
        int pos1 = takeFile.indexOf(stripIT);
        int len1 = stripIT.length();
        String fpart1 = takeFile.substring(0, pos1);
        String fpart2 = takeFile.substring(pos1 + len1, takeFile.length());
        takeFile = fpart1 + fpart2;
        return takeFile;
    }

    public static void ClearVoc(String pid) {
        switch (NamedCommon.protocol) {
            case "u2cs":
                u2cs_CleanVoc(pid);
                break;
            case "real":
                rCommons.CleanVoc(pid);
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
        }
    }

    private static void u2cs_CleanVoc(String pid) {
        if (NamedCommon.debugging) uCommons.uSendMessage("Clean-up VOC ----------------------------------------");
        if (NamedCommon.VOC == null) {
            OpenVOC();
            if (NamedCommon.ZERROR) return;
        }
        if (!NamedCommon.VOC.isOpen()) {
            OpenVOC();
            if (NamedCommon.ZERROR) return;
        }
        String pfx = "      ). ";
        String cmd = "";
        String savePID = NamedCommon.pid;
        if (stopProc) NamedCommon.pid = "";

        // catch missed items # 2
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) uRest: Q-Files by \"upl\" and PID: " + NamedCommon.pid);
        cmd = "SELECT VOC LIKE upl...";
        if (!pid.equals("")) cmd += NamedCommon.pid;
        cmd += " AND WITH F1 = \"Q\"";
        if (NamedCommon.debugging) uCommons.uSendMessage(pfx + "Execute: " + cmd);
        boolean success = uniExec(cmd);
        if (success) RemoveVocItems();

        cmd = "SELECT VOC LIKE PA-...";
        cmd += " AND WITH F1 = \"PA\"";
        if (NamedCommon.debugging) uCommons.uSendMessage(pfx + "Execute: " + cmd);
        success = uniExec(cmd);
        if (success) RemoveVocItems();

        // catch missed items # 3
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) uBulk: Q-Files by Account and PID:");
        String conf = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash;
        String AcctList = uCommons.ReadDiskRecord(conf + "DACCT");
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage="";
        String[] aList = AcctList.split("\n");
        for (int i = 0; i < aList.length; i++) {
            if (aList[i].isEmpty()) continue;
            cmd = "SELECT VOC LIKE ..._" + aList[i] + "_";
            if (pid.equals("")) {
                cmd += "...";
            } else {
                cmd += NamedCommon.pid;
            }
            cmd += " AND WITH F1 = \"Q\"";

            if (NamedCommon.debugging) uCommons.uSendMessage(pfx + "Execute: " + cmd);
            success = uniExec(cmd);
            if (success) RemoveVocItems();
        }

        if (stopProc) NamedCommon.pid = savePID;
    }

    private static void RemoveVocItems() {
        while (true) {
            try {
                UniSelectList uSelect = NamedCommon.uSession.selectList(0);
                while (true) {
                    try {
                        NamedCommon.uID = uSelect.next();
                        while (!NamedCommon.uID.equals(null) && !NamedCommon.uID.equals("") && !NamedCommon.ZERROR) {
                            DeleteVOCitem(NamedCommon.uID);
                            NamedCommon.uID = uSelect.next();
                        }
                        break;
                    } catch (UniSelectListException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            uCommons.uSendMessage(e.getMessage());
                            NamedCommon.ZERROR = true;
                            break;
                        }
                    }
                }
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    uCommons.uSendMessage(e.getMessage());
                    NamedCommon.ZERROR = true;
                    break;
                }
            }
        }
    }

    private static void DeleteVOCitem(UniString uID) {
        while (true) {
            try {
                NamedCommon.VOC.deleteRecord(NamedCommon.uID);
                if (NamedCommon.debugging) uCommons.uSendMessage("      ). Clean-up: " + NamedCommon.uID + " removed from VOC");
                break;
            } catch (UniFileException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    uCommons.uSendMessage(e.getMessage());
                    NamedCommon.ZERROR = true;
                    break;
                }
            }
        }
    }

    public static boolean uniExec(String cmd) {
        if (!NamedCommon.sConnected) return false;
        // Why was this line here? Is it for Kiwibank or RAB ??
//        if (NamedCommon.task.equals("099")) uCommons.uSendMessage(cmd);
        boolean okay = true;
        uniExecResp = "";
        while (true) {
            try {
                uniCmd = null;
                uniCmd = NamedCommon.uSession.command();
                uniCmd.setCommand(cmd);
                try {
                    uniCmd.exec();
                } catch (UniCommandException e) {
                    okay = false;
                    uCommons.uSendMessage(NamedCommon.databaseType + " error: " + e.getMessage());
                    break;
                }
                if (uniCmd.response() != null) {
                    uniExecResp = uniCmd.response().replaceAll("\\r?\\n", " ");
                }
                uniCmd = null;
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    okay = false;
                    uCommons.uSendMessage("uniExec: uniSession Error: " + e.getMessage());
                    break;
                }
            }
        }
        return okay;
    }

    public static boolean ExecuteSourceCmd(String executeThis) {
        if (NamedCommon.task.equals("014")) return true;
        boolean disconnect = false;
        if (!NamedCommon.sConnected) {
            SourceDB.ConnectSourceDB();
            disconnect = true;
        }
        String ans = "";
        switch (NamedCommon.protocol) {
            case "u2cs":
                while (true) {
                    try {
                        uniCmd = null;
                        uniCmd = NamedCommon.uSession.command();
                        uniCmd.setCommand(executeThis);
                        uniCmd.exec();
                        // do we need the response? Don't think so.
//                        resp = uniCmd.response();
                        break;
                    } catch (UniSessionException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Cannot set uniCmd: " + e.getMessage();
                            uCommons.uSendMessage(NamedCommon.Zmessage);
                            return false;
                        }
                    } catch (UniCommandException e) {
                        if (!u2Commons.TestAlive()) {
                            SourceDB.ReconnectService();
                        } else {
                            uCommons.uSendMessage("Issue with this command: [" + executeThis + "]");
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "Cannot execute uniCmd: " + e.getMessage();
                            uCommons.uSendMessage(NamedCommon.Zmessage);
                            return false;
                        }
                    }
                }
                break;
            case "real":
                String cmd = "{EXE}{{exec=" + executeThis + "}";
                ans = u2Commons.MetaBasic(cmd);
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
        }

        if (disconnect) SourceDB.DisconnectSourceDB();

        return !NamedCommon.ZERROR;
    }

    public static void CheckSource() {
        if (NamedCommon.debugging) uCommons.uSendMessage("---------------------------------------------------------------");
        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) Read from VOC");
        pingString = ReadAnItem("VOC", "BP.UPL","","","");
        if (NamedCommon.ZERROR) {
            NamedCommon.ZERROR = false;
            if (!SourceDB.ReConnect()) {
                uCommons.uSendMessage("(A) rFuel cannot reconnect successfully. Stoping now.");
                System.exit(0);
            }
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) Read from Library");
        pingString = ReadAnItem("BP.UPL", "properties","","","");
        if (NamedCommon.ZERROR || pingString.equals("")) {
            if (!SourceDB.ReConnect()) {
                uCommons.uSendMessage("(B) rFuel cannot reconnect successfully. Stoping now.");
                System.exit(0);
            }
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("---------------------------------------------------------------");
        if (NamedCommon.debugging) uCommons.uSendMessage("Confirmed: rFuel has a clean connection to Source DB.");
        if (NamedCommon.debugging) uCommons.uSendMessage("---------------------------------------------------------------");
    }

    public static void CloseAllFiles() {
        // do not use the uClose method - Reslience will put it into endless loop.

        if (NamedCommon.sConnected) {
            if (NamedCommon.uRequests != null) {
                try {
                    NamedCommon.uRequests.close();
                    NamedCommon.uRequests = null;
                } catch (UniFileException e) {
                    NamedCommon.uRequests = null;
                }
            }

            int c1 = NamedCommon.PointerFiles.size();
            int c2 = NamedCommon.fHandles.size();
            int c3 = NamedCommon.u2Handles.size();
            int c4 = NamedCommon.OpenFiles.size();

            if ((c1 + c2 + c3) == 0) return;
            if (!NamedCommon.protocol.equals("u2cs")) return;

            if (NamedCommon.protocol.equals("u2cs") && !NamedCommon.uSession.isActive()) return;

            if (NamedCommon.isRest && !NamedCommon.ZERROR && NamedCommon.protocol.equals("u2cs")) {
                for (int of = 0; of < c3; of++) {
                    NamedCommon.u2Handles.set(0, uClose(NamedCommon.u2Handles.get(0)));
                    try {
                        NamedCommon.VOC.deleteRecord(NamedCommon.PointerFiles.get(of));
                        if (NamedCommon.debugging) uCommons.uSendMessage(NamedCommon.PointerFiles.get(of) + " removed from VOC");
                    } catch (UniFileException e) {
                        NamedCommon.u2Handles.set(0, null);
                    }
                }
            } else {
                if (!NamedCommon.isNRT) u2Commons.ClearVoc(NamedCommon.pid);
            }

            for (int of = 0; of < c2; of++) {
                if (NamedCommon.fHandles.get(of) == null) continue;
                try {
                    if (NamedCommon.fHandles.get(of).isOpen()) NamedCommon.fHandles.get(of).close();
                } catch (UniFileException e) {
                    uCommons.uSendMessage("CloseAllFiles(): cannot close() [" + NamedCommon.fHandles.get(of).getFileName() + "]");
                    uCommons.uSendMessage(e.getMessage());
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                }
            }
        }

        NamedCommon.fHandles.clear();
        NamedCommon.fTagNames.clear();
        NamedCommon.u2Handles.clear();
        if (NamedCommon.sConnected) NamedCommon.PointerFiles.clear();
        if (NamedCommon.sConnected) NamedCommon.OpenFiles.clear();
        NamedCommon.U2File = null;
    }

    public static String Cleanse(String datum) {
        datum = datum.replaceAll("[\\p{Cc}]", "");
        datum = datum.replaceAll("[\\p{Cntrl}]", "");
        datum = datum.replaceAll("[^\\x00-\\x7F]", "");
        return datum;
    }
}
