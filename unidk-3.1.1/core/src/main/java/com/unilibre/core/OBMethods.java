package com.unilibre.core;

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class OBMethods {

    // #variables# --------------------------------------------------
    //  #self#      =   page            this page
    //  #prev#      =   prevPage        previous page
    //  #next#      =   nextPage        next page
    //  #last#      =   lastPage        last page
    //  #rtotal#    =   totRecs         total nbr records
    //  #ptotal#    =   lastPage        total nbr pages
    //  #pgsize#    =   pgSize          number of items per page
    //  #lowdate#   =   lowdate         the date filter
    //  #p-key#     =   iid             the item id (primary key)
    // ----------- ---------------------------------------------------

    private static ArrayList<String> jField = new ArrayList<>();
    private static ArrayList<String> jValue = new ArrayList<>();
    private static boolean needsBlank = true, noCache = false;
    private static ArrayList<String> impTmpls = new ArrayList<>();
    private static ArrayList<String> impTlines = new ArrayList<>();
    private static ArrayList<String> impLines = new ArrayList<>();
    private static int pg, pgSz, prevPage, nextPage, lastPage, totRecs, dsdControl;
    private static int nbrDays = 366;
    private static String page, pgSize, lowdate, iid, blank = "[<END>]";

    public static String GetCustomerID() {
        String ans = "<<PASS>>";
        Reset();
        needsBlank = false;

        String subr = "SR.GETCUSTOMERID";
        String[] callArgs = new String[5];
        callArgs = ClearArgs(callArgs);
        String dsd = BuildDSD();
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        iid = APImsg.APIget("item");
        callArgs[0] = "";
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";
        uCommons.uSendMessage("   .) Calling " + subr);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String payload = "";
        payload = callArgs[4];
        payload = CleansePayload(payload);
        String[] recArr = payload.split(NamedCommon.FMark);
        totRecs = recArr.length;

        CheckResults(dsd, recArr);

        uCommons.uSendMessage("   .) GetCustomerId DB fetch complete. Build JSON payload");

        GetHashVars();

        String jsnText, templateLine = "";
        if (!NamedCommon.ZERROR) {
            String[] lParts = (payload + NamedCommon.sep + blank).split(NamedCommon.sep);
            String tmp1, tmp2;
            int fnd, eoi;
            String[] jsnLines;
            String tPath = NamedCommon.BaseCamp + "/templates/", fqfn = "";
            fqfn = tPath + NamedCommon.Template;
            jsnText = uCommons.ReadDiskRecord(fqfn);
            jsnLines = jsnText.split("\\r?\\n");
            eoi = jsnLines.length;
            jsnText = "";
            for (int i = 0; i < eoi; i++) {
                templateLine = jsnLines[i].trim();
                if (templateLine.trim().startsWith("#")) continue;
                while (templateLine.contains("$")) {
                    tmp1 = uCommons.FieldOf(templateLine, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = NamedCommon.SubsList.indexOf(tmp1);
                    if (fnd >= 0) {
                        tmp2 = lParts[fnd];
                    } else {
                        tmp2 = "";
                    }
                    if (tmp2.equals(blank)) tmp2 = "";
                    templateLine = templateLine.replace(tmp1, tmp2);
                }
                if (templateLine.contains("#")) templateLine = SubsHashVars(templateLine);
                jsnText += templateLine;
            }
        } else {
            jsnText = NamedCommon.Zmessage;
        }

        Standardise(jsnText);

        return ans;
    }

    public static String GetCustomer() {
        String ans = "<<PASS>>";
        Reset();

        String subr = "SR.GETCUSTOMER";
        String[] callArgs = new String[5];
        for (int i = 0; i < callArgs.length; i++) { callArgs[i] = ""; }
        String dsd = BuildDSD();
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        iid = APImsg.APIget("item");
//        uCommons.uSendMessage("   .) ");
//        uCommons.uSendMessage("   .) CDR-DataHolder : -------------------------------------- ");
//        uCommons.uSendMessage("   .) DSD: " + dsd);
        callArgs[0] = "";
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";
        uCommons.uSendMessage("   .) Calling " + subr);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL";
        }
        String payload = "";
        payload = callArgs[4];
        if (NamedCommon.CleanseData) payload = u2Commons.Cleanse(payload);
        payload = CleansePayload(payload);
        String[] recArr = payload.split(NamedCommon.FMark);
        totRecs = recArr.length;

        CheckResults(dsd, recArr);

        uCommons.uSendMessage("   .) GetCustomer DB fetch complete. Build JSON payload");

        GetHashVars();

        String jsnText, templateLine = "";
        if (!NamedCommon.ZERROR) {
            String[] lParts = (payload + NamedCommon.sep + blank).split(NamedCommon.sep);

            String tmp1, tmp2;
            int fnd, eoi;
            String[] jsnLines;
            String tPath = NamedCommon.BaseCamp + "/templates/", fqfn = "";
            fqfn = tPath + NamedCommon.Template;
            jsnText = uCommons.ReadDiskRecord(fqfn);
            jsnLines = jsnText.split("\\r?\\n");
            eoi = jsnLines.length;
            jsnText = "";
            for (int i = 0; i < eoi; i++) {
                templateLine = jsnLines[i].trim();
                if (templateLine.trim().startsWith("#")) continue;
                while (templateLine.contains("$")) {
                    tmp1 = uCommons.FieldOf(templateLine, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = NamedCommon.SubsList.indexOf(tmp1);
                    if (fnd >= 0) {
                        tmp2 = lParts[fnd];
                    } else {
                        tmp2 = "";
                    }
                    while(tmp2.contains("$")) { tmp2 = tmp2.replace("$", "__"); }
                    while(tmp2.contains("#")) { tmp2 = tmp2.replace("#", "~~"); }
                    if (tmp2.equals(blank)) tmp2 = "";
                    templateLine = templateLine.replace(tmp1, tmp2);
                }
                if (templateLine.contains("#")) templateLine = SubsHashVars(templateLine);
                jsnText += templateLine;
            }
        } else {
            jsnText = NamedCommon.Zmessage;
        }

        Standardise(jsnText);

        return ans;
    }

    public static String GetAccounts() {
        String ans = "<<PASS>>", jsnText="";
        Reset();

        String dsd = BuildDSD();
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String err = "";
        iid = APImsg.APIget("item");
        String subr = "SR.GETACCOUNTS";
        String[] callArgs = new String[5];
        for (int i = 0; i < callArgs.length; i++) { callArgs[i] = ""; }
        callArgs[0] = err;
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";
        uCommons.uSendMessage("   .) Calling " + subr);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String payload = callArgs[4];
        if (NamedCommon.CleanseData) payload = u2Commons.Cleanse(payload);
        payload = CleansePayload(payload);
        String[] recArr = payload.split(NamedCommon.FMark);
        String[] chkArr = recArr[0].split(NamedCommon.sep);
        totRecs = recArr.length;
        CheckComponents(dsd, chkArr);

        uCommons.uSendMessage("   .) GetAccounts fetched " + totRecs + " records. Build them into a JSON payload");
        if (!NamedCommon.ZERROR) {
            jsnText = OBHandler(recArr);
        } else {
            jsnText = NamedCommon.Zmessage;
        }

        Standardise(jsnText);
        NamedCommon.DataList.clear();
        NamedCommon.DataList.add(jsnText);
        jsnText = "";

        return ans;
    }

    public static String GetPayees() {
        String ans = "<<PASS>>";
        Reset();

        needsBlank = false;

        String dsd = BuildDSD();
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String err = "";
        iid = APImsg.APIget("item");
//        uCommons.uSendMessage("   .) ");
//        uCommons.uSendMessage("   .) CDR-DataHolder : -------------------------------------- ");
        String subr = "SR.GETPAYEES";
        String[] callArgs = new String[5];
        callArgs = ClearArgs(callArgs);
        callArgs[0] = err;
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";
        uCommons.uSendMessage("   .) Calling " + subr);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String payload = callArgs[4];
        payload = CleansePayload(payload);
        if (NamedCommon.CleanseData) payload = u2Commons.Cleanse(payload);
        String[] recArr = payload.split(NamedCommon.FMark);
        totRecs = recArr.length;

        CheckResults(dsd, recArr);

        uCommons.uSendMessage("   .) GetPayees DB fetch complete. Build JSON payload");

        String jsnText="";
        if (!NamedCommon.ZERROR) {
            jsnText = OBHandler(recArr);
            if (NamedCommon.ZERROR) ans="<<FAIL>>";
        } else {
            jsnText = NamedCommon.Zmessage;
        }

        Standardise(jsnText);

        return ans;
    }

    public static String GetPayments() {
        String ans = "<<PASS>>";
        Reset();
        needsBlank = false;

        String dsd = BuildDSD();
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String err = "";
        iid = APImsg.APIget("item");
        String subr = "SR.GETPAYMENTS";
        String[] callArgs = new String[5];
        for (int i = 0; i < callArgs.length; i++) { callArgs[i] = ""; }
        callArgs[0] = err;
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";
        uCommons.uSendMessage("   .) Calling " + subr);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String payload = callArgs[4];
        payload = CleansePayload(payload);
        if (NamedCommon.CleanseData) payload = u2Commons.Cleanse(payload);
        String[] recArr = payload.split(NamedCommon.FMark);
        String[] chkArr = recArr[0].split(NamedCommon.sep);
        totRecs = recArr.length;

        CheckResults(dsd, recArr);

        uCommons.uSendMessage("   .) GetPayments DB fetch complete. Build JSON payload");

        String jsnText;
        if (!NamedCommon.ZERROR) {
            jsnText = OBHandler(recArr);
            if (NamedCommon.ZERROR) ans = jsnText;
        } else {
            jsnText = NamedCommon.Zmessage;
        }

        Standardise(jsnText);

        return ans;
    }

    public static String GetTransactions() {
        String ans = "<<PASS>>";
        if (!uCommons.APIGetter("limit").equals("")) {
            try {
                nbrDays = Integer.valueOf(uCommons.APIGetter("limit"));
            } catch (NumberFormatException nfe){
                uCommons.uSendMessage("**********************************************************************");
                uCommons.uSendMessage("   .) Tran limit ERROR: ["+uCommons.APIGetter("limit")+"] not an INTEGER. Using 366");
                uCommons.uSendMessage("**********************************************************************");
                nbrDays = 366;
            }
        }
        Reset();

        GetHashVars();

        String dsd = BuildDSD();
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String iid = APImsg.APIget("item");

        String payload = "", totRecs = "";
        String subr = "SR.READTRANS";
        String[] callArgs = new String[6];
        uCommons.uSendMessage("   .) --------------------------------------------------------- ");
        for (int i = 0; i < callArgs.length; i++) { callArgs[i] = ""; }
        callArgs[1] = dsd;
        callArgs[2] = lowdate;
        callArgs[3] = iid;
        callArgs[4] = iid;
        callArgs[5] = "";
        uCommons.uSendMessage("   .) Calling " + subr);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL";
        }
        totRecs = callArgs[5];
        uCommons.uSendMessage("   .) Built the dataset with " + totRecs + " transactions.");

        callArgs = new String[5];
        for (int i = 0; i < callArgs.length; i++) {
            callArgs[i] = "";
        }
        subr = "SR.GETTRANS";
        callArgs[1] = APImsg.APIget("page");
        callArgs[2] = APImsg.APIget("page-size");
        callArgs[3] = iid;
//        callArgs[3] = correl;
        uCommons.uSendMessage("   .) Calling " + subr + " for page # " + page);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            NamedCommon.DataList.clear();
            NamedCommon.DataList.add(NamedCommon.Zmessage.trim());
            return "<<FAIL";
        }
        payload = callArgs[4];
        payload = CleansePayload(payload);
        if (NamedCommon.CleanseData) payload = u2Commons.Cleanse(payload);
        String[] recArr = payload.split(NamedCommon.FMark);
        int eol = recArr.length, eop;
        uCommons.uSendMessage("   .) " + eol + " transactions returned. In a page-size of " + pgSize);

        String[] chkArr = recArr[0].split(NamedCommon.sep);
        CheckComponents(dsd, chkArr);

        JSONObject jMaster = new JSONObject();
        JSONObject jLinks = new JSONObject();
        JSONObject jMeta = new JSONObject();
        JSONObject jData = new JSONObject();

        jLinks.put("self", page);
        jLinks.put("first", "1");
        jLinks.put("prev", String.valueOf(prevPage));
        jLinks.put("next", String.valueOf(nextPage));
        jLinks.put("last", String.valueOf(lastPage));

        jMeta.put("totalRecords", totRecs);
        jMeta.put("totalPages", String.valueOf(lastPage));

        CreateSubsArrays();
        if (NamedCommon.ZERROR) return "<<FAIL>>";

        String[] lParts;
        String key, val, tmp1, tmp2;
        int fnd, eoi = jField.size();

        JSONArray jdatArr = new JSONArray();
        JSONObject arrElement;

        uCommons.uSendMessage("   .) Build transaction array");
        for (int i = 0; i < eol; i++) {
            lParts = (recArr[i] + NamedCommon.sep + blank).split(NamedCommon.sep);
            arrElement = new JSONObject();
            for (int c = 0; c < eoi; c++) {
                key = jField.get(c);
                val = jValue.get(c);
                while (val.contains("$")) {
                    tmp1 = uCommons.FieldOf(val, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = NamedCommon.SubsList.indexOf(tmp1);
                    if (fnd >= 0) {
                        tmp2 = lParts[fnd];
                        if (tmp2.equals(blank)) tmp2 = "";
                        val = val.replace(tmp1, tmp2);
                    } else {
                        val = "unprocessed-data-element-"+tmp1;
                        break;
                    }
                }
                arrElement.put(key, val);
            }
            jdatArr.put(arrElement);
            arrElement = null;
        }

        uCommons.uSendMessage("   .) load transaction array");
        jData.put("transactions", jdatArr);
        uCommons.uSendMessage("   .) load data section");
        jMaster.put("data", jData);
        uCommons.uSendMessage("   .) load links section");
        jMaster.put("links", jLinks);
        uCommons.uSendMessage("   .) load meta section");
        jMaster.put("meta", jMeta);
        uCommons.uSendMessage("   .) load Master");
        NamedCommon.DataList.clear();
        NamedCommon.DataList.add(jMaster.toString());
        return ans;
    }

    public static String GetTransactionsV2() {
        uCommons.uSendMessage("   .) ");
        String ans = "<<PASS>>";

        if (!uCommons.APIGetter("limit").equals("")) {
            try {
                nbrDays = Integer.valueOf(uCommons.APIGetter("limit"));
            } catch (NumberFormatException nfe){
                uCommons.uSendMessage("**********************************************************************");
                uCommons.uSendMessage("   .) Tran V2 ERROR: ["+uCommons.APIGetter("limit")+"] not an INTEGER. Using 366");
                uCommons.uSendMessage("**********************************************************************");
                nbrDays = 366;
            }
        }
        Reset();
        needsBlank = false;

        String dsd = BuildDSD();
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        dsdControl = dsd.split(NamedCommon.FMark).length;
        iid = APImsg.APIget("item");
        if (iid.equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "No customer details provided.";
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        String subr = "SR.READTRANSV2";
        String[] callArgs = new String[6];

        // ------- cleanup database cache of transactions IF there was a recent CDR request ----------------

        // Separate the function of purge-cache from paging !!!
//        GetHashVars();
//        Boolean purgeCach = (APImsg.APIget("purge-cache").toLowerCase().equals("true"));
//        noCache = (pgSz == 0 || pg   == 0 || purgeCach);

        noCache = (APImsg.APIget("purge-cache").toLowerCase().equals("true"));

        if (noCache) {
            // BaaS request: do NOT cache in &SavedLists&
            String killCache = "CDR.TIDYUP " + iid;
            uCommons.uSendMessage("   .) " + killCache);
            u2Commons.uniExec(killCache);
        }
        // -------------------------------------------------------------------------------------------------

        String fromDte = APImsg.APIget("date-from");
        String toDte   = APImsg.APIget("date-to");
        if (!fromDte.equals("") && !toDte.equals("")) {
            String fmt = "yyyy-MM-dd";
            int lowDate = uCommons.iconvD(fromDte, fmt);
            int highDte = uCommons.iconvD(toDte, fmt);
            lowdate = lowDate + "-" + highDte;
            uCommons.uSendMessage("   .) fromDte: " + fromDte);
            uCommons.uSendMessage("   .)   toDte: " + toDte);
            uCommons.uSendMessage("   .)     fmt: " + fmt);
        }
        uCommons.uSendMessage("   .) lowdate: " + lowdate);

        callArgs = ClearArgs(callArgs);
        callArgs[1] = dsd;
        callArgs[2] = lowdate;
        callArgs[3] = iid;
        callArgs[4] = iid;  // This is CorrelationID but iid makes more sense in UV.
        callArgs[5] = "";
        uCommons.uSendMessage("   .) Calling " + subr);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL";
        }

        totRecs = Integer.valueOf(callArgs[5]);
        uCommons.uSendMessage("   .) total of " + totRecs + " transactions for the account");
        uCommons.uSendMessage("   .) GetHashVars");
        GetHashVars();

        // Separate the function of purge-cache from paging !!!
//        noCache = (pgSz == 0 || pg   == 0 || purgeCach);
//        if (noCache) {
//            pgSz = 999999999;
//            pg   = 1;
//            page = String.valueOf(pg);
//            pgSize = String.valueOf(pgSz);
//            nextPage = 1;
//            lastPage = 1;
//        } else {
//            if (pgSz < 10) pgSz = 10;
//            if (pg < 1) pg = 1;
//        }

        callArgs = new String[5];
        callArgs = ClearArgs(callArgs);
        subr = "SR.GETTRANS";
        callArgs[1] = page;
        callArgs[2] = pgSize;
        callArgs[3] = iid;
        uCommons.uSendMessage("   .) Calling " + subr + " for page # " + page);
        callArgs = u2Commons.uniCallSub(subr, callArgs);
        if (!callArgs[0].equals("")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = callArgs[0];
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL";
        }
        String payload = CleansePayload(callArgs[4]);
        if (NamedCommon.CleanseData) payload = u2Commons.Cleanse(payload);
        String[] recArr = payload.split(NamedCommon.FMark);
        uCommons.uSendMessage("   .) returning transactions, " + pgSize + " per page.");

        callArgs = ClearArgs(callArgs);
        CheckResults(dsd, recArr);

        String jsnText = OBHandlerV2(recArr);       // PERFECT for simple one only import template !!
        if (NamedCommon.ZERROR) ans = jsnText;

        if (noCache) {
            // BaaS request: do NOT cache in &SavedLists&
            String killCache = "CDR.TIDYUP " + iid;
            uCommons.uSendMessage("   .) " + killCache);
            u2Commons.uniExec(killCache);
        }

        Standardise(jsnText);

        uCommons.uSendMessage("   .) Done.");
        noCache = false;
        return ans;
    }

    public static String GetJointAccounts() {
        uCommons.uSendMessage("   .) ");
        String ans = "<<PASS>>";
        Reset();
        needsBlank = false;

        String dsd = BuildDSD();
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            return "<<FAIL>>";
        }
        NamedCommon.DataList.clear();;
        NamedCommon.DataList.add("under construction");
        return "";
    }

    private static void Standardise(String jsnText) {

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        String tmp1="";
        int psx1;
        while (jsnText.contains("@ID@")) {
            psx1 = jsnText.indexOf("@ID@");
            tmp1 = jsnText.substring(0,psx1) + iid + jsnText.substring(psx1+4, jsnText.length());
            jsnText = tmp1;
        }
        tmp1 = "";
        psx1 = 0;
        NamedCommon.DataList.clear();
        NamedCommon.DataList.add(jsnText.trim());

    }

    private static String[] ClearArgs(String[] inArgs) {
        for (int i = 0; i < inArgs.length; i++) { inArgs[i] = ""; }
        return inArgs;
    }

    private static void CheckComponents(String dsd, String[] chkArr) {
        if (NamedCommon.SubsList.size() != chkArr.length) {
//            NamedCommon.Zmessage = "ERROR: mismatch between dsd and returned data string.";
            uCommons.uSendMessage("   ). *****");
            uCommons.uSendMessage("   ). ERROR: mismatch between dsd and returned data string.");
            uCommons.uSendMessage("   ). SubsList() found "+NamedCommon.SubsList.size()+" items but rFuel returned "+chkArr.length+" items.");
//            ShowComponents(dsd, chkArr);
            uCommons.uSendMessage("   ). Results should not be trusted.");
            uCommons.uSendMessage("   ). *****");
        }
    }

    private static void Reset() {
        jField.clear();
        jValue.clear();
        needsBlank = true;
        impTmpls.clear();
        impLines.clear();
        pg = 0;
        pgSz = 0;
        prevPage = 0;
        nextPage = 0;
        lastPage = 0;
        totRecs = 0;
        page = "";
        pgSize = "";
        lowdate = "";
        iid = "";
        String today = uCommons.GetToday();
        lowdate = String.valueOf(Integer.valueOf(today) - nbrDays);
    }

    private static String OBHandlerV2(String[] recArr) {
        if (NamedCommon.SubsList.get(0).equals("")) NamedCommon.SubsList.remove(0);
//        uCommons.uSendMessage("   .) GetHashVars");
//        GetHashVars();
        uCommons.uSendMessage("   .) CreateSubsArrays");
        CreateSubsArrays();
        if (NamedCommon.ZERROR) return "<<FAIL>>";
        impTmpls.clear();
        impLines.clear();
        uCommons.uSendMessage("   .) GetTemplate");
        String jsnText = GetTemplate(NamedCommon.Template);
        if (NamedCommon.ZERROR) return "<<FAIL>>";

        JSONObject jMaster = null;
        JSONObject oMaster = new JSONObject();
        JSONObject innerObj = null;
        Iterator<String> jKeys = null;
        Iterator<String> innerKeys = null;
        String zkey, zval, ikey, akey;

        String[] tmpArr = jsnText.split("\\r?\\n");
        int eot = tmpArr.length;
        String tmpStr, outStr = "";
        for (int t = 0; t < eot; t++) {
            tmpStr = tmpArr[t].trim();
            if (!tmpStr.startsWith("#")) outStr += tmpStr + "\n";
        }
        jsnText = outStr;
        try {
            jMaster = new JSONObject(jsnText);
            jKeys = jMaster.keys();
            while (jKeys.hasNext()) {
                zkey = jKeys.next();
                zval = jMaster.get(zkey).toString();

                if (zval.startsWith("{")) {
                    innerObj = (JSONObject) jMaster.get(zkey);
                    innerKeys = innerObj.keys();
                    ikey = innerKeys.next();
                    if (innerObj.get(ikey) instanceof JSONArray) {
                        uCommons.uSendMessage("   .) load the [" + zkey + "].[" + ikey + "] array");
                        akey = ikey;
                        innerObj = SubsArrayHandler(akey, recArr);
                        oMaster.put(zkey, innerObj);
                    } else {
                        uCommons.uSendMessage("   .) load " + zkey + " section");
                        zval = SubsHashVars(zval);
                        innerObj = new JSONObject(zval);
                        oMaster.put(zkey, innerObj);
                    }
                } else {
                    uCommons.uSendMessage("   .) load " + zkey + " section");
                    zval = SubsHashVars(zval);
                    oMaster.put(zkey, zval);
                }
                innerObj = null;
            }
        } catch (JSONException je) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "JSON Error: " + je.getMessage();
            uCommons.uSendMessage(NamedCommon.Zmessage);
            return "<<FAIL>>";
        }

        uCommons.uSendMessage("   .) Stringify JSONObject.");
        jsnText = oMaster.toString();
        jsnText = jsnText.replaceAll("\\r?\\n", "");
        return jsnText;
    }

    private static JSONObject SubsArrayHandler(String ikey, String[] recArr) {
        JSONObject jObject = new JSONObject();
        JSONArray jArray = new JSONArray();
        JSONObject arrElement = null;

        String key, val, tmp1, tmp2;
        String[] lParts;

        int nbrRecs = recArr.length, eoi = jField.size(), fnd, lpLen;

        for (int r = 0; r < nbrRecs; r++) {
            lParts = (recArr[r] + NamedCommon.sep + blank).split(NamedCommon.sep);
            lpLen  = lParts.length;
            arrElement = new JSONObject();
            // --------------------------------------------------------------------
            //  Need to read the import template and subs the variables
            //  THEN arrElement.put(lineKey, lineVal);
            //  THEN jArray.put(arrElement);
            //  THEN jObject.put(ikey, jArray);
            // --------------------------------------------------------------------
            for (int c = 0; c < eoi; c++) {
                key = jField.get(c);
                val = jValue.get(c);
                while (val.contains("$")) {
                    tmp1 = uCommons.FieldOf(val, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = NamedCommon.SubsList.indexOf(tmp1);
                    if (fnd >= 0) {
                        if (fnd < lpLen) {
                            tmp2 = lParts[fnd];
                            if (tmp2.equals(blank)) tmp2 = "";
                            val = val.replace(tmp1, tmp2);
                        } else {
//                            val = "empty-data-element-1";
                            val = "";
                        }
                    } else {
                        val = "unmapped-data-element-"+tmp1;
                        break;
                    }
                }
                arrElement.put(key, val);
            }
            jArray.put(arrElement);
            arrElement = null;
        }
        jObject.put(ikey, jArray);
        return jObject;
    }

    public static String CleansePayload(String payload) {
        payload = payload.replaceAll("\\$", "__");
        payload = payload.replaceAll("\\#", "~~");
        return uCommons.jParse(payload);
    }

    private static String OBHandler(String[] recArr) {
        GetHashVars();
//        CreateSubsArrays();     // not sure this is needed - jField & jValue arrays
        impTmpls.clear();
        impTlines.clear();
        impLines.clear();
        String[] jsnMaster = GetImportTemplates(NamedCommon.Template);       // Add imports to impTemp list
        // -----------------------------------------------------------------------
        String jsnText = "", masterText;
        masterText = DoHashVarSubs(jsnMaster, recArr);
        jsnMaster = masterText.split("\\r?\\n");
        LoadAllTemplates();
        // -----------------------------------------------------------------------
        if (NamedCommon.ZERROR) return "<<FAIL>>";
        if (impTmpls != null) {
            String[] chkLines = new String[1];
            int eol = impTmpls.size();
            for (int i = 0; i < eol; i++) {
                chkLines[0] = impLines.get(i);
                jsnText = DoHashVarSubs(chkLines, recArr);
                impLines.set(i, jsnText);
            }
            DollarVarsImportTemplates(recArr);     // Do string substituions in all import array items
        }

        if (impLines.size() == 0) {
            jsnText = StitchTogetherV2(masterText);
        } else {
            jsnText = StitchTogether(masterText);
        }

        jsnText = jsnText.replaceAll("\\r?\\n", "");
        return jsnText;
    }

    private static void LoadAllTemplates() {
        ArrayList<String> tmpDone = new ArrayList<>();
        tmpDone.add(NamedCommon.Template);
        String[] tmpLineArray;
        String tmpLines;
        int eot, eoc;
        boolean more = true;
        while (more) {
            eot = impTmpls.size();
            for (int t = 0; t < eot; t++) {
                if (tmpDone.indexOf(impTmpls.get(t)) < 0) {
                    tmpLineArray = GetImportTemplates(impTmpls.get(t));        // Add imports to impTemp list
                    tmpLines = "";
                    for (int tl = 0; tl < tmpLineArray.length; tl++) { tmpLines += tmpLineArray[tl] + "\n"; }
                    impTlines.set(t, tmpLines);
                    tmpLineArray = null;
                    tmpLines = "";
                    tmpDone.add(impTmpls.get(t));
                }
            }
            eoc = impTmpls.size();
            more = (eot != eoc);
        }
        tmpDone.clear();
    }

    private static String StitchTogether(String jsnText) {

        // stitch them all together into jsnText

        // ---------------- support notes ------------ this is COMPLICATED !!
        //  Objects that MATTER :
        //  1.  impLines    - ArrayList: holds prebuilt responses for each import template impTmpl
        //  2.  impRecs     - ArrayList: int: defines which linePart to work with, then increments!
        //  3.  lineParts   - String[] : impLines holds multi lines, separated by <fm> in one lone string.
        //                             : lineParts are these lines, accessible by location in the string.
        //  4.  posx        - int      : identifies which template we are working with.
        //  5.  recPos      - int      : identifies which record from lineParts to use.
        //  6.  prePos      - int      : the automatic re-import setup for the next group is VITAL.
        //                             : prePos is calculated and appended to the import line so we know
        //                             : which linePart we are up to - otherwise everything is re-imported again.
        //  7.  tmpLines    - String[] : this is used to interrogate the lines being imported, in case there are imports
        //                             : inside the block. If there are imports inside the block, set the import to use
        //                             : the SAME recPos as this block - do this by tagging the import with prePos.
        // -------------------------------------------------------------------------------------------------------------

        ArrayList<String> jsnArray = new ArrayList<>();

        String[] lineParts, tmpLines;
        String templateLine, tmplName, saveLine;
        boolean reStitch = true, hasHash=false;
        int posx, recPos, prePos, eot, eoi, startFrom=0;

        ArrayList<Integer> impRecs = new ArrayList<>();
        for (int i = 0; i < impLines.size(); i++) { impRecs.add(0); }

        while (reStitch) {
            hasHash = jsnText.contains("#");
            jsnArray = new ArrayList<String>( Arrays.asList(jsnText.split("\\r?\\n")));
            eoi = jsnArray.size();
            jsnText = "";
            for (int i = 0; i < eoi; i++) {
                templateLine = jsnArray.get(i);
                if (i >= startFrom) {
                    templateLine = templateLine.replaceAll("\\r?\\n", "");
                    if (templateLine.startsWith("%import%")) {
                        if (startFrom < i) startFrom = i;
                        saveLine = templateLine;
                        tmplName = templateLine.replace("%import%", "").trim();
                        prePos = 999;
                        if (tmplName.endsWith("~")) {
                            prePos = Integer.valueOf(uCommons.FieldOf(tmplName, "~", 2));
                            tmplName = uCommons.FieldOf(tmplName, "~", 1);
                        }

                        posx = impTmpls.indexOf(tmplName);
                        if (posx < 0) continue;

                        lineParts = impLines.get(posx).split(NamedCommon.FMark);
                        templateLine = "";

                        if (prePos != 999) {
                            recPos = prePos;
                        } else {
                            recPos = impRecs.get(posx);
                        }
                        if ((recPos + 1) >= lineParts.length) saveLine = "";

                        if (lineParts.length > recPos) {
                            if (!saveLine.equals("")) {
                                if (saveLine.endsWith("~")) {
                                    // NC.Template drives ALL templates
                                    // impLines(0) drives the subsequent templates so posx = 0 is the KEY !!!
                                    // impLines(n) hang off impLines(0)
                                    eot = recPos;
                                    if (posx == 0) eot = totRecs;
                                    if (prePos >= eot) {
                                        saveLine = "";
                                    } else {
                                        saveLine = uCommons.FieldOf(saveLine, "~", 1) + "~" + (recPos + 1) + "~";
                                    }
                                } else {
                                    saveLine += "~" + (recPos + 1) + "~";
                                }
                            }
                            templateLine += lineParts[recPos];
                            if (templateLine.contains("%import%")) {
                                tmpLines = templateLine.split("\\r?\\n");
                                eot = tmpLines.length;
                                templateLine = "";
                                for (int t = 0; t < eot; t++) {
                                    if (tmpLines[t].startsWith("%import%")) {
                                        tmpLines[t] = tmpLines[t].trim() + "~" + recPos + "~";
                                    }
                                    if (!tmpLines[t].trim().equals("")) templateLine += tmpLines[t] + "\n";
                                }
                                tmpLines = null;
                                eot = 0;
                            }
                            if (!templateLine.endsWith("\n")) templateLine += "\n";
                            templateLine += saveLine;
                            recPos++;
                            impRecs.set(posx, recPos);
                            tmplName = "";
                        }
                    }
                    if (hasHash) {
                        if (templateLine.contains("#")) templateLine = SubsHashVars(templateLine);
                    }
                }
                if (!templateLine.equals("")) jsnText += templateLine + "\n";
            }
            reStitch = jsnText.contains("%import%");
        }
        return jsnText;
    }

    private static String StitchTogetherV2(String jsnText) {

        // stitch them all together into jsnText

        ArrayList<String> jsnArray = new ArrayList<>();

        String templateLine, tmplName, importLine;
        boolean reStitch = true, hasHash=false;
        int posx, eoi;

        ArrayList<Integer> impRecs = new ArrayList<>();
        for (int i = 0; i < impLines.size(); i++) { impRecs.add(0); }

        while (reStitch) {
            hasHash = jsnText.contains("#");
            jsnArray = new ArrayList<String>( Arrays.asList(jsnText.split("\\r?\\n")));
            eoi = jsnArray.size();
            jsnText = "";
            for (int i = 0; i < eoi; i++) {
                templateLine = jsnArray.get(i);

                templateLine = templateLine.replaceAll("\\r?\\n", "");
                if (templateLine.startsWith("%import%")) {
                    tmplName = templateLine.replace("%import%", "").trim();
                    posx = impTmpls.indexOf(tmplName);
                    if (posx < 0) continue;
                    importLine = impLines.get(posx);
                    templateLine = importLine.replace(NamedCommon.FMark, "");
                    if (!templateLine.endsWith("\n")) templateLine += "\n";
                    tmplName = "";
                }
                if (hasHash) {
                    if (templateLine.contains("#")) templateLine = SubsHashVars(templateLine);
                }
                if (!templateLine.equals("")) jsnText += templateLine + "\n";
            }
            reStitch = jsnText.contains("%import%");
        }
        return jsnText;
    }

    private static void DollarVarsImportTemplates(String[] dList) {

        // Do string substituions in all import array items

        boolean redo = false;
        boolean proc = true;
        String replUsed="";
        String replChr;
        int mvPos = 0, mvMax=0;
        int nbrImports = impTmpls.size();
        int nbrRecs = dList.length, fnd, eol;
        String[] jsnLines, lParts, chkArr;
        String templateLine, jsnTemp = "", tmp1, tmp2;

        for (int r = 0; r < nbrRecs; r++) {
            lParts = (dList[r] + NamedCommon.sep + blank).split(NamedCommon.sep);
            eol = lParts.length - 1;
            redo = false;
            mvPos = 0;
            mvMax=0;
            for (int i = 0; i < nbrImports; i++) {
                proc = true;
                while (proc) {
                    jsnLines = impTlines.get(i).split("\\r?\\n");
                    int eoimp = jsnLines.length;
                    for (int j = 0; j < eoimp; j++) {
                        templateLine = jsnLines[j].trim();
                        if (templateLine.trim().startsWith("#")) continue;
                        while (templateLine.contains("$")) {
                            tmp1 = uCommons.FieldOf(templateLine, "\\$", 2);
                            tmp1 = "$" + tmp1 + "$";
                            fnd = NamedCommon.SubsList.indexOf(tmp1);
                            if (fnd >= 0) {
                                if (fnd <= eol) {
                                    tmp2 = lParts[fnd];
                                } else {
                                    tmp2 = "";
                                }
                                if (tmp2.equals(blank)) tmp2 = "";
                                if (!redo && tmp2.contains(NamedCommon.VMark)) redo = true;
                                chkArr = tmp2.split(NamedCommon.VMark);
                                if (chkArr.length > mvMax) mvMax = chkArr.length;
                                if (chkArr.length > mvPos) {
                                    tmp2 = chkArr[mvPos];
                                } else {
                                    tmp2 = "";
                                }

                                if (tmp2.contains("$")) tmp2 = tmp2.replaceAll("\\$", "__");
                                if (tmp2.contains("#")) { tmp2 = tmp2.replaceAll("\\#", "~~"); }

                                templateLine = templateLine.replace(tmp1, tmp2);
                            } else {
                                tmp2 = "";
                                templateLine = templateLine.replace(tmp1, tmp2);
                            }
                        }

                        if (templateLine.contains("#")) templateLine = SubsHashVars(templateLine);
                        jsnTemp += templateLine + "\n";
                    }
                    if (redo) {
                        mvPos++;
                        if (mvPos >= mvMax) proc = false;
                    } else {
                        proc = false;
                    }
                }

                if (!impLines.get(i).equals("")) jsnTemp = impLines.get(i) + NamedCommon.FMark + jsnTemp;
                impLines.set(i, jsnTemp);
                jsnTemp = "";
            }
        }
    }

    private static String DoHashVarSubs(String[] jsnLines, String[] lParts) {

        // build jsnText which will be held on impLines

        String jsnText = "", templateLine, tmp1, tmp2;
        int eoi = jsnLines.length, fnd;
        for (int i = 0; i < eoi; i++) {
            templateLine = jsnLines[i].trim();
            // starts with # means it's a comment line
            if (templateLine.trim().startsWith("#")) continue;
            // contains a # means it has hashVars
            if (templateLine.contains("#")) templateLine = SubsHashVars(templateLine);
            // do $var$ subs ONLY when one record returned !!
            if (totRecs == 1) {
                while (templateLine.contains("$")) {
                    tmp1 = uCommons.FieldOf(templateLine, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = NamedCommon.SubsList.indexOf(tmp1);
                    if (fnd >= 0) {
                        tmp2 = lParts[fnd];
                        if (tmp2.equals(blank)) tmp2 = "";
                    } else {
                        tmp2 = "unknown-hash-value-"+tmp1;
                    }
                    templateLine = templateLine.replace(tmp1, tmp2);
                }
            }
            jsnText += templateLine.trim() + "\n";
        }
        return jsnText;
    }

    private static String GetTemplate(String template) {
        String tPath = NamedCommon.BaseCamp + "/templates/", fqfn = "";
        fqfn = tPath + NamedCommon.Template;
//        fqfn = tPath + template;
        return uCommons.ReadDiskRecord(fqfn);
    }

    private static String[] GetImportTemplates(String template) {
        String tPath = NamedCommon.BaseCamp + "/templates/", fqfn = "";
        fqfn = tPath + template;
        uCommons.uSendMessage("    ). Using template: "+fqfn);
        String jsnText = uCommons.ReadDiskRecord(fqfn);
        String[] jsnLines = jsnText.split("\\r?\\n");
        int eoi = jsnLines.length;
        jsnText = "";
        String templateLine;

        // Get all import templates into impTmpls array

        for (int i = 0; i < eoi; i++) {
            templateLine = jsnLines[i].trim();
            if (templateLine.trim().startsWith("#")) continue;
            if (templateLine.startsWith("%import%")) {
                templateLine = templateLine.replace("%import%", "").trim();
                if (NamedCommon.debugging) uCommons.uSendMessage("      ). Found template: "+templateLine);
                impTmpls.add(templateLine);
                impTlines.add("");
                impLines.add("");
                jsnText = "";
            }
        }
        if (totRecs > 1 && impTmpls.size() == 0) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "ERORR: more than one DB record in action list and only one output template.";
            uCommons.uSendMessage(NamedCommon.Zmessage);
            NamedCommon.DataList.clear();
            NamedCommon.DataList.add("");
        }
        return jsnLines;
    }

    private static void GetHashVars() {

        page = APImsg.APIget("page");
        pgSize = APImsg.APIget("page-size");

        try {
            pg = Integer.valueOf(page);
        } catch (NumberFormatException nfe) {
            pg = 1;
            page = String.valueOf(pg);
        }
        try {
            pgSz = Integer.valueOf(pgSize);
        } catch (NumberFormatException nfe) {
            pgSz = 1000;
            pgSize = String.valueOf(pgSz);
        }

        int divisor = pgSz;
        if (pgSz == 0) divisor = 1;
        try {
            double nbrPgs;
            nbrPgs = totRecs / divisor;
            lastPage = (int) nbrPgs;
//            if (lastPage < 1) lastPage = 1;
            if ((nbrPgs * pgSz) != totRecs) lastPage++;
        } catch (NumberFormatException nfe) {
            totRecs = 0;
            lastPage = 0;
        }
        if (pg > lastPage && lastPage > 0) {
            pg = lastPage;
            page = String.valueOf(pg);
        }
        nextPage = pg + 1;
        prevPage = pg - 1;
        if (prevPage < 1) prevPage = 1;
        if (nextPage > lastPage) nextPage = lastPage;
    }

    private static String SubsHashVars(String inLine) {
        String tmp1 = "";
        while (inLine.contains("#")) {
            tmp1 = uCommons.FieldOf(inLine, "\\#", 2);
            tmp1 = "#" + tmp1 + "#";
            switch (tmp1) {
                case "#self#":
                    inLine = inLine.replace(tmp1, String.valueOf(pg));
                    break;
                case "#prev#":
                    inLine = inLine.replace(tmp1, String.valueOf(prevPage));
                    break;
                case "#next#":
                    inLine = inLine.replace(tmp1, String.valueOf(nextPage));
                    break;
                case "#last#":
                    inLine = inLine.replace(tmp1, String.valueOf(lastPage));
                    break;
                case "#pgsize#":
                    inLine = inLine.replace(tmp1, String.valueOf(pgSz));
                    break;
                case "#lowdate#":
                    inLine = inLine.replace(tmp1, lowdate);
                    break;
                case "#p-key#":
                    inLine = inLine.replace(tmp1, iid);
                    break;
                case "#rtotal#":
                    inLine = inLine.replace(tmp1, String.valueOf(totRecs));
                    break;
                case "#ptotal#":
                    inLine = inLine.replace(tmp1, String.valueOf(lastPage));
                    break;
//                default:
                    //  some data has the "#" in it which confuses this logic.
//                    uCommons.uSendMessage(tmp1+" is an invalid hash variable.");
//                    return inLine;
            }
            tmp1 = "";
        }
        return inLine;
    }

    private static void CreateSubsArrays() {
        String jsnText, templateLine = "";
        String[] jsnLines, lineParts;
        String tPath = NamedCommon.BaseCamp + "/templates/", fqfn = "";

        jField.clear();
        jValue.clear();
        for (int t = 0; t < NamedCommon.Templates.size(); t++) {
            fqfn = tPath + NamedCommon.Templates.get(t);
            jsnText = uCommons.ReadDiskRecord(fqfn);
            if (NamedCommon.ZERROR) return;

            jsnLines = jsnText.split("\\r?\\n");
            templateLine = "";

            for (int i = 0; i < jsnLines.length; i++) {
                templateLine = jsnLines[i].trim();
                while (templateLine.contains("\"")) {
                    templateLine = templateLine.replace("\"", "");
                }
                while (templateLine.contains(",")) {
                    templateLine = templateLine.replace(",", "");
                }
                if (templateLine.startsWith("#")) continue;
                if (templateLine.contains("{")) continue;
                if (templateLine.contains("}")) continue;
                lineParts = templateLine.split(":");
                if (lineParts.length > 1) {
                    if (lineParts[1].contains("$")) {
                        jField.add(lineParts[0]);
                        jValue.add(lineParts[1].trim());
                    }
                }
            }
        }
    }

    public static String DebugDSD(){
        return BuildDSD();
    }

    private static String BuildDSD() {
        NamedCommon.Templates.clear();
        NamedCommon.TmplList.clear();
        NamedCommon.DataList.clear();

        NamedCommon.SubsList.clear();
        if (needsBlank) NamedCommon.SubsList.add("");

        String csvName, content, line, dsd = "", cma = ",", tmpl = "", subs = "", chk="";
        int nbrCsvItems = NamedCommon.csvList.length;

        for (int i = 0; i < nbrCsvItems; i += 1) {
            uCommons.uSendMessage("   .) BuildDSD(" + NamedCommon.csvList[i] + ")");
            csvName = NamedCommon.BaseCamp + "/maps/" + NamedCommon.csvList[i];
            ArrayList<String> csvLines = null;
            try {
                content = new String(Files.readAllBytes(Paths.get(csvName)));
                if (content.startsWith("ENC(")) content = uCipher.Decrypt(content);
                csvLines = new ArrayList<>(Arrays.asList(content.split("\\r?\\n")));
                int eol = csvLines.size();
                for (int j = 0; j < eol; j++) {
                    line = csvLines.get(j) + cma + cma + cma + cma + cma + cma + cma + cma + cma + cma + blank;

                    if (line.trim().equals("") || line.trim().startsWith("#")) continue;

                    String[] lParts = line.split("\\,");

                    dsd += lParts[0] + cma + lParts[1] + cma + lParts[2] + cma + lParts[3] + cma + lParts[4] + NamedCommon.FMark;
                    chk += lParts[0] + cma + lParts[1] + cma + lParts[2] + cma + lParts[3] + cma + lParts[4] + cma + lParts[6] + NamedCommon.FMark;

                    subs = lParts[6];
                    if (lParts.length < 8 || subs.equals("")) {
                        tmpl = NamedCommon.Template;
                    } else {
                        tmpl = lParts[7];
                    }
                    if (tmpl.equals("")) tmpl = NamedCommon.Template;
                    if (!subs.equals("")) {
                        if (NamedCommon.SubsList.indexOf(subs) < 0) NamedCommon.SubsList.add(subs);
                        if (NamedCommon.TmplList.indexOf(tmpl) < 0) NamedCommon.TmplList.add(tmpl);
                        if (NamedCommon.Templates.indexOf(tmpl) < 0) {
                            NamedCommon.Templates.add(tmpl);
                            String tPath = NamedCommon.BaseCamp + "/templates/", fqfn = "";
                            fqfn = tPath + NamedCommon.Template;
                            String junk = uCommons.ReadDiskRecord(fqfn);
                            if (NamedCommon.ZERROR) return "";
                            uCommons.uSendMessage("      .) Load: "+tmpl);
                        }
                    }
                }
            } catch (IOException e) {
                NamedCommon.Zmessage = "<<FAIL>> Cannot find " + csvName;
                NamedCommon.ZERROR = true;
                return "";
            }
        }
        return dsd;
    }

    private static void CheckResults(String dsd, String[] recArr) {

        if (!needsBlank) return;

        String[] chkArr = (recArr[0]+NamedCommon.sep+blank).split(NamedCommon.sep);
        int chk1 = NamedCommon.SubsList.size();
        int chk2 = chkArr.length;
        if (chk1 != chk2) {
//            NamedCommon.Zmessage = "ERROR: mismatch between dsd and returned data string.";
            uCommons.uSendMessage("   ). *****");
            uCommons.uSendMessage("   ). ERROR: mismatch between dsd and returned data string.");
            uCommons.uSendMessage("   ). SubsList() found "+NamedCommon.SubsList.size()+" items but rFuel returned "+chkArr.length+" items.");
//            ShowComponents(dsd, chkArr);
            uCommons.uSendMessage("   ). Results should not be trusted.");
            uCommons.uSendMessage("   ). *****");
        }
    }

    private static void ShowComponents(String dsd, String[] chkArr) {
        boolean alldone = false;
        String[] chkDSD = dsd.split(NamedCommon.FMark);
        int idx=0, width=30;
        String t1, t2, t3, t4;
        uCommons.uSendMessage("   ). "+uCommons.LeftHash("DSD", width)+" "+uCommons.LeftHash("SubsList()", width)+" "+uCommons.LeftHash("rFuel", width));
        uCommons.uSendMessage("   ). ------------------------------------------------------------------------------------------");
        while (!alldone) {
            t1="";t2=""; t4="";
            if (idx < NamedCommon.SubsList.size())  t1 = NamedCommon.SubsList.get(idx);
            if (idx >=NamedCommon.SubsList.size())  t1 = "--";
            if (idx < chkArr.length)                t2 = chkArr[idx];
            if (idx >=chkArr.length)                t2 = "--";
            t3 = uCommons.LeftHash(String.valueOf(idx), 3);
            if (idx < chkDSD.length)                t4 = chkDSD[idx];
            if (idx >=chkDSD.length)                t4 = "--";

            if (!(t1+t2).equals("") && !(t1+t2).equals("----")) {
                t1 = uCommons.LeftHash("\"" + t1 + "\"", width);
                t2 = uCommons.LeftHash("\"" + t2 + "\"", width);
                t4 = uCommons.LeftHash(t4, width);
                uCommons.uSendMessage("   "+ t3 + t4 + t1 + " " + t2);
            }

            idx++;
            if (idx > NamedCommon.SubsList.size() && idx > chkArr.length && idx > chkDSD.length) alldone=true;
        }
    }

}
