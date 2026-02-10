package com.unilibre.cdroem;

import com.northgateis.reality.rsc.RSC;
import com.northgateis.reality.rsc.RSCException;
import com.unilibre.cipher.uCipher;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import static com.unilibre.cdroem.commons.Day0;
import static java.time.temporal.ChronoUnit.DAYS;

public class nsOBMethods {

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

    private ArrayList<String> jField = new ArrayList<>();
    private ArrayList<String> jValue = new ArrayList<>();
    private boolean needsBlank = true, noCache = false;
    private nsCommonData comData;
    private ArrayList<String> impTmpls = new ArrayList<>();
    private ArrayList<String> impTlines = new ArrayList<>();
    private ArrayList<String> impLines = new ArrayList<>();
    private int pg, pgSz, prevPage, nextPage, lastPage, totRecs, dsdControl;
    private int nbrDays = 366;
    private String[] callArgs = new String[50];
    private String page, pgSize, lowdate, iid, blank = "[<END>]";
    private String FMark = "<fm>", sep = "<tm>", IMark = "<im>", KMark = "<km>", VMark = "<vm>", SMark = "<sm>";

    public String GetCustomerID(nsCommonData comdata) {
        comData = comdata;
        String ans = "<<PASS>>";
        Reset();
        needsBlank = false;

        String subr = "SR.GETCUSTOMERID";
        callArgs = ClearArgs(callArgs);

        String dsd = BuildDSD(comData);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        iid = comData.nsMsgGet("customer");
        if (iid.equals("")) iid = comData.nsMsgGet("item");
        callArgs[0] = "";
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = comData.nsMsgGet("correlationid");
        callArgs[4] = "";
        logger.logthis(comData.logHeader + "   .) Calling " + subr);

        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }
        
        String errCondition = callArgs[0];
        if (!errCondition.equals("")) {
            logger.logthis(comData.logHeader + "ERROR: Database error - " + errCondition);
            comData.ZERROR = true;
            comData.Zmessage = errCondition;
        }
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        String payload = "";
        payload = callArgs[4];
        payload = CleansePayload(payload);
        String[] recArr = payload.split(FMark);
        totRecs = recArr.length;

        CheckResults(dsd, recArr);

        logger.logthis(comData.logHeader + "   .) GetCustomerId DB fetch complete. Build JSON payload");

        GetHashVars();

        String jsnText, templateLine = "";
        if (!comData.ZERROR) {
            String[] lParts = (payload + sep + blank).split(sep);
            String tmp1, tmp2;
            int fnd, eoi;
            String[] jsnLines;
            String tPath = comData.BaseCamp + "/", fqfn = "";
            fqfn = tPath + comData.nsMapGet("template");
            jsnText = ReadFromDisk(fqfn);
            jsnLines = jsnText.split("\\r?\\n");
            eoi = jsnLines.length;
            jsnText = "";
            for (int i = 0; i < eoi; i++) {
                templateLine = jsnLines[i].trim();
                if (templateLine.trim().startsWith("#")) continue;
                while (templateLine.contains("$")) {
                    tmp1 = FieldOf(templateLine, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = comData.SubsList.indexOf(tmp1);
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
            jsnText = "";
        }

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        comData.DataList.clear();
        comData.DataList.add(jsnText.trim());
        return ans;
    }

    public String GetCustomer(nsCommonData comdata) {
        comData = comdata;
        String ans = "<<PASS>>";
        Reset();

        String subr = "SR.GETCUSTOMER";
        callArgs = ClearArgs(callArgs);
        String dsd = BuildDSD(comData);
        if (comData.ZERROR) return "";
        iid = comData.nsMsgGet("customer");
        if (iid.equals("")) iid = comData.nsMsgGet("item");
        callArgs[0] = "";
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";

        logger.logthis(comData.logHeader + "  .) Calling " + subr);
        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        if (!callArgs[0].equals("")) {
            comData.ZERROR = true;
            comData.Zmessage = callArgs[0];
            cdrServer.status = "406";
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            return "<<FAIL>>";
        }
        String payload = "";
        payload = callArgs[4];
        payload = CleansePayload(payload);
        String[] recArr = payload.split(FMark);
        totRecs = recArr.length;

        CheckResults(dsd, recArr);

        logger.logthis(comData.logHeader + "   .) GetCustomer DB fetch complete. Build JSON payload");

        GetHashVars();

        String jsnText, templateLine = "";
        if (!comData.ZERROR) {
            String[] lParts = (payload + sep + blank).split(sep);
            String tmp1, tmp2;
            int fnd, eoi;
            String[] jsnLines;
            String tPath = comData.BaseCamp + "/", fqfn = "";
            fqfn = tPath + comData.nsMapGet("template");
            jsnText = ReadFromDisk(fqfn);
            jsnLines = jsnText.split("\\r?\\n");
            eoi = jsnLines.length;
            jsnText = "";
            for (int i = 0; i < eoi; i++) {
                templateLine = jsnLines[i].trim();
                if (templateLine.trim().startsWith("#")) continue;
                while (templateLine.contains("$")) {
                    tmp1 = FieldOf(templateLine, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = comData.SubsList.indexOf(tmp1);
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
            jsnText = comData.Zmessage;
        }

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        comData.DataList.clear();
        comData.DataList.add(jsnText.trim());
        return ans;
    }

    public String GetAccounts(nsCommonData comdata) throws IOException {
        comData = comdata;
        String ans = "<<PASS>>";
        Reset();

        String dsd = BuildDSD(comData);
        if (comData.ZERROR) {
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData = null;
            return "<<FAIL>>";
        }
        String err = "";
        iid = comData.nsMsgGet("customer");
        if (iid.equals("")) iid = comData.nsMsgGet("item");
        String subr = "SR.GETACCOUNTS";
        callArgs = ClearArgs(callArgs);
        callArgs[0] = err;
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";
        logger.logthis(comData.logHeader + "   .) Calling " + subr);
        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        if (!callArgs[0].equals("")) {
            comData.ZERROR = true;
            comData.Zmessage = callArgs[0];
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData = null;
            return "<<FAIL>>";
        }
        String payload = callArgs[4];
        payload = CleansePayload(payload);
        String[] recArr = payload.split(FMark);
        String[] chkArr = recArr[0].split(sep);
        totRecs = recArr.length;
        CheckComponents(dsd, chkArr);

        logger.logthis(comData.logHeader + "   .) GetAccounts fetched " + totRecs + " records. Build them into a JSON payload");
        if (!comData.ZERROR) {
            ans = OBHandler(recArr);
        } else {
            ans = comData.Zmessage;
        }

        while (ans.contains("__")) ans = ans.replace("__", "$");
        while (ans.contains("~~")) ans = ans.replace("~~", "#");

        comData.DataList.clear();
        comData.DataList.add(ans.trim());
        return ans;
    }

    public String GetPayees(nsCommonData comdata) throws IOException {
        comData = comdata;
        String ans = "<<PASS>>";
        Reset();
        needsBlank = false;

        String dsd = BuildDSD(comData);
        if (comData.ZERROR) {
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            return "<<FAIL>>";
        }
        String err = "";
        iid = comData.nsMsgGet("customer");
        if (iid.equals("")) iid = comData.nsMsgGet("item");
        String subr = "SR.GETPAYEES";
        callArgs = ClearArgs(callArgs);
        callArgs[0] = err;
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";
        logger.logthis(comData.logHeader + "   .) Calling " + subr);
        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        if (!callArgs[0].equals("")) {
            comData.ZERROR = true;
            comData.Zmessage = callArgs[0];
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            return "<<FAIL>>";
        }
        String payload = callArgs[4];
        payload = CleansePayload(payload);
        String[] recArr = payload.split(FMark);
        totRecs = recArr.length;

        CheckResults(dsd, recArr);

        logger.logthis(comData.logHeader + "   .) GetPayees DB fetch complete. Build JSON payload");

        String jsnText="";
        if (!comData.ZERROR) {
            jsnText = OBHandler(recArr);
            if (comData.ZERROR) ans="<<FAIL>>";
        } else {
            jsnText = comData.Zmessage;
        }

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        comData.DataList.clear();
        comData.DataList.add(jsnText.trim());
        return ans;
    }

    public String GetPayments(nsCommonData comdata) throws IOException {
        comData = comdata;
        String ans = "<<PASS>>";
        Reset();
        needsBlank = false;

        String dsd = BuildDSD(comData);
        if (comData.ZERROR) {
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData = null;
            return "<<FAIL>>";
        }
        String err = "";
        iid = comData.nsMsgGet("customer");
        if (iid.equals("")) iid = comData.nsMsgGet("item");
        String subr = "SR.GETPAYMENTS";
        callArgs = ClearArgs(callArgs);
        callArgs[0] = err;
        callArgs[1] = dsd;
        callArgs[2] = iid;
        callArgs[3] = iid;
        callArgs[4] = "";
        logger.logthis(comData.logHeader + "   .) Calling " + subr);
        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        if (!callArgs[0].equals("")) {
            comData.ZERROR = true;
            comData.Zmessage = callArgs[0] + " when calling " + subr;
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            return "<<FAIL>>";
        }
        String payload = callArgs[4];
        payload = CleansePayload(payload);
        String[] recArr = payload.split(FMark);
        String[] chkArr = recArr[0].split(sep);
        totRecs = recArr.length;

        CheckResults(dsd, recArr);

        logger.logthis(comData.logHeader + "   .) GetPayments DB fetch complete. Build JSON payload");

        String jsnText;
        if (!comData.ZERROR) {
            jsnText = OBHandler(recArr);
            if (comData.ZERROR) ans = jsnText;
        } else {
            jsnText = comData.Zmessage;
        }

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        comData.DataList.clear();
        comData.DataList.add(jsnText.trim());
        return ans;
    }

    public String GetTransactions(nsCommonData comdata) throws IOException {
        comData = comdata;
        String ans = "<<PASS>>", trxLimit = "366", tmpVal="";
        tmpVal = comData.nsMapGet("limit"); if (!tmpVal.equals("")) trxLimit = tmpVal;
        tmpVal = comData.nsMsgGet("limit"); if (!tmpVal.equals("")) trxLimit = tmpVal;
        Reset();

        try {
            nbrDays = Integer.valueOf(trxLimit);
        } catch (NumberFormatException nfe){
            logger.logthis(comData.logHeader + "**********************************************************************");
            logger.logthis(comData.logHeader + "   .) Tran limit ERROR: ["+comData.nsMapGet("limit")+"] is not an INTEGER. Using 366");
            logger.logthis(comData.logHeader + "**********************************************************************");
            comData.nbrDays = 366;
        }

        GetHashVars();

        String dsd = BuildDSD(comData);
        if (comData.ZERROR) {
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData = null;
            return "<<FAIL>>";
        }
        iid = comData.nsMsgGet("customer");
        if (iid.equals("")) iid = comData.nsMsgGet("item");

        String payload = "", totRecs = "";
        String subr = "SR.READTRANS";
        logger.logthis(comData.logHeader + "   .) --------------------------------------------------------- ");
        callArgs = ClearArgs(callArgs);
        callArgs[1] = dsd;
        callArgs[2] = lowdate;
        callArgs[3] = iid;
        callArgs[4] = iid;
        callArgs[5] = "";
        logger.logthis(comData.logHeader + "   .) Calling " + subr);
        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        if (!callArgs[0].equals("")) {
            comData.ZERROR = true;
            comData.Zmessage = callArgs[0];
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData = null;
            return "<<FAIL";
        }
        totRecs = callArgs[5];
        logger.logthis(comData.logHeader + "   .) Built the dataset with " + totRecs + " transactions.");

        callArgs = ClearArgs(callArgs);
        subr = "SR.GETTRANS";
        callArgs[1] = comData.nsMsgGet("page");
        callArgs[2] = comData.nsMsgGet("page-size");
        callArgs[3] = iid;
        callArgs[4] = "";
        logger.logthis(comData.logHeader + "   .) Calling " + subr + " for page # " + page);
        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        if (!callArgs[0].equals("")) {
            comData.ZERROR = true;
            comData.Zmessage = callArgs[0];
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData.DataList.clear();
            comData.DataList.add(comData.Zmessage.trim());
            comData = null;
            return "<<FAIL";
        }
        payload = callArgs[4];
        payload = CleansePayload(payload);
        String[] recArr = payload.split(FMark);
        int eol = recArr.length;
        logger.logthis(comData.logHeader + "   .) " + eol + " transactions returned. In a page-size of " + pgSize);

        String[] chkArr = recArr[0].split(sep);
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
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        String[] lParts;
        String key, val, tmp1, tmp2;
        int fnd, eoi = jField.size();

        JSONArray jdatArr = new JSONArray();
        JSONObject arrElement;

        logger.logthis(comData.logHeader + "   .) Build transaction array");
        for (int i = 0; i < eol; i++) {
            lParts = (recArr[i] + sep + blank).split(sep);
            arrElement = new JSONObject();
            for (int c = 0; c < eoi; c++) {
                key = jField.get(c);
                val = jValue.get(c);
                while (val.contains("$")) {
                    tmp1 = FieldOf(val, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = comData.SubsList.indexOf(tmp1);
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

        logger.logthis(comData.logHeader + "   .) load transaction array");
        jData.put("transactions", jdatArr);
        logger.logthis(comData.logHeader + "   .) load data section");
        jMaster.put("data", jData);
        logger.logthis(comData.logHeader + "   .) load links section");
        jMaster.put("links", jLinks);
        logger.logthis(comData.logHeader + "   .) load meta section");
        jMaster.put("meta", jMeta);
        logger.logthis(comData.logHeader + "   .) load Master");
        comData.DataList.clear();
        comData.DataList.add(jMaster.toString());
        return ans;
    }

    public String GetTransactionsV2(nsCommonData comdata) throws IOException {
        comData = comdata;
        String ans = "<<PASS>>";
        logger.logthis(comData.logHeader + "  .) ");

        Reset();
        GetHashVars();

        if (!comData.nsMapGet("limit").equals("")) {
            try {
                nbrDays = Integer.valueOf(comData.nsMapGet("limit"));
            } catch (NumberFormatException nfe){
                logger.logthis(comData.logHeader + "**********************************************************************");
                logger.logthis(comData.logHeader + "   .) Tran V2 ERROR: ["+comData.nsMapGet("limit")+"] not an INTEGER. Using 366");
                logger.logthis(comData.logHeader + "**********************************************************************");
                nbrDays = 366;
            }
        }
        needsBlank = false;

        String dsd = BuildDSD(comData);
        if (comData.ZERROR) {
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData = null;
            return "<<FAIL>>";
        }
        String correl = comData.nsMsgGet("correlationid");
        if (correl.equals("")) correl = String.valueOf(UUID.randomUUID());
        if (correl.endsWith("-Debug")) correl += "-" + String.valueOf(UUID.randomUUID());

        dsdControl = dsd.split(FMark).length;
        iid = comData.nsMsgGet("customer");
        if (iid.equals("")) iid = comData.nsMsgGet("item");
        String subr = "SR.READTRANSV2";

        // --------------------------------------------------------------------------------
        // V2 uses the CDR.WORKFILE to cache pages of transaction in the database
        //    also fixes narratives which are broken into n lines
        //    also adds biller names and biller codes to narratives
        //    also links NPP in/out data in with transactions
        //    then takes the fixed CDR.WORKFILE items and stores in &SAVEDLISTS& as a cache
        //    the cache is removed by CDR.TIDYUP
        // --------------------------------------------------------------------------------

        callArgs = ClearArgs(callArgs);
        callArgs[1] = dsd;
        callArgs[2] = lowdate;
        callArgs[3] = iid;
        callArgs[4] = correl;
        callArgs[5] = "";
        logger.logthis(comData.logHeader + "   .) Calling " + subr);
        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        if (!callArgs[0].equals("")) {
            comData.ZERROR = true;
            comData.Zmessage = callArgs[0];
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData = null;
            return "<<FAIL";
        }

        totRecs = Integer.valueOf(callArgs[5]);
        GetHashVars();      // yes, do it again !!

        // ------------------------------------------------------------------------------------
        // At this point, all the transactions are in &SAVEDLISTS& and the page can be returned
        // ------------------------------------------------------------------------------------

        callArgs = ClearArgs(callArgs);
        subr = "SR.GETTRANS";
        callArgs[1] = page;
        callArgs[2] = pgSize;
        callArgs[3] = correl;
        callArgs[4] = "";
        logger.logthis(comData.logHeader + "   .) Calling " + subr + " for page # " + page);
        callArgs = CallSub(subr, callArgs);
        if (comData.ZERROR) {
            comData = null;
            return "<<FAIL>>";
        }

        if (!callArgs[0].equals("")) {
            comData.ZERROR = true;
            comData.Zmessage = callArgs[0];
            logger.logthis(comData.logHeader + "ERROR: " + comData.Zmessage);
            comData = null;
            return "<<FAIL>>";
        }
        String payload = CleansePayload(callArgs[4]);
        String[] recArr = payload.split(FMark);
        totRecs = recArr.length;
        logger.logthis(comData.logHeader + "   .) " + totRecs + " transactions returned on a page-size of " + pgSize);

        callArgs = ClearArgs(callArgs);
        CheckResults(dsd, recArr);

        String jsnText = OBHandlerV2(recArr);       // PERFECT for simple one only import template !!
        if (comData.ZERROR) ans = jsnText;          // ?????

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        // ------- cleanup database cache of transactions IF there was a recent CDR request ----------------
//        boolean purgeCach = (APImsg.APIget("purge-cache").toLowerCase().equals("true"));
//        boolean purgeCache = comData.nsMsgGet("purge-cache").toLowerCase().equals("true");
        noCache = comData.nsMsgGet("purge-cache").toLowerCase().equals("true");
        if (noCache) {
            // BaaS request: do NOT cache in &SavedLists&
            subr  = "SR.TIDYUP";
            String killCache = subr + "(" + correl + ")";
            logger.logthis("    .) " + killCache);
            callArgs = ClearArgs(callArgs);
            callArgs[1] = correl;
            callArgs = CallSub(subr, callArgs);
        }
        // -------------------------------------------------------------------------------------------------

        comData.DataList.clear();
        comData.DataList.add(jsnText.trim());
        logger.logthis(comData.logHeader + "   .) Done.");
        return ans;
    }

    private String FieldOf(String inVal, String delim, int element) {
        String ans = "";
        String[] parts = inVal.split(delim);
        if (parts.length >= element) {
            ans = parts[element-1];
        }
        return ans;
    }

    private String[] ClearArgs(String[] inArgs) {
        for (int i = 0; i < inArgs.length; i++) { inArgs[i] = "@@"; }
        inArgs[0] = "";
        return inArgs;
    }

    public String[] LoadProgram(nsCommonData comdata, String subr, String[] callArgs) {
        comData = comdata;
        return CallSub(subr, callArgs);
    }

    public String[] CallSub(String subr, String[] callArgs) {
        String[] outArgs = new String[callArgs.length];
        outArgs[0] = "";
        String dbgCallString="CALL "+subr+" (";

        if (!comData.DBconnected) {
            comData.ConnectProxy();
            if (comData.ZERROR) return null;
        }

        RSC sub = new RSC(comData.rcon, subr);
        try {
            int eoi = callArgs.length;
            String junk="";
            for (int c=0 ; c < eoi ; c++) {
                junk = callArgs[c];
                if (junk.equals("@@")) break;
                sub.getParam(c+1).setValue(junk);
                dbgCallString += "\""+callArgs[c]+"\",";
            }
            dbgCallString = dbgCallString.substring(0, dbgCallString.length()-1)  + ")";
            sub.execute();
            for (int c=0 ; c < eoi ; c++) { outArgs[c] = sub.getParam(c+1).getString(); }
        } catch (RSCException e) {
            comData.Zmessage = e.toString().replaceAll("\\r?\\n", "");
            comData.ZERROR = true;
            logger.logthis(" ");
            logger.logthis("ERROR: " + comData.Zmessage);
            logger.logthis("       " + dbgCallString);
            logger.logthis(" ");
        }
//        logger.logthis("       " + dbgCallString);
        dbgCallString = "";
        sub = null;
        callArgs = null;
        return outArgs;
    }

    private void CheckComponents(String dsd, String[] chkArr) {
        if (comData.SubsList.size() != chkArr.length) {
            // This happens when we have items in the dsd that are not used in the templates.
            comData.Zmessage = "ERROR: mismatch between substitution strings and returned data string.";
            logger.logthis(comData.logHeader + "   ). *****");
            logger.logthis(comData.logHeader + "   ). "+comData.Zmessage);
            logger.logthis(comData.logHeader + "   ). *****");
        }
    }
    
    private String OBHandlerV2(String[] recArr) throws IOException {
        if (comData.SubsList.get(0).equals("")) comData.SubsList.remove(0);
        logger.logthis(comData.logHeader + "   .) GetHashVars");
        GetHashVars();
        logger.logthis(comData.logHeader + "   .) CreateSubsArrays");
        CreateSubsArrays();
        if (comData.ZERROR) return "<<FAIL>>";
        impTmpls.clear();
        impLines.clear();
        logger.logthis(comData.logHeader + "   .) GetTemplate");
        String jsnText = GetTemplate(comData.nsMapGet("template"));
        if (comData.ZERROR) return "<<FAIL>>";

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
                        logger.logthis(comData.logHeader + "   .) load the [" + zkey + "].[" + ikey + "] array");
                        akey = ikey;
                        innerObj = SubsArrayHandler(akey, recArr);
                        oMaster.put(zkey, innerObj);
                    } else {
                        logger.logthis(comData.logHeader + "   .) load " + zkey + " section");
                        zval = SubsHashVars(zval);
                        innerObj = new JSONObject(zval);
                        oMaster.put(zkey, innerObj);
                    }
                } else {
                    logger.logthis(comData.logHeader + "   .) load " + zkey + " section");
                    zval = SubsHashVars(zval);
                    oMaster.put(zkey, zval);
                }
                innerObj = null;
            }
        } catch (JSONException je) {
            comData.ZERROR = true;
            comData.Zmessage = "ERROR: in JSON " + je.getMessage();
            logger.logthis(comData.logHeader + je.getMessage());
            return "<<FAIL>>";
        }

        logger.logthis(comData.logHeader + "   .) Stringify JSONObject.");
        jsnText = oMaster.toString();
        jsnText = jsnText.replaceAll("\\r?\\n", "");
        return jsnText;
    }

    private JSONObject SubsArrayHandler(String ikey, String[] recArr) {
        JSONObject jObject = new JSONObject();
        JSONArray jArray = new JSONArray();
        JSONObject arrElement = null;

        String key, val, tmp1, tmp2;
        String[] lParts;

        int nbrRecs = recArr.length, eoi = jField.size(), fnd, lpLen;

        for (int r = 0; r < nbrRecs; r++) {
            lParts = (recArr[r] + sep + blank).split(sep);
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
                    tmp1 = FieldOf(val, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = comData.SubsList.indexOf(tmp1);
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

    public String CleansePayload(String payload) {
        payload = payload.replaceAll("\\$", "__");
        payload = payload.replaceAll("\\#", "~~");
        // will probably need to jParse because of json chars in the data.
        return payload;
    }

    private String OBHandler(String[] recArr) throws IOException {
        GetHashVars();
//        CreateSubsArrays();     // not sure this is needed - jField & jValue arrays
        impTmpls.clear();
        impTlines.clear();
        impLines.clear();
        // -----------------------------------------------------------------------
        String[] jsnMaster = GetImportTemplates(comData.nsMapGet("template"));       // Add imports to impTemp list
        String jsnText = "", masterText;
        masterText = DoHashVarSubs(jsnMaster, recArr);
        jsnMaster = masterText.split("\\r?\\n");
        LoadAllTemplates();
        // -----------------------------------------------------------------------
        if (comData.ZERROR) return "<<FAIL>>";
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

    private void LoadAllTemplates() throws IOException {
        ArrayList<String> tmpDone = new ArrayList<>();
        tmpDone.add(comData.nsMapGet("template"));
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

    private String StitchTogether(String jsnText) {

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
                            prePos = Integer.valueOf(FieldOf(tmplName, "~", 2));
                            tmplName = FieldOf(tmplName, "~", 1);
                        }

                        posx = impTmpls.indexOf(tmplName);
                        if (posx < 0) continue;

                        lineParts = impLines.get(posx).split(FMark);
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
                                        saveLine = FieldOf(saveLine, "~", 1) + "~" + (recPos + 1) + "~";
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

    private String StitchTogetherV2(String jsnText) {

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
                    templateLine = importLine.replace(FMark, "");
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

    private void DollarVarsImportTemplates(String[] dList) {

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
            lParts = (dList[r] + sep + blank).split(sep);
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
                            tmp1 = FieldOf(templateLine, "\\$", 2);
                            tmp1 = "$" + tmp1 + "$";
                            fnd = comData.SubsList.indexOf(tmp1);
                            if (fnd >= 0) {
                                if (fnd <= eol) {
                                    tmp2 = lParts[fnd];
                                } else {
                                    tmp2 = "";
                                }
                                if (tmp2.equals(blank)) tmp2 = "";
                                if (!redo && tmp2.contains(VMark)) redo = true;
                                chkArr = tmp2.split(VMark);
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

                if (!impLines.get(i).equals("")) jsnTemp = impLines.get(i) + FMark + jsnTemp;
                impLines.set(i, jsnTemp);
                jsnTemp = "";
            }
        }
    }

    private String DoHashVarSubs(String[] jsnLines, String[] lParts) {

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
                    tmp1 = FieldOf(templateLine, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = comData.SubsList.indexOf(tmp1);
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

    private String GetTemplate(String template) throws IOException {
        String tPath = comData.BaseCamp + "/", fqfn = "";
        fqfn = tPath + comData.nsMapGet("template");
        return ReadFromDisk(fqfn);
    }

    private String[] GetImportTemplates(String template) throws IOException {
        String tPath = comData.BaseCamp + "/", fqfn = "";
        fqfn = tPath + template;
        logger.logthis(comData.logHeader + "   ). Prepare template: "+fqfn);
        String jsnText = ReadFromDisk(fqfn);
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
                logger.logthis(comData.logHeader + "      ). Import template: "+templateLine);
                impTmpls.add(templateLine);
                impTlines.add("");
                impLines.add("");
                jsnText = "";
            }
        }
        if (totRecs > 1 && impTmpls.size() == 0) {
            comData.ZERROR = true;
            comData.Zmessage = "ERORR: more than one DB record in action list and only one output template.";
            logger.logthis(comData.logHeader + comData.Zmessage);
            comData.DataList.clear();
            comData.DataList.add("");
        }
        return jsnLines;
    }

    private void GetHashVars() {

        page = comData.nsMsgGet("page");
        pgSize = comData.nsMsgGet("page-size");

        try {
            pg = Integer.valueOf(page);
        } catch (NumberFormatException nfe) {
            pg = 1;
        }
        try {
            pgSz = Integer.valueOf(pgSize);
        } catch (NumberFormatException nfe) {
            pgSz = 1000;
        }

        if (pg < 1) pg = 1;
        if (pgSz < 10) pgSz = 10;
        page = String.valueOf(pg);
        pgSize = String.valueOf(pgSz);
        double nbrPgs;
        try {
            nbrPgs = totRecs / pgSz;
            lastPage = (int) nbrPgs;
            if ((nbrPgs * pgSz) != totRecs) lastPage++;
        } catch (NumberFormatException nfe) {
            totRecs = 0;
            lastPage = 1;
        }
        if (lastPage == 0) lastPage = 1;
        if (pg > lastPage) {
            pg = lastPage;
            page = String.valueOf(pg);
        }
        nextPage = pg + 1;
        prevPage = pg - 1;
        if (prevPage < 1) prevPage = 1;
        if (nextPage > lastPage) nextPage = lastPage;
    }

    private String SubsHashVars(String inLine) {
        String tmp1 = "";
        while (inLine.contains("#")) {
            tmp1 = FieldOf(inLine, "\\#", 2);
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
//                    logger.logthis(comData.logHeader + tmp1+" is an invalid hash variable.");
//                    return inLine;
            }
            tmp1 = "";
        }
        return inLine;
    }

    private void CreateSubsArrays() throws IOException {
        String jsnText, templateLine = "";
        String[] jsnLines, lineParts;
        String tPath = comData.BaseCamp + "/", fqfn = "";

        jField.clear();
        jValue.clear();
        for (int t = 0; t < comData.Templates.size(); t++) {
            fqfn = tPath + comData.Templates.get(t);
            jsnText = ReadFromDisk(fqfn);
            if (comData.ZERROR) return;

            jsnLines = jsnText.split("\\r?\\n");
            templateLine = "";

            for (int i = 0; i < jsnLines.length; i++) {
                templateLine = jsnLines[i].trim();
                while (templateLine.contains("\"")) { templateLine = templateLine.replace("\"", ""); }
                while (templateLine.contains(",")) { templateLine = templateLine.replace(",", ""); }
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

    private String BuildDSD(nsCommonData comData) {
        comData.Templates.clear();
        comData.TmplList.clear();
        comData.DataList.clear();

        comData.SubsList.clear();
        if (needsBlank) comData.SubsList.add("");

        String csvName, content = "", line, dsd = "", cma = ",", tmpl = "", subs = "", chk="";
        String[] csvList = comData.nsMapGet("LIST").split(",");
        int nbrCsvItems = csvList.length;

        for (int i = 0; i < nbrCsvItems; i += 1) {
            logger.logthis(comData.logHeader + "  .) BuildDSD(" + csvList[i] + ")");
            csvName = comData.BaseCamp + comData.slash + csvList[i];
            ArrayList<String> csvLines = null;

            try {
                content = new String(Files.readAllBytes(Paths.get(csvName)));
                if (content.startsWith("ENC(")) content = uCipher.Decrypt(content);
            } catch (IOException e) {
                comData.ZERROR = true;
                comData.Zmessage = e.getMessage();
                comData.uSendMessage("ERROR: " + comData.Zmessage);
            }
            csvLines = new ArrayList<>(Arrays.asList(content.split("\\r?\\n")));
            int eol = csvLines.size();
            for (int j = 0; j < eol; j++) {
                line = csvLines.get(j) + cma + cma + cma + cma + cma + cma + cma + cma + cma + cma + blank;

                if (line.trim().equals("") || line.trim().startsWith("#")) continue;

                String[] lParts = line.split("\\,");

                dsd += lParts[0] + cma + lParts[1] + cma + lParts[2] + cma + lParts[3] + cma + lParts[4] + FMark;
                chk += lParts[0] + cma + lParts[1] + cma + lParts[2] + cma + lParts[3] + cma + lParts[4] + cma + lParts[6] + FMark;

                subs = lParts[6];
                if (lParts.length < 8 || subs.equals("")) {
                    tmpl = comData.nsMapGet("template");
                } else {
                    tmpl = lParts[7];
                }
                if (tmpl.equals("")) tmpl = comData.nsMapGet("template");
                if (!subs.equals("")) {
                    if (comData.SubsList.indexOf(subs) < 0) comData.SubsList.add(subs);
                    if (comData.TmplList.indexOf(tmpl) < 0) comData.TmplList.add(tmpl);
                    if (comData.Templates.indexOf(tmpl) < 0) {
                        comData.Templates.add(tmpl);
                        String tPath = comData.BaseCamp + comData.slash, fqfn = "";
                        fqfn = tPath + tmpl;
                        String junk = ReadFromDisk(fqfn);
                        if (comData.ZERROR) return "";
                        logger.logthis(comData.logHeader + "     .) Load: " + tmpl);
                    }
                }
            }
        }
        return dsd;
    }

    private void CheckResults(String dsd, String[] recArr) {

        if (!needsBlank) return;

        String[] chkArr = (recArr[0]+sep+blank).split(sep);
        int chk1 = comData.SubsList.size();
        int chk2 = chkArr.length;
        if (chk1 != chk2) {
            comData.Zmessage = "ERROR: mismatch between dsd and returned data string.";
            logger.logthis(comData.logHeader + "   ). *****");
            logger.logthis(comData.logHeader + "   ). "+comData.Zmessage);
            logger.logthis(comData.logHeader + "   ). SubsList() found "+comData.SubsList.size()+" items but rFuel returned "+chkArr.length+" items.");
            logger.logthis(comData.logHeader + "   ). Results should not be trusted.");
            logger.logthis(comData.logHeader + "   ). *****");
        }
    }

    private void Reset() {
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
        LocalDate now = LocalDate.now();
        int today = (int) DAYS.between(Day0, now);
        lowdate = String.valueOf(Integer.valueOf(today) - nbrDays);
    }

    public String ReadFromDisk(String fqn) {
        BufferedReader BRin = null;
        FileReader fr = null;
        String rec = "";
        try {
            fr = new FileReader(fqn);
            BRin = new BufferedReader(fr);
            String line = null;
            line = BRin.readLine();
            while ((line) != null) {
                rec = rec + line + "\n";
                line = BRin.readLine();
            }
            BRin.close();
            BRin = null;
            fr.close();
            fr = null;
        } catch (FileNotFoundException e) {
            comData.ZERROR = true;
            comData.Zmessage = e.getMessage();
//            commons.Zmessage = e.getMessage();
            comData.uSendMessage("ERROR: " + fqn + " not found");
        } catch (IOException e) {
            commons.ZERROR = true;
            commons.Zmessage = e.getMessage();
            commons.Zmessage = e.getMessage();
            comData.uSendMessage("ERROR: " + commons.Zmessage);
        }
        return rec;
    }
    
}
