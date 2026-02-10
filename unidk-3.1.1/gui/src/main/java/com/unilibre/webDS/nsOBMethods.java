package com.unilibre.webDS;

import com.northgateis.reality.rsc.RSC;
import com.northgateis.reality.rsc.RSCException;
import com.unilibre.cipher.uCipher;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static com.unilibre.webDS.Constants.*;
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

    private final Map<String,String> jsonMap = new HashMap<>();
    private boolean noCache = false;
    private final ArrayList<String> impTmpls = new ArrayList<>();
    private final ArrayList<String> impTlines = new ArrayList<>();
    private final ArrayList<String> impLines = new ArrayList<>();
    private int pg, pgSz, prevPage, nextPage, lastPage, totRecs;
    private int nbrDays = 366;
    private String page;
    private String pgSize;
    private String lowdate;
    private String iid;

    public String GetCustomerID(nsCommonData comData) throws IOException {
        Reset();
        String dsd = BuildDSD(false, comData);
        List<String> output = CallStandardRfuelSub(dsd, comData, "SR.GETCUSTOMERID");

        String payload = output.get(4);
        payload = CleansePayload(payload);
        String[] recArr = payload.split(fMark);
        totRecs = recArr.length;

        CheckResults(false, comData, recArr);

        logger.logthis(comData.logHeader + "   .) GetCustomerId DB fetch complete. Build JSON payload");

        GetHashVars(comData);

        String[] lParts = (payload + tMark + blank).split(tMark);
        String tmp1, tmp2;
        int fnd, eoi;
        String[] jsnLines;
        String tPath = commons.BaseCamp + slash;
        String fqfn = tPath + comData.nsMapGet("template");
        StringBuilder jsnText = new StringBuilder(ReadFromDisk(fqfn));
        String templateLine;
        jsnLines = jsnText.toString().split("\\r?\\n");
        eoi = jsnLines.length;
        jsnText = new StringBuilder();
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
            jsnText.append(templateLine);
        }

        while (jsnText.toString().contains("__")) jsnText = new StringBuilder(jsnText.toString().replace("__", "$"));
        while (jsnText.toString().contains("~~")) jsnText = new StringBuilder(jsnText.toString().replace("~~", "#"));

        return jsnText.toString().trim();
    }

    public String GetCustomer(nsCommonData comData) throws IOException {
        Reset();

        String dsd = BuildDSD(true, comData);
        List<String> callArgs = CallStandardRfuelSub(dsd, comData, "SR.GETCUSTOMER");

        String payload = callArgs.get(4);
        payload = CleansePayload(payload);
        String[] recArr = payload.split(fMark);
        totRecs = recArr.length;

        CheckResults(true, comData, recArr);

        logger.logthis(comData.logHeader + "   .) GetCustomer DB fetch complete. Build JSON payload");

        GetHashVars(comData);

        StringBuilder jsnText;
        String templateLine;
        String[] lParts = (payload + tMark + blank).split(tMark);
        String tmp1, tmp2;
        int fnd, eoi;
        String[] jsnLines;
        String tPath = commons.BaseCamp + "/", fqfn;
        fqfn = tPath + comData.nsMapGet("template");
        jsnText = new StringBuilder(ReadFromDisk(fqfn));
        jsnLines = jsnText.toString().split("\\r?\\n");
        eoi = jsnLines.length;
        jsnText = new StringBuilder();
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
            jsnText.append(templateLine);
        }

        while (jsnText.toString().contains("__")) jsnText = new StringBuilder(jsnText.toString().replace("__", "$"));
        while (jsnText.toString().contains("~~")) jsnText = new StringBuilder(jsnText.toString().replace("~~", "#"));

        comData.DataList.clear();
        return jsnText.toString().trim();
    }

    public String GetAccounts(nsCommonData comData) throws IOException {
        Reset();

        String dsd = BuildDSD(true, comData);
        List<String> output = CallStandardRfuelSub(dsd, comData, "SR.GETACCOUNTS");
        String payload = output.get(4);
        payload = CleansePayload(payload);
        String[] recArr = payload.split(fMark);
        String[] chkArr = recArr[0].split(tMark);
        totRecs = recArr.length;
        CheckComponents(comData, chkArr);

        logger.logthis(comData.logHeader + "   .) GetAccounts fetched " + totRecs + " records. Build them into a JSON payload");
        String ans = OBHandler(comData, recArr);

        while (ans.contains("__")) ans = ans.replace("__", "$");
        while (ans.contains("~~")) ans = ans.replace("~~", "#");

        comData.DataList.clear();
        comData.DataList.add(ans.trim());
        return ans;
    }

    public String GetPayees(nsCommonData comData) throws IOException {
        Reset();

        String dsd = BuildDSD(false, comData);
        List<String> output = CallStandardRfuelSub(dsd, comData, "SR.GETPAYEES");
        String payload = output.get(4);
        payload = CleansePayload(payload);
        String[] recArr = payload.split(fMark);
        totRecs = recArr.length;

        CheckResults(false, comData, recArr);

        logger.logthis(comData.logHeader + "   .) GetPayees DB fetch complete. Build JSON payload");

        String jsnText = OBHandler(comData, recArr);

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        return jsnText.trim();
    }

    public String GetPayments(nsCommonData comData) throws IOException {
        Reset();

        String dsd = BuildDSD(false, comData);
        List<String> output = CallStandardRfuelSub(dsd, comData, "SR.GETPAYMENTS");
        String payload = output.get(4);
        payload = CleansePayload(payload);
        String[] recArr = payload.split(fMark);
        totRecs = recArr.length;

        CheckResults(false, comData, recArr);

        logger.logthis(comData.logHeader + "   .) GetPayments DB fetch complete. Build JSON payload");

        String jsnText = OBHandler(comData, recArr);

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        return jsnText.trim();
    }

    public String GetTransactions(nsCommonData comData) throws IOException {
        String trxLimit = "366", tmpVal;
        tmpVal = comData.nsMapGet("limit"); if (!tmpVal.equals("")) trxLimit = tmpVal;
        tmpVal = comData.nsMsgGet("limit"); if (!tmpVal.equals("")) trxLimit = tmpVal;
        Reset();

        try {
            nbrDays = Integer.parseInt(trxLimit);
        } catch (NumberFormatException nfe){
            logger.logthis(comData.logHeader + "**********************************************************************");
            logger.logthis(comData.logHeader + "   .) Tran limit ERROR: ["+comData.nsMapGet("limit")+"] is not an INTEGER. Using 366");
            logger.logthis(comData.logHeader + "**********************************************************************");
            nbrDays = 366;
        }

        GetHashVars(comData);

        String dsd = BuildDSD(true, comData);
        extractItemId(comData);
        List<String> readTransOutput = CallStandardRfuelSub(dsd, comData, "SR.READTRANS");
        String totRecs = readTransOutput.get(5);
        logger.logthis(comData.logHeader + "   .) Built the dataset with " + totRecs + " transactions.");

        String subr = "SR.GETTRANS";
        List<String> getTransArgs = new LinkedList<>();
        getTransArgs.add("");
        getTransArgs.add(comData.nsMsgGet("page"));
        getTransArgs.add(comData.nsMsgGet("page-size"));
        getTransArgs.add(iid);
        getTransArgs.add("");
        logger.logthis(comData.logHeader + "   .) Calling " + subr + " for page # " + page);
        List<String> getTransOutput = CallSub(comData, subr, getTransArgs);

        if (!getTransOutput.get(0).equals("")) {
            String message = getTransOutput.get(0);
            logger.logthis(comData.logHeader + "ERROR: " + message);
            throw new RFuelException(500, message);
        }
        String payload = getTransOutput.get(4);
        payload = CleansePayload(payload);
        String[] recArr = payload.split(fMark);
        int eol = recArr.length;
        logger.logthis(comData.logHeader + "   .) " + eol + " transactions returned. In a page-size of " + pgSize);

        String[] chkArr = recArr[0].split(tMark);
        CheckComponents(comData, chkArr);

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

        CreateSubsArrays(comData);

        String[] lParts;
        String key, val, tmp1, tmp2;

        JSONArray jdatArr = new JSONArray();
        JSONObject arrElement;

        logger.logthis(comData.logHeader + "   .) Build transaction array");
        for (String s : recArr) {
            lParts = (s + tMark + blank).split(tMark);
            arrElement = new JSONObject();
            for (Map.Entry<String, String> entry : jsonMap.entrySet()) {
                key = entry.getKey();
                val = entry.getValue();

                while (entry.getValue().contains("$")) {
                    tmp1 = FieldOf(entry.getValue(), "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    int fnd = comData.SubsList.indexOf(tmp1);
                    if (fnd >= 0) {
                        tmp2 = lParts[fnd];
                        if (tmp2.equals(blank))
                            tmp2 = "";
                        val = val.replace(tmp1, tmp2);
                    } else {
                        val = "unprocessed-data-element-" + tmp1;
                        break;
                    }
                }
                arrElement.put(key, val);
            }
            jdatArr.put(arrElement);
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

        return jMaster.toString();
    }

    public String GetTransactionsV2(nsCommonData comData) throws IOException {
        logger.logthis(comData.logHeader + "  .) ");

        Reset();
        GetHashVars(comData);

        if (!comData.nsMapGet("limit").equals("")) {
            try {
                nbrDays = Integer.parseInt(comData.nsMapGet("limit"));
            } catch (NumberFormatException nfe){
                logger.logthis(comData.logHeader + "**********************************************************************");
                logger.logthis(comData.logHeader + "   .) Tran V2 ERROR: ["+comData.nsMapGet("limit")+"] not an INTEGER. Using 366");
                logger.logthis(comData.logHeader + "**********************************************************************");
                nbrDays = 366;
            }
        }

        String dsd = BuildDSD(false, comData);
        extractItemId(comData);
        String correl = comData.nsMsgGet("correlationid");
        if (correl.equals("")) correl = String.valueOf(UUID.randomUUID());
        if (correl.endsWith("-Debug")) correl += "-" + UUID.randomUUID();

        String subr = "SR.READTRANSV2";

        // --------------------------------------------------------------------------------
        // V2 uses the CDR.WORKFILE to cache pages of transaction in the database
        //    also fixes narratives which are broken into n lines
        //    also adds biller names and biller codes to narratives
        //    also links NPP in/out data in with transactions
        //    then takes the fixed CDR.WORKFILE items and stores in &SAVEDLISTS& as a cache
        //    the cache is removed by CDR.TIDYUP
        // --------------------------------------------------------------------------------

        List<String> readTransV2Args = new LinkedList<>();
        readTransV2Args.add("");
        readTransV2Args.add(dsd);
        readTransV2Args.add(lowdate);
        readTransV2Args.add(iid);
        readTransV2Args.add(correl);
        readTransV2Args.add("");
        logger.logthis(comData.logHeader + "   .) Calling " + subr);

        List<String> readTransV2Output = CallSub(comData, subr, readTransV2Args);

        if (!readTransV2Output.get(0).equals("")) {
            String message = readTransV2Output.get(0);
            logger.logthis(comData.logHeader + "ERROR: " + message);
            throw new RFuelException(500, message);
        }

        totRecs = Integer.parseInt(readTransV2Output.get(5));
        GetHashVars(comData);      // yes, do it again !!

        // ------------------------------------------------------------------------------------
        // At this point, all the transactions are in &SAVEDLISTS& and the page can be returned
        // ------------------------------------------------------------------------------------

        List<String> getTransArgs = new LinkedList<>();
        subr = "SR.GETTRANS";
        getTransArgs.add("");
        getTransArgs.add(page);
        getTransArgs.add(pgSize);
        getTransArgs.add(correl);
        getTransArgs.add("");
        logger.logthis(comData.logHeader + "   .) Calling " + subr + " for page # " + page);
        List<String> getTransOutput = CallSub(comData, subr, getTransArgs);

        if (!getTransOutput.get(0).equals("")) {
            String message = getTransOutput.get(0);
            logger.logthis(comData.logHeader + "ERROR: " + message);
            throw new RFuelException(500, message);
        }
        String payload = CleansePayload(getTransOutput.get(4));
        String[] recArr = payload.split(fMark);
        totRecs = recArr.length;
        logger.logthis(comData.logHeader + "   .) " + totRecs + " transactions returned on a page-size of " + pgSize);

        CheckResults(false, comData, recArr);

        String jsnText = OBHandlerV2(comData, recArr);       // PERFECT for simple one only import template !!

        while (jsnText.contains("__")) jsnText = jsnText.replace("__", "$");
        while (jsnText.contains("~~")) jsnText = jsnText.replace("~~", "#");

        // ------- cleanup database cache of transactions IF there was a recent CDR request ----------------
//        Boolean purgeCach = (APImsg.APIget("purge-cache").toLowerCase().equals("true"));
//        boolean purgeCache = comData.nsMsgGet("purge-cache").toLowerCase().equals("true");
        noCache = comData.nsMsgGet("purge-cache").equalsIgnoreCase("true");
        if (noCache) {
            // BaaS request: do NOT cache in &SavedLists&
            subr  = "SR.TIDYUP";
            String killCache = subr + "(" + correl + ")";
            logger.logthis("    .) " + killCache);
            List<String> tidyUpArgs = new LinkedList<>();
            tidyUpArgs.add("");
            tidyUpArgs.add(correl);
            CallSub(comData, subr, tidyUpArgs);
        }
        // -------------------------------------------------------------------------------------------------

        logger.logthis(comData.logHeader + "   .) Done.");

        return jsnText.trim();
    }

    private String FieldOf(String inVal, String delim, int element) {
        String ans = "";
        String[] parts = inVal.split(delim);
        if (parts.length >= element) {
            ans = parts[element-1];
        }
        return ans;
    }

    public List<String> CallSub(nsCommonData comData, String subr, List<String> callArgs) throws RSCException {
        List<String> outArgs = new LinkedList<>();
        outArgs.add("");

        if (!comData.DBconnected) {
            comData.ConnectProxy();
        }

        RSC sub = new RSC(comData.rcon, subr);
        try {
            int idx = 1;
            for (String arg: callArgs) {
                sub.getParam(idx++).setValue(arg);
            }
            sub.execute();
            for (int c=1 ; c < idx ; c++) { outArgs.add(sub.getParam(c+1).getString()); }
        } catch (RSCException e) {
            String dbgCallString="CALL "+subr+" (" + callArgs.stream().map( s -> "\"" + s + "\"").collect(Collectors.joining(",")) + ")";
            String errMsg = e.toString().replaceAll("\\r?\\n", "");
            logger.logthis(" ");
            logger.logthis("ERROR: " + errMsg);
            logger.logthis("       " + dbgCallString);
            logger.logthis(" ");
            throw new RFuelException(500,errMsg);
        }
        return outArgs;
    }

    private void CheckComponents(nsCommonData comData, String[] chkArr) {
        if (comData.SubsList.size() != chkArr.length) {
            // This happens when we have items in the dsd that are not used in the templates.
            String message = "ERROR: mismatch between substitution strings and returned data string.";
            logger.logthis(comData.logHeader + "   ). *****");
            logger.logthis(comData.logHeader + "   ). "+ message);
            logger.logthis(comData.logHeader + "   ). *****");
            // TODO: This didn't set ZError???
        }
    }
    
    private String OBHandlerV2(nsCommonData comData, String[] recArr) throws IOException {
        if (comData.SubsList.get(0).equals("")) comData.SubsList.remove(0);
        logger.logthis(comData.logHeader + "   .) GetHashVars");
        GetHashVars(comData);
        logger.logthis(comData.logHeader + "   .) CreateSubsArrays");
        CreateSubsArrays(comData);
        impTmpls.clear();
        impLines.clear();
        logger.logthis(comData.logHeader + "   .) GetTemplate");
        String jsnText = GetTemplate(comData);

        JSONObject jMaster;
        JSONObject oMaster = new JSONObject();
        JSONObject innerObj;
        Iterator<String> jKeys;
        Iterator<String> innerKeys;
        String zkey, zval, ikey, akey;

        String[] tmpArr = jsnText.split("\\r?\\n");
        String tmpStr;
        StringBuilder outStr = new StringBuilder();
        for (String s : tmpArr) {
            tmpStr = s.trim();
            if (!tmpStr.startsWith("#"))
                outStr.append(tmpStr).append("\n");
        }
        jsnText = outStr.toString();
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
                        innerObj = SubsArrayHandler(comData, akey, recArr);
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
            }
        } catch (JSONException je) {
            String message = "ERROR: in JSON " + je.getMessage();
            logger.logthis(comData.logHeader + message);
            throw new RFuelException(500, message);
        }

        logger.logthis(comData.logHeader + "   .) Stringify JSONObject.");
        jsnText = oMaster.toString();
        jsnText = jsnText.replaceAll("\\r?\\n", "");
        return jsnText;
    }

    private JSONObject SubsArrayHandler(nsCommonData comData, String ikey, String[] recArr) {
        JSONObject jObject = new JSONObject();
        JSONArray jArray = new JSONArray();
        JSONObject arrElement;

        String key, val, tmp1, tmp2;
        String[] lParts;

        int fnd, lpLen;

        for (String s : recArr) {
            lParts = (s + tMark + blank).split(tMark);
            lpLen = lParts.length;
            arrElement = new JSONObject();
            // --------------------------------------------------------------------
            //  Need to read the import template and subs the variables
            //  THEN arrElement.put(lineKey, lineVal);
            //  THEN jArray.put(arrElement);
            //  THEN jObject.put(ikey, jArray);
            // --------------------------------------------------------------------
            for (Map.Entry<String, String> entry : jsonMap.entrySet()) {
                key = entry.getKey();
                val = entry.getValue();
                while (val.contains("$")) {
                    tmp1 = FieldOf(val, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = comData.SubsList.indexOf(tmp1);
                    if (fnd >= 0) {
                        if (fnd < lpLen) {
                            tmp2 = lParts[fnd];
                            if (tmp2.equals(blank))
                                tmp2 = "";
                            val = val.replace(tmp1, tmp2);
                        } else {
                            //                            val = "empty-data-element-1";
                            val = "";
                        }
                    } else {
                        val = "unmapped-data-element-" + tmp1;
                        break;
                    }
                }
                arrElement.put(key, val);
            }
            jArray.put(arrElement);
        }
        jObject.put(ikey, jArray);
        return jObject;
    }

    public String CleansePayload(String payload) {
        payload = payload.replaceAll("\\$", "__");
        payload = payload.replaceAll("#", "~~");
        // will probably need to jParse because of json chars in the data.
        return payload;
    }

    private String OBHandler(nsCommonData comData, String[] recArr) throws IOException {
        GetHashVars(comData);
//        CreateSubsArrays();     // not sure this is needed - jField & jValue arrays
        impTmpls.clear();
        impTlines.clear();
        impLines.clear();
        // -----------------------------------------------------------------------
        String[] jsnMaster = GetImportTemplates(comData, comData.nsMapGet("template"));       // Add imports to impTemp list
        String jsnText, masterText;
        masterText = DoHashVarSubs(comData, jsnMaster, recArr);
        LoadAllTemplates(comData);
        // -----------------------------------------------------------------------
        String[] chkLines = new String[1];
        int eol = impTmpls.size();
        for (int i = 0; i < eol; i++) {
            chkLines[0] = impLines.get(i);
            jsnText = DoHashVarSubs(comData, chkLines, recArr);
            impLines.set(i, jsnText);
        }
        DollarVarsImportTemplates(comData, recArr);     // Do string substituions in all import array items

        if (impLines.size() == 0) {
            jsnText = StitchTogetherV2(masterText);
        } else {
            jsnText = StitchTogether(masterText);
        }

        jsnText = jsnText.replaceAll("\\r?\\n", "");
        return jsnText;
    }

    private void LoadAllTemplates(nsCommonData comData) throws IOException {
        ArrayList<String> tmpDone = new ArrayList<>();
        tmpDone.add(comData.nsMapGet("template"));
        String[] tmpLineArray;
        String tmpLines;
        int eot, eoc;
        boolean more = true;
        while (more) {
            eot = impTmpls.size();
            for (int t = 0; t < eot; t++) {
                if (!tmpDone.contains(impTmpls.get(t))) {
                    tmpLineArray = GetImportTemplates(comData, impTmpls.get(t));        // Add imports to impTemp list
                    StringBuilder tmpLinesBuilder = new StringBuilder();
                    for (String s : tmpLineArray) {
                        tmpLinesBuilder.append(s).append("\n");
                    }
                    tmpLines = tmpLinesBuilder.toString();
                    impTlines.set(t, tmpLines);
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

        ArrayList<String> jsnArray;

        String[] lineParts, tmpLines;
        StringBuilder templateLine;
        String tmplName;
        String saveLine;
        boolean reStitch = true, hasHash;
        int posx, recPos, prePos, eot, eoi, startFrom=0;

        ArrayList<Integer> impRecs = new ArrayList<>();
        for (int i = 0; i < impLines.size(); i++) { impRecs.add(0); }

        while (reStitch) {
            hasHash = jsnText.contains("#");
            jsnArray = new ArrayList<>(Arrays.asList(jsnText.split("\\r?\\n")));
            eoi = jsnArray.size();
            StringBuilder jsnTextBuilder = new StringBuilder();
            for (int i = 0; i < eoi; i++) {
                templateLine = new StringBuilder(jsnArray.get(i));
                if (i >= startFrom) {
                    templateLine = new StringBuilder(templateLine.toString().replaceAll("\\r?\\n", ""));
                    if (templateLine.toString().startsWith("%import%")) {
                        if (startFrom < i) startFrom = i;
                        saveLine = templateLine.toString();
                        tmplName = templateLine.toString().replace("%import%", "").trim();
                        prePos = 999;
                        if (tmplName.endsWith("~")) {
                            prePos = Integer.parseInt(FieldOf(tmplName, "~", 2));
                            tmplName = FieldOf(tmplName, "~", 1);
                        }

                        posx = impTmpls.indexOf(tmplName);
                        if (posx < 0) continue;

                        lineParts = impLines.get(posx).split(fMark);
                        templateLine = new StringBuilder();

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
                            templateLine.append(lineParts[recPos]);
                            if (templateLine.toString().contains("%import%")) {
                                tmpLines = templateLine.toString().split("\\r?\\n");
                                eot = tmpLines.length;
                                templateLine = new StringBuilder();
                                for (int t = 0; t < eot; t++) {
                                    if (tmpLines[t].startsWith("%import%")) {
                                        tmpLines[t] = tmpLines[t].trim() + "~" + recPos + "~";
                                    }
                                    if (!tmpLines[t].trim().equals("")) templateLine.append(tmpLines[t]).append("\n");
                                }
                            }
                            if (!templateLine.toString().endsWith("\n")) templateLine.append("\n");
                            templateLine.append(saveLine);
                            recPos++;
                            impRecs.set(posx, recPos);
                        }
                    }
                    if (hasHash) {
                        if (templateLine.toString().contains("#")) templateLine =
                            new StringBuilder(SubsHashVars(templateLine.toString()));
                    }
                }
                if (!templateLine.toString().equals("")) jsnTextBuilder.append(templateLine).append("\n");
            }
            jsnText = jsnTextBuilder.toString();
            reStitch = jsnText.contains("%import%");
        }
        return jsnText;
    }

    private String StitchTogetherV2(String jsnText) {

        // stitch them all together into jsnText

        ArrayList<String> jsnArray;

        String templateLine, tmplName, importLine;
        boolean reStitch = true, hasHash;
        int posx, eoi;

        while (reStitch) {
            hasHash = jsnText.contains("#");
            jsnArray = new ArrayList<>( Arrays.asList(jsnText.split("\\r?\\n")));
            eoi = jsnArray.size();
            StringBuilder jsnTextBuilder = new StringBuilder();
            for (int i = 0; i < eoi; i++) {
                templateLine = jsnArray.get(i);

                templateLine = templateLine.replaceAll("\\r?\\n", "");
                if (templateLine.startsWith("%import%")) {
                    tmplName = templateLine.replace("%import%", "").trim();
                    posx = impTmpls.indexOf(tmplName);
                    if (posx < 0) continue;
                    importLine = impLines.get(posx);
                    templateLine = importLine.replace(fMark, "");
                    if (!templateLine.endsWith("\n")) templateLine += "\n";
                }
                if (hasHash) {
                    if (templateLine.contains("#")) templateLine = SubsHashVars(templateLine);
                }
                if (!templateLine.equals("")) jsnTextBuilder.append(templateLine).append("\n");
            }
            jsnText = jsnTextBuilder.toString();
            reStitch = jsnText.contains("%import%");
        }
        return jsnText;
    }

    private void DollarVarsImportTemplates(nsCommonData comData, String[] dList) {

        // Do string substituions in all import array items

        boolean redo;
        boolean proc;
        int mvPos, mvMax;
        int nbrImports = impTmpls.size();
        int fnd, eol;
        String[] jsnLines, lParts, chkArr;
        String templateLine;
        StringBuilder jsnTemp = new StringBuilder();
        String tmp1;
        String tmp2;

        for (String s : dList) {
            lParts = (s + tMark + blank).split(tMark);
            eol = lParts.length - 1;
            redo = false;
            mvPos = 0;
            mvMax = 0;
            for (int i = 0; i < nbrImports; i++) {
                proc = true;
                while (proc) {
                    jsnLines = impTlines.get(i).split("\\r?\\n");
                    for (String jsnLine : jsnLines) {
                        templateLine = jsnLine.trim();
                        if (templateLine.trim().startsWith("#"))
                            continue;
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
                                if (tmp2.equals(blank))
                                    tmp2 = "";
                                if (!redo && tmp2.contains(vMark))
                                    redo = true;
                                chkArr = tmp2.split(vMark);
                                if (chkArr.length > mvMax)
                                    mvMax = chkArr.length;
                                if (chkArr.length > mvPos) {
                                    tmp2 = chkArr[mvPos];
                                } else {
                                    tmp2 = "";
                                }

                                if (tmp2.contains("$"))
                                    tmp2 = tmp2.replaceAll("\\$", "__");
                                if (tmp2.contains("#")) {
                                    tmp2 = tmp2.replaceAll("#", "~~");
                                }

                                templateLine = templateLine.replace(tmp1, tmp2);
                            } else {
                                tmp2 = "";
                                templateLine = templateLine.replace(tmp1, tmp2);
                            }
                        }

                        if (templateLine.contains("#"))
                            templateLine = SubsHashVars(templateLine);
                        jsnTemp.append(templateLine).append("\n");
                    }
                    if (redo) {
                        mvPos++;
                        if (mvPos >= mvMax)
                            proc = false;
                    } else {
                        proc = false;
                    }
                }

                if (!impLines.get(i).equals(""))
                    jsnTemp.insert(0, impLines.get(i) + fMark);
                impLines.set(i, jsnTemp.toString());
                jsnTemp = new StringBuilder();
            }
        }
    }

    private String DoHashVarSubs(nsCommonData comData, String[] jsnLines, String[] lParts) {

        // build jsnText which will be held on impLines

        StringBuilder jsnText = new StringBuilder();
        String templateLine;
        String tmp1;
        String tmp2;
        int fnd;
        for (String jsnLine : jsnLines) {
            templateLine = jsnLine.trim();
            // starts with # means it's a comment line
            if (templateLine.trim().startsWith("#"))
                continue;
            // contains a # means it has hashVars
            if (templateLine.contains("#"))
                templateLine = SubsHashVars(templateLine);
            // do $var$ subs ONLY when one record returned !!
            if (totRecs == 1) {
                while (templateLine.contains("$")) {
                    tmp1 = FieldOf(templateLine, "\\$", 2);
                    tmp1 = "$" + tmp1 + "$";
                    fnd = comData.SubsList.indexOf(tmp1);
                    if (fnd >= 0) {
                        tmp2 = lParts[fnd];
                        if (tmp2.equals(blank))
                            tmp2 = "";
                    } else {
                        tmp2 = "unknown-hash-value-" + tmp1;
                    }
                    templateLine = templateLine.replace(tmp1, tmp2);
                }
            }
            jsnText.append(templateLine.trim()).append("\n");
        }
        return jsnText.toString();
    }

    private String GetTemplate(nsCommonData comData) throws IOException {
        String tPath = baseCamp + "/", fqfn;
        fqfn = tPath + comData.nsMapGet("template");
        return ReadFromDisk(fqfn);
    }

    private String[] GetImportTemplates(nsCommonData comData, String template) throws IOException {
        String tPath = baseCamp + "/", fqfn;
        fqfn = tPath + template;
        logger.logthis(comData.logHeader + "   ). Prepare template: "+fqfn);
        String jsnText = ReadFromDisk(fqfn);
        String[] jsnLines = jsnText.split("\\r?\\n");
        String templateLine;

        // Get all import templates into impTmpls array

        for (String jsnLine : jsnLines) {
            templateLine = jsnLine.trim();
            if (templateLine.trim().startsWith("#"))
                continue;
            if (templateLine.startsWith("%import%")) {
                templateLine = templateLine.replace("%import%", "").trim();
                logger.logthis(comData.logHeader + "      ). Import template: " + templateLine);
                impTmpls.add(templateLine);
                impTlines.add("");
                impLines.add("");
            }
        }
        if (totRecs > 1 && impTmpls.size() == 0) {
            String message = "ERROR: more than one DB record in action list and only one output template.";
            logger.logthis(comData.logHeader + message);
            throw new RFuelException(500, message);
        }
        return jsnLines;
    }

    private void GetHashVars(nsCommonData comData) {

        page = comData.nsMsgGet("page");
        pgSize = comData.nsMsgGet("page-size");

        try {
            pg = Integer.parseInt(page);
        } catch (NumberFormatException nfe) {
            pg = 1;
        }
        try {
            pgSz = Integer.parseInt(pgSize);
        } catch (NumberFormatException nfe) {
            pgSz = 1000;
        }

        if (pg < 1) pg = 1;
        if (pgSz < 10) pgSz = 10;
        page = String.valueOf(pg);
        pgSize = String.valueOf(pgSz);
        int nbrPgs;
        try {
            nbrPgs = totRecs / pgSz;
            lastPage = nbrPgs;
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
        String tmp1;
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
        }
        return inLine;
    }

    private void CreateSubsArrays(nsCommonData comData) throws IOException {
        String jsnText, templateLine;
        String[] jsnLines, lineParts;
        String tPath = baseCamp + "/", fqfn;

        jsonMap.clear();
        for (int t = 0; t < comData.Templates.size(); t++) {
            fqfn = tPath + comData.Templates.get(t);
            jsnText = ReadFromDisk(fqfn);

            jsnLines = jsnText.split("\\r?\\n");

            for (String jsnLine : jsnLines) {
                templateLine = jsnLine.trim();
                while (templateLine.contains("\"")) {
                    templateLine = templateLine.replace("\"", "");
                }
                while (templateLine.contains(",")) {
                    templateLine = templateLine.replace(",", "");
                }
                if (templateLine.startsWith("#"))
                    continue;
                if (templateLine.contains("{"))
                    continue;
                if (templateLine.contains("}"))
                    continue;
                lineParts = templateLine.split(":");
                if (lineParts.length > 1) {
                    if (lineParts[1].contains("$")) {
                        jsonMap.put(lineParts[0], lineParts[1].trim());
                    }
                }
            }
        }
    }

    private String BuildDSD(boolean needsBlank, nsCommonData comData) throws IOException {
        comData.Templates.clear();
        comData.TmplList.clear();

        comData.SubsList.clear();
        if (needsBlank) comData.SubsList.add("");

        String csvName;
        String content;
        String line;
        StringBuilder dsd = new StringBuilder();
        String cma = ",";
        String tmpl;
        String subs;
        String[] csvList = comData.nsMapGet("LIST").split(",");
        for (String s : csvList) {
            logger.logthis(comData.logHeader + "  .) BuildDSD(" + s + ")");
            csvName = baseCamp + slash + s;
            ArrayList<String> csvLines;

            content = new String(Files.readAllBytes(Paths.get(csvName)));
            if (content.startsWith("ENC(")) content = uCipher.Decrypt(content);
            csvLines = new ArrayList<>(Arrays.asList(content.split("\\r?\\n")));
            for (String csvLine : csvLines) {
                line = csvLine + cma + cma + cma + cma + cma + cma + cma + cma + cma + cma + blank;

                if (line.trim().equals("") || line.trim().startsWith("#"))
                    continue;

                String[] lParts = line.split(",");

                dsd
                    .append(lParts[0])
                    .append(cma)
                    .append(lParts[1])
                    .append(cma)
                    .append(lParts[2])
                    .append(cma)
                    .append(lParts[3])
                    .append(cma)
                    .append(lParts[4])
                    .append(fMark);
                String chk =
                    lParts[0] + cma + lParts[1] + cma + lParts[2] + cma + lParts[3] + cma + lParts[4] + cma + lParts[6]
                        + fMark;

                subs = lParts[6];
                if (lParts.length < 8 || subs.equals("")) {
                    tmpl = comData.nsMapGet("template");
                } else {
                    tmpl = lParts[7];
                }
                if (tmpl.equals(""))
                    tmpl = comData.nsMapGet("template");
                if (!subs.equals("")) {
                    if (!comData.SubsList.contains(subs))
                        comData.SubsList.add(subs);
                    if (!comData.TmplList.contains(tmpl))
                        comData.TmplList.add(tmpl);
                    if (!comData.Templates.contains(tmpl)) {
                        comData.Templates.add(tmpl);
                        String tPath = baseCamp + slash, fqfn;
                        fqfn = tPath + tmpl;
                        String junk = ReadFromDisk(fqfn);
                        logger.logthis(comData.logHeader + "     .) Load: " + tmpl);
                    }
                }
            }
        }
        return dsd.toString();
    }

    private String CheckResults(boolean needsBlank, nsCommonData comData, String[] recArr) {

        if (!needsBlank) return "";

        String[] chkArr = (recArr[0]+tMark+blank).split(tMark);
        int chk1 = comData.SubsList.size();
        int chk2 = chkArr.length;
        if (chk1 != chk2) {
            String out = "ERROR: mismatch between dsd and returned data string.";
            logger.logthis(comData.logHeader + "   ). *****");
            logger.logthis(comData.logHeader + "   ). "+ out);
            logger.logthis(comData.logHeader + "   ). SubsList() found "+comData.SubsList.size()+" items but rFuel returned "+chkArr.length+" items.");
            logger.logthis(comData.logHeader + "   ). Results should not be trusted.");
            logger.logthis(comData.logHeader + "   ). *****");
            return out;
        }

        return "";
    }

    // This shouldn't be neccesary anymore because we create a new nsOBMethods per request
    private void Reset() {
        jsonMap.clear();
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
        int today = (int) DAYS.between(day0, now);
        lowdate = String.valueOf(today - nbrDays);
    }

    public String ReadFromDisk(String fqn) throws IOException {
        BufferedReader BRin;
        FileReader fr;
        StringBuilder rec = new StringBuilder();
        fr = new FileReader(fqn);
        BRin = new BufferedReader(fr);
        String line = BRin.readLine();
        while ((line) != null) {
            rec.append(line).append("\n");
            line = BRin.readLine();
        }
        BRin.close();
        fr.close();
        return rec.toString();
    }

    public void extractItemId(nsCommonData comData) {
        iid = comData.nsMsgGet("customer");
        if (iid.equals("")) iid = comData.nsMsgGet("item");
    }

    public List<String> CallStandardRfuelSub(String dsd, nsCommonData comData, String subroutine) throws IOException {
        List<String> callArgs = new LinkedList<>();
        extractItemId(comData);
        callArgs.add("");
        callArgs.add(dsd);
        callArgs.add(iid);
        callArgs.add(comData.nsMsgGet("correlationid"));
        callArgs.add("");
        logger.logthis(comData.logHeader + "   .) Calling " + subroutine);

        List<String> outlist = CallSub(comData, subroutine, callArgs);

        String errCondition = outlist.get(0);
        if (!errCondition.equals("")) {
            logger.logthis(comData.logHeader + "ERROR: Database error - " + errCondition);
            throw new RFuelException(500, errCondition);
        }

        return outlist;
    }
    
}
