package com.unilibre.restful;

import com.unilibre.commons.APImsg;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.u2Commons;
import com.unilibre.commons.uCommons;
//import com.unilibre.restcommons.rCommons;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.commons.lang3.StringEscapeUtils.escapeJson;
import static org.apache.commons.lang3.StringEscapeUtils.escapeXml11;


public class FormatResponse {

    public static final String blank ="[<end>]";
    private String sep = "", tmplDir = "templates/", Template = "", basecamp="";
    private boolean isAssoc = false;
    private ArrayList<String> UniConstructor = new ArrayList<>();
    private ArrayList<String> doneThese = new ArrayList<>();
    private ArrayList<String> lines = new ArrayList<>();
    private ArrayList<String> outpt = new ArrayList<>();
    public ArrayList<String> Templates = new ArrayList<>();
    public ArrayList<String> DataList = new ArrayList<>();
    public ArrayList<String> DataLineage = new ArrayList<>();
    public ArrayList<String> SubsList = new ArrayList<>();
    public ArrayList<String> TmplList = new ArrayList<>();
    public ArrayList<Boolean> AsocList = new ArrayList<>();
    public ArrayList<String> holdVals = new ArrayList<>();
    public ArrayList<String> holdTags = new ArrayList<>();

    public void SetBaseCamp(String b) { this.basecamp = b; }

    public void SetTemplate(String t) {
        this.Template = t;
        SetTemplates(t);
    }

    public void SetTemplates(String t) { this.Templates.add(t); }

    public void SetDataList(ArrayList arr) { this.DataList = arr; }

    public void SetDataLineage(ArrayList arr) { this.DataLineage = arr; }

    public void SetSubsList(ArrayList arr) { this.SubsList = arr; }

    public void SetTmplList(ArrayList arr) { this.TmplList = arr; }

    public void SetAsocList(ArrayList arr) { this.AsocList = arr; }


    public String Replacements(String processor) {

        boolean backfilled = false, isJSON = false, isXML = false, isCSV = false, SysErr = false, backfill = false;
        String tmpl, subs, valu, indx;
        String[] curDetails = {"", "", "", "", "", ""};
        ArrayList<String> ucIndex = new ArrayList<>();

        String tplName, tmpLine;
        if (Templates.indexOf(Template) < 0) {
            DataList.add(0, "");
            DataLineage.add(0, "");
            SubsList.add(0, "");
            TmplList.add(0, Template);
            Templates.add(0, Template);
            AsocList.add(0, false);
        }
        int nbrTPs = Templates.size();
        String data;
        for (int tt = 0; tt < nbrTPs; tt++) {
            tplName = Templates.get(tt);
            data = uCommons.ReadDiskRecord(basecamp + tmplDir + tplName, true);
            lines = (ArrayList<String>) Arrays.asList((data).split("\\r?\\n"));
            data = "";
//            lines = rCommons.ReadTemplate(basecamp + tmplDir + tplName);
            for (int t = 0; t < lines.size(); t++) {
                tmpLine = lines.get(t).trim();
                if (lines.get(t).replace(" ", "").equals("")) {
                    lines.remove(t);
                    t--;
                    continue;
                }
                lines.set(t, tmpLine);
            }

            if (lines.get(0).startsWith("{")) isJSON = true;
            if (lines.get(0).startsWith("<")) isXML = true;
            if (lines.get(0).toLowerCase().startsWith("csv")) {
                isCSV = true;
                isXML = true;
            }
        }

        if (TmplList.indexOf(Templates.get(0)) < 0) {
            if (TmplList.size() == 0) {
                TmplList.add(Templates.get(0));
            } else {
                if (TmplList.get(0).equals("")) {
                    TmplList.set(0, Templates.get(0));
                } else {
                    TmplList.add(Templates.get(0));
                }
            }
        }
        int nbrItems = TmplList.size();
        int uPos, aRepeats, mRepeats, sRepeats;
        // ----------------------------------------------------
        // -------- Build the UniConstructor ------------------
        // ----------------------------------------------------
        for (int ix = 0; ix < nbrItems; ix++) {
            tmpl = TmplList.get(ix);
            subs = SubsList.get(ix).trim();
            if (!subs.equals("")) {
                valu = DataList.get(ix);
                valu = valu.replace("\t", "<tab>");
                indx = tmpl + "\t" + subs;
                uPos = ucIndex.indexOf(indx);
                aRepeats = 0;mRepeats = 0;sRepeats = 0;
                if (uPos < 0) {
                    ucIndex.add(indx);
                    uPos = ucIndex.indexOf(indx);
                    if (uPos < 0) {
                        uCommons.uSendMessage(processor + " "+ indx + " has not been built. Stopping now.");
                        SysErr = true;
                        break;
                    }
                    if (valu.contains(NamedCommon.FMark)) aRepeats = aRepeats + u2Commons.sDcount(valu, "A");
                    if (valu.contains(NamedCommon.VMark)) mRepeats = mRepeats + u2Commons.sDcount(valu, "M");
                    if (valu.contains(NamedCommon.SMark)) sRepeats = sRepeats + u2Commons.sDcount(valu, "S");
                    if (aRepeats == 0) aRepeats = 1;
                    if (mRepeats == 0) mRepeats = 1;
                    if (sRepeats == 0) sRepeats = 1;
                } else {
                    // re-used the same variable in the same template !
                    curDetails = UniConstructor.get(uPos).split("\t");
                    valu = curDetails[2] + NamedCommon.FMark + valu;
                    aRepeats = 2;
                }
                UniConstructor.add(uPos, indx + "\t" + valu + "\t" + aRepeats + "\t" + mRepeats + "\t" + sRepeats);
            } else {
                indx = tmpl + "\t" + subs;
                UniConstructor.add(indx + "\t\t\t\t");
            }
        }

        String answer = "";
        lines.clear();
        outpt.clear();
        if (!SysErr) {
            lines.clear();
            // -------------------------------------------------- //
            // ----------- for each template in the list -------- //
            // -------------------------------------------------- //

            boolean lChk = false;
            String xmlLineage = "";
            String tmplName="", tempLine="", payload="";
            int nbrTemplates = Templates.size();
            for (int tt = 0; tt < nbrTemplates; tt++) {
                tmplName = Templates.get(tt);
//                lines = rCommons.ReadTemplate(basecamp + tmplDir + tmplName);
                data = uCommons.ReadDiskRecord(basecamp + tmplDir + tmplName, true);
                lines = (ArrayList<String>) Arrays.asList((data).split("\\r?\\n"));
                data = "";


                // ----------- tidy up the template -----------

                for (int t = 0; t < lines.size(); t++) {
                    tempLine = lines.get(t).trim();
                    if (tempLine.startsWith("#")) { lines.remove(t); t--; continue; }
                    if (lines.get(t).replace(" ", "").equals("")) { lines.remove(t); t--; continue; }
                    if (lines.get(0).toUpperCase().startsWith("CSV")) { lines.remove(t); t--; continue; }
                    lines.set(t, tempLine);
                }

                // ----------- validate csv template -----------
                if (isCSV) {
                    ArrayList<String> nChk = new ArrayList<String>(Arrays.asList(lines.get(0).split("\\,")));   // template line 1: node list
                    ArrayList<String> eChk = new ArrayList<String>(Arrays.asList(lines.get(1).split("\\,")));   // template line 2: elelement list
                    if (nChk.size() != eChk.size()) {
                        uCommons.uSendMessage(processor + " " + "Error in (CSV) template ["+tmplName+"]: " + nChk.size() + " nodes do not match with " + eChk.size() + " elements");
                        return "";
                    }
                    nChk.clear();
                    eChk.clear();
                }

                // ----------- build payload for the template -----------
                payload = BuildPayLoad(lines, tmplName);
                outpt.add(payload);

                // ----------- prepare for next iteration -----------
                lines.clear();
            }

            /* -------------------------------------------------- */
            /* ----------------- Stitch them all together ------- */
            /* -------------------------------------------------- */

            int fPos, nbrLines;
            String includeTmpl, workFld, thisLine, tmp1, tmp2;
            answer = "";
            if (!isCSV) {
                boolean done = false, fnd;
                workFld = outpt.get(0);
                fnd = (workFld.indexOf("%import%")   > 0);
                fnd = (fnd || workFld.indexOf("\"$") > 0);
                if (fnd) {
                    fnd = false;
                    while (workFld.startsWith("\n")) { workFld = workFld.substring(1, workFld.length()); }
                    lines = new ArrayList<>(Arrays.asList(workFld.split("\\r?\\n")));
                    workFld = "";
                    while (!done) {
                        nbrLines = lines.size();
                        fnd = false;
                        for (int ln = 0; ln < nbrLines; ln++) {
                            thisLine = lines.get(ln);
                            fPos = thisLine.indexOf("%import%");
                            if (fPos >= 0) {
                                fnd = true;
                                if (!thisLine.startsWith("{")) {
                                    thisLine = thisLine.replaceAll("\\ ", "");
                                }
                                includeTmpl = uCommons.FieldOf(thisLine, "%", 3);
                                fPos = Templates.indexOf(includeTmpl);
                                if (fPos >= 0) {
                                    thisLine = outpt.get(fPos);
                                } else {
                                    thisLine = includeTmpl + " has not been mapped in your csv";
                                }
                            }
                            //
                            // ---------------------------------------------------------------------------------------------
                            // Sometimes the number of items in an association can get screwed up and rather than following
                            // the screw up, recognise that it CAN happen and cater for it by setting $var$ to empty-string.
                            // ---------------------------------------------------------------------------------------------
                            //
                            fPos = thisLine.indexOf("$");
                            while (fPos > 0) {
                                tmp1 = thisLine.substring(0, fPos);
                                tmp2 = thisLine.substring(fPos + 1, thisLine.length());
                                fPos = tmp2.indexOf("$");
                                tmp2 = tmp2.substring(fPos + 1, tmp2.length());
                                thisLine = tmp1 + tmp2;
                                fPos = thisLine.indexOf("$");
                            }
                            if (!workFld.equals("")) workFld += "\n";
                            workFld += thisLine.trim();
                        }
                        if (!fnd) {
                            done = true;
                        } else {
                            while (workFld.startsWith("\n")) {
                                workFld = workFld.substring(1, workFld.length());
                            }
                            lines = new ArrayList<>(Arrays.asList(workFld.split("\\r?\\n")));
                            workFld = "";
                        }
                    }
                }
                answer = workFld;
                lines.clear();
            }
        }
        answer = ScrubData(answer, "\t");
        answer = ScrubData(answer, "\r");
        answer = ScrubData(answer, "\n");
        answer = ScrubData(answer, "\\ \\ ");
        answer = answer.trim();
        return answer;
    }

    private String ScrubData(String valu, String chr) {
        valu = valu.replaceAll(chr, "");
        return valu;
    }

    private String BuildPayLoad(ArrayList<String> lines, String tmplName) {
        uCommons.uSendMessage("   .) BuildPayLoad(" + tmplName+ ")");
        String tmpl, payLoad = "", thisLoad = "";
        int arepeats, mrepeats, srepeats, repeats;
        boolean repeatingVals = false;
        String[] conParts;
        int nbrItems = UniConstructor.size();
        for (int u2 = 0; u2 < nbrItems; u2++) {
            conParts = UniConstructor.get(u2).split("\t");
            tmpl = conParts[0];
            if (doneThese.indexOf(tmpl) > -1) continue;
            if (!tmpl.equals(tmplName)) continue;
            if (conParts.length < 6) continue;
            // ------------------------------------------ //
            arepeats = Integer.valueOf(conParts[3]);
            mrepeats = Integer.valueOf(conParts[4]);
            srepeats = Integer.valueOf(conParts[5]);

            conParts = null;
            repeats = 1;
            if (arepeats > repeats) repeats = arepeats;
            if (mrepeats > repeats) repeats = mrepeats;
            if (srepeats > repeats) repeats = srepeats;
            holdVals.clear();
            holdTags.clear();

            thisLoad = "\n" + Substitutions(lines, tmplName, repeats);

            repeatingVals = false;
            repeatingVals = (repeatingVals || thisLoad.contains(NamedCommon.FMark));
            repeatingVals = (repeatingVals || thisLoad.contains(NamedCommon.VMark));
            repeatingVals = (repeatingVals || thisLoad.contains(NamedCommon.SMark));

            if (repeatingVals) {
                payLoad = "[";
                thisLoad = "";

                int eoh = holdVals.size();
                int eol = lines.size(), tPos;
                int aCnt=0, mCnt=0, sCnt=0;
                String tmpData, avData, mvData, svData;
                // -------------------------------------------------------------------------
                String thisTag, thisLine, datum;
                ArrayList<String> tagsDone = new ArrayList<>();
                // -------------------------------------------------------------------------

                // holdVals should have been backfilled - that's why fm vm and sm are in the data.
                tmpData = holdVals.get(0);      // just to get an aCnt value !!

                aCnt = u2Commons.sDcount(tmpData, "A");
                for (int avc=1 ; avc <= aCnt; avc++) {
                    avData = u2Commons.sExtract(tmpData, avc, 0, 0);
                    mCnt = u2Commons.sDcount(avData, "M");
                    for (int mvc=1 ; mvc <= mCnt ; mvc++) {
                        mvData = u2Commons.sExtract(avData, 1, mvc, 0);
                        sCnt = u2Commons.sDcount(mvData, "S");
                        // -------------------------------------------------------------------------
                        for (int svc=1 ; svc <= sCnt ; svc++) {
                            for (int ln = 0; ln < eol; ln++) {
                                thisLine = lines.get(ln);
                                while (thisLine.indexOf("$") > -1) {
                                    thisTag = uCommons.FieldOf(thisLine, "\\$", 2);
                                    if (thisTag.equals("")) { thisLoad += thisLine + "\n"; continue; }
                                    thisTag = "$" + thisTag + "$";
                                    tPos = holdTags.indexOf(thisTag);
                                    if (tPos > -1) {
                                        svData = u2Commons.sExtract(holdVals.get(tPos), avc, mvc, svc);
                                        datum = svData;
                                        thisLine = thisLine.replace(thisTag, datum);
                                    } else {
                                        datum = "Item not mapped";
                                        thisLine = thisLine.replace(thisTag, datum);
                                    }
                                }
                                thisLoad += thisLine + "\n";
                            }
                        }
                        payLoad += thisLoad + ",";
                        thisLoad = "";
                        // -------------------------------------------------------------------------
                    }
                }
                payLoad += "]";
                payLoad = payLoad.replace(",]", "]");
                tmpData=""; avData=""; mvData=""; svData=""; thisLine="";
            } else {
                payLoad = thisLoad;
            }
            doneThese.add(tmpl);
        }
        if (payLoad.equals("") && doneThese.indexOf(tmplName) < 0) {
            // Probably a placeholder template for %import% template(s)
            for (int l=0; l < lines.size(); l++) { payLoad += lines.get(l) + "\n"; }
        }
        return payLoad;
    }

    private String Substitutions(ArrayList<String> lines, String thisTemplate, int repeat) {
        // thisTemplate - the template from the TemplList   //
        // lines        - array of lines from thisTemplate  //

        boolean isJSON=false, isXML=false;

        if (lines.get(0).startsWith("{")) isJSON = true;
        if (lines.get(0).startsWith("<")) isXML = true;

        String xmlLine = "", theTag = "", datum = "", xmlRequest = "", sPart = "", lineage = "", xmlLineage="";
        ArrayList<String> lineTags = new ArrayList<>();
        ArrayList<String> lineVals = new ArrayList<>();
        ArrayList<String> lineTemp = new ArrayList<>();
        ArrayList<String> lineLocn = new ArrayList<>();
        lineTags.clear();
        lineVals.clear();
        lineTemp.clear();
        lineLocn.clear();
        boolean splitData, assocFLAG = false;
        String prevSVline;

        int maxA = 0, maxM = 0, maxS = 0;
        int assocCnt = 1;
        int nbrLines = lines.size(), fPos = 0;
        String[] amsLines;

        for (int ll = 0; ll < nbrLines; ll++) {
            xmlLine = lines.get(ll);
            if (!xmlLine.contains("$")) {
                xmlRequest += xmlLine + "\r\n";
                xmlLineage += "\r\n";
            } else {
                while (xmlLine.contains("$")) {
                    lineTags.clear();
                    lineVals.clear();
                    lineTemp.clear();
                    lineLocn.clear();

                    String holdLine = xmlLine;

                    int nbrTags = 0;
                    int[] dPos = new int[500];
                    for (int d = 0; d < dPos.length; d++) { dPos[d] = 0; }
                    int dpIdx = 0;
                    boolean isFirst = true;
                    String dataGroup = "";
                    while (holdLine.contains("$")) {
                        theTag = uCommons.FieldOf(holdLine, "\\$", 2);
                        if (theTag.equals("")) continue;
                        theTag = "$" + theTag + "$";
                        fPos = SubsList.indexOf(theTag);
                        if (fPos  < 0) {
                            holdLine = "";
                        } else {
                            datum = DataList.get(fPos);
                            if (!isFirst) dataGroup += "<fm>";
                            dataGroup += datum;
                            holdLine = holdLine.replace(theTag, "@@@");
                            nbrTags++;
                            dPos[dpIdx] = fPos;
                            dpIdx++;
                        }
                        isFirst = false;
                    }
                    if (dataGroup.contains("<fm>") && nbrTags > 1) {
                        dataGroup = FormatAssoc(dataGroup);
                        String[] dataResult = (dataGroup+"<fm>" + blank).split("\\<fm\\>");
                        for (int d = 0; d < nbrTags; d++) {
                            if (dataResult[d].equals(blank)) continue;
                            fPos = dPos[d];
                            dataGroup = dataResult[d];
                            DataList.set(fPos, dataGroup);
                        }
                        dataResult = null;
                    }
                    dPos = null;
                    dataGroup = "";
                    holdLine = xmlLine;
                    int nbrReplacements = 0, nbrAVs = 0, nbrMVs = 0, nbrSVs = 0, tNbr = 0, rChk = 0;
                    int parcelCount=0, tagCount=0;
                    // -----------------------------------------------------------------------------
                    // Loop for each tag in THIS LINE only !!!
                    // -----------------------------------------------------------------------------
                    for (int tag = 0; tag < nbrTags; tag++) {
                        theTag = uCommons.FieldOf(holdLine, "\\$", 2);
                        if (theTag.equals("")) continue;    // a repeat of a tag previously encountered - such as assocNbr
                        theTag = "$" + theTag + "$";
                        assocFLAG = false;

                        holdLine = holdLine.replace(theTag, "@@@");
                        boolean found = false;
                        boolean exitSW = false;
                        while (!exitSW) {
                            fPos = SubsList.indexOf(theTag);
                            if (fPos >= 0) {
                                if (TmplList.get(fPos).equals(thisTemplate)) {
                                    datum = DataList.get(fPos);
                                    isAssoc = false; // AsocList.get(fPos);
                                    lineage = DataLineage.get(fPos);
                                    found = true;
                                    exitSW = true;
                                } else {
                                    int nbrItems = SubsList.size();
                                    fPos++;
                                    datum = "[" + theTag + "] is not mapped";
                                    lineage = "Error";
                                    for (int ii = fPos; ii < nbrItems; ii++) {
                                        if (SubsList.get(ii).equals(theTag)) {
                                            if (TmplList.get(ii).equals(thisTemplate)) {
                                                datum = DataList.get(ii);
                                                isAssoc = false; // AsocList.get(ii);
                                                lineage = DataLineage.get(ii);
                                                found = true;
                                                break;
                                            }
                                        }
                                    }
                                    exitSW = true;
                                }
                            } else {
                                // handle $repeats$ and/or $iteration$ HERE //
                                if (theTag.startsWith("$assoc")) {
                                    switch (theTag) {
                                        case "$assocTotal$":
                                            datum = String.valueOf(repeat);
                                            found = true;
                                            break;
                                        case "$assocNbr$":
                                            datum = String.valueOf(assocCnt);
                                            found = true;
                                            assocFLAG = true;
                                            break;
                                        default:
                                            String eMessage = "ERROR:  '=assoc' recordset indicators are $assocTotal$ or $assocNbr$. ";
                                            eMessage += "You have [" + theTag + "] - please correct and re-run";
                                            uCommons.uSendMessage(eMessage);
                                            eMessage = "";
                                    }
                                } else {
                                    datum = "[" + theTag + "] is not mapped";
                                    lineage = "Error";
                                }
                                exitSW = true;
                            }
                        }

                        if (!found) {
                            uCommons.uSendMessage("DataConverter error: " + theTag + " from the csv was not found in the template");
                            continue;
                        }
                        if (datum.toLowerCase().equals("!x!")) datum = "";

                        lineTags.add(theTag);
                        lineVals.add(datum);
                        lineTemp.add(xmlLine);
                        lineLocn.add(lineage);
                        holdTags.add(theTag);
                        holdVals.add(datum);
                        nbrAVs = u2Commons.sDcount(datum, "A");
                        parcelCount = parcelCount + nbrAVs;
                        int svChk = 0, mvChk = 0;
                        for (int aa = 1; aa <= nbrAVs; aa++) {
                            mvChk = u2Commons.sDcount(u2Commons.sExtract(datum, aa, 0, 0), "M");
                            parcelCount = parcelCount + mvChk;
                            for (int mm = 1; mm <= mvChk; mm++) {
                                svChk = u2Commons.sDcount(u2Commons.sExtract(datum, aa, mm, 0), "S");
                                parcelCount = parcelCount + svChk;
                            }
                        }
                        tagCount++;
                    }

                    int upperlimit = parcelCount;
                    nbrReplacements= lineTags.size();

                    String[] outLines = new String[upperlimit];
                    amsLines = null;
                    amsLines = new String[upperlimit];
                    for (int yy = 0; yy < upperlimit; yy++) { outLines[yy] = "";amsLines[yy] = ""; }

                    boolean done = false, doneA = false, doneM = false, doneS = false, shuffled = false;
                    String thisLineage, thisPart, escTemp, escSave;
                    int aCnt, mCnt, sCnt;

                    tNbr = lineTags.size();

                    for (int yy = 0; yy < tNbr; yy++) {
                        int lnCtr = 0;
                        String tmpxml = "";

                        theTag = lineTags.get(yy);
                        xmlLine = lineTemp.get(yy);
                        datum = lineVals.get(yy);
                        lineage = lineLocn.get(yy);
                        if (amsLines[lnCtr].equals("")) amsLines[lnCtr] = xmlLine;

                        if (!thisTemplate.equals(Template)) {
                            // this is for normal templates as well as %import% templates
                            splitData = false;
                        } else {
                            // this is for csv templates - try to make sure these are not used anymore
                            splitData = (lineage.toUpperCase().contains("N"));
                            splitData = (splitData || theTag.equals("$assocNbr$"));
                            splitData = (splitData && !datum.contains(NamedCommon.FMark));
                        }

                        done = false; // doneA = false; doneM = false; doneS = false;
                        if (!splitData) {
                            if (APImsg.APIget("showLineage").toLowerCase().equals("true")) {
                                if ((lineage.length() - 2) == lineage.replaceAll("\\-", "").length()) {
                                    String[] lparts = lineage.split("\\-");
                                    if (lparts[0].toUpperCase().equals("N")) lparts[0] = "1";
                                    if (lparts[1].toUpperCase().equals("N")) lparts[1] = "1";
                                    if (lparts[2].toUpperCase().equals("N")) lparts[2] = "1";
                                    if (lparts[0].equals("0")) {
                                        lparts[1] = "0";
                                        lparts[2] = "";
                                    }
                                    thisPart = "<value " +
                                            "AV=\"" + lparts[0] + "\" " +
                                            "MV=\"" + lparts[1] + "\" " +
                                            "SV=\"" + lparts[2] + "\"" +
                                            ">" + datum + "</value>";
                                    datum = thisPart;
                                }
                            }
                            if (datum.equals(" ")) datum = "";

                            aCnt = u2Commons.sDcount(datum, "A");
                            for (int repA=1 ; repA <= aCnt; repA++) {
                                escTemp = u2Commons.sExtract(datum, repA, 0, 0);
                                mCnt = u2Commons.sDcount(escTemp, "M");
                                for (int repM=1 ; repM <= mCnt; repM++) {
                                    escTemp = u2Commons.sExtract(datum, repA, repM, 0);
                                    sCnt = u2Commons.sDcount(escTemp, "S");
                                    for (int repS=1 ; repS <= sCnt; repS++) {
                                        escTemp = u2Commons.sExtract(datum, repA, repM, repS);
                                        escSave = escTemp;
                                        if (isJSON) {
                                            escTemp = escapeJson(escTemp);
                                            while (escTemp.contains("\\/")) {
                                                escTemp = escTemp.replace("\\/", "/");
                                            }
                                        }
                                        if (isXML) escTemp = escapeXml11(escTemp);
                                        if (!escTemp.equals(escSave)) datum = u2Commons.sReplace(datum, repA, repM, repS, escTemp);
                                    }
                                }
                            }

                            // --------------------------------------------------------------------------------------------
                            if (outLines[lnCtr].equals(""))  outLines[lnCtr] = xmlLine;
                            outLines[lnCtr] = outLines[lnCtr].replace(theTag, datum);
                            amsLines[lnCtr] = amsLines[lnCtr].replace(theTag, lineage.replace("-", "_"));
                            int l = lnCtr + 1;
                            while (!outLines[l].equals("")) {
                                outLines[l] = outLines[l].replace(theTag, datum);
                                amsLines[l] = amsLines[l].replace(theTag, lineage.replace("-", "_"));
                                l++;
                            }
                            // --------------------------------------------------------------------------------------------

                        } else {
                            int AVcnt = u2Commons.sDcount(datum, "A");
                            if (isAssoc && maxA > AVcnt) AVcnt = maxA;
                            for (int av = 1; av <= AVcnt; av++) {
                                String saveLineage = amsLines[lnCtr];
                                prevSVline = "";
                                thisPart = u2Commons.sExtract(datum, av, 0, 0);
                                int MVcnt = u2Commons.sDcount(thisPart, "M");
                                if (MVcnt > maxM) maxM = MVcnt;
                                if (isAssoc && maxM > MVcnt) MVcnt = maxM;
                                for (int mv = 1; mv <= MVcnt; mv++) {
                                    String saveLine = "";
                                    if (prevSVline.equals("")) prevSVline = outLines[lnCtr];
                                    if (prevSVline.equals("") || assocFLAG) prevSVline = xmlLine;
                                    thisPart = u2Commons.sExtract(datum, av, mv, 0);
                                    int SVcnt = u2Commons.sDcount(thisPart, "S");
                                    if (SVcnt > maxS) maxS = SVcnt;
                                    if (isAssoc && maxS > SVcnt) SVcnt = maxS;
                                    for (int sv = 1; sv <= SVcnt; sv++) {
                                        if (!done) done = (doneA && doneM && doneS);
                                        if (!theTag.equals("$assocNbr$")) {
                                            thisPart = u2Commons.sExtract(datum, av, mv, sv);
                                        } else {
                                            assocFLAG = true;
                                            continue;
                                        }
                                        thisLineage = "";
                                        if (thisPart.equals(" ")) thisPart = "";
                                        if (APImsg.APIget("showLineage").toLowerCase().equals("true") && !lineage.equals("Error")) {
                                            String[] lparts = lineage.split("\\-");
                                            if (lparts[0].toUpperCase().contains("N")) lparts[0] = String.valueOf(av);
                                            if (lparts[1].toUpperCase().contains("N")) lparts[1] = String.valueOf(mv);
                                            if (lparts[2].toUpperCase().contains("N")) lparts[2] = String.valueOf(sv);
                                            if (lparts[0].equals("0")) {
                                                lparts[1] = "0";
                                                lparts[2] = "";
                                            }
                                            thisLineage = lparts[0] + "_" + lparts[1] + "_" + lparts[2];
                                            thisPart = "<value " +
                                                    "AV=\"" + lparts[0] + "\" " +
                                                    "MV=\"" + lparts[1] + "\" " +
                                                    "SV=\"" + lparts[2] + "\"" +
                                                    ">" + thisPart + "</value>";
                                        }
                                        if (!outLines[lnCtr].equals("")) {
                                            saveLine = outLines[lnCtr];
                                            prevSVline = saveLine;
                                        } else {
                                            saveLine = prevSVline;
                                        }

                                        if (isJSON) {
                                            thisPart = escapeJson(thisPart);
                                            while (thisPart.contains("\\/")) { thisPart = thisPart.replace("\\/", "/"); }
                                        }
                                        if (isXML) thisPart = escapeXml11(thisPart);

                                        tmpxml = saveLine.replace(theTag, thisPart);
                                        outLines[lnCtr] = tmpxml;
                                        amsLines[lnCtr] = saveLineage.replace(theTag, thisLineage);
                                        lnCtr++;
                                        if (lnCtr > upperlimit) {
                                            uCommons.uSendMessage("ERROR in multi-part data handling");
                                            return "";
                                        }
                                        if (!amsLines[lnCtr].equals("")) saveLineage = amsLines[lnCtr];
                                        if ((sv + 1) <= SVcnt) {
                                            if (!done || !shuffled) {
                                                // ---- shuffle everything down one to make way for the next sv substitution -------
                                                for (int xx = (nbrReplacements - 1); xx >= lnCtr; xx--) {
                                                    outLines[xx] = outLines[xx - 1];
                                                    amsLines[xx] = amsLines[xx - 1];
                                                }
                                                outLines[lnCtr] = prevSVline;
                                                shuffled = true;
                                            }
                                        }
                                        if (!doneS) doneS = ((sv + 1) > maxS);
                                    }
                                    if (!doneM) doneM = ((mv + 1) > maxM);
                                }
                                if (!doneA) doneA = ((av + 1) > maxA);
                            }
                        }
                    }
                    String[] holdItems;
                    String chk, holdItem;
                    int tpos;

                    if (outLines.length > 0) {
                        for (int yy = 0; yy <= outLines.length; yy++) {
                            holdLine = outLines[yy];
                            if (holdLine == "") break;
                            if (sep.equals("")) sep = ",";
                            holdItems = holdLine.split("<tm>");
                            boolean chkIt = false;
                            for (int h = 0; h < holdItems.length; h++) {
                                holdItem = holdItems[h];
                                chkIt = (holdItem.startsWith("$") && holdItem.endsWith("$"));
                                if (!chkIt) {
                                    //
                                } else {
                                    // back-fill - mix of repeating values with non-repeating values
                                    if (holdItem.equals("$assocNbr$")) {
                                        holdLine = holdLine.replace(holdItem, String.valueOf(assocCnt));
                                        assocCnt++;
                                        continue;
                                    }
                                    chk = "[" + holdItem + "]";
                                    if (holdLine.indexOf(chk) < 0) {
                                        datum = ""; //lineVals.get(lineTags.indexOf(theTag));
                                        holdLine = holdLine.replace(holdItem, datum);
                                        continue;
                                    }
                                    tpos = SubsList.indexOf(holdItem);
                                    if (tpos < 0) {
                                        datum = "";
                                    } else {
                                        datum = DataList.get(tpos);
                                    }
                                    holdLine = holdLine.replace(holdItem, datum);
                                }
                            }
                            outLines[yy] = holdLine;
                            xmlRequest += outLines[yy] + "\r\n";
                            xmlLineage += amsLines[yy] + "\r\n";
                        }
                    }
                    xmlLine = "";
                    outLines = null;
                }
                if (xmlLine != "") xmlRequest += xmlLine + "\r\n";
            }
        }
        return xmlRequest;
    }

    public static String FormatAssoc(String dataGroup) {
        if (dataGroup.endsWith("<fm>")) dataGroup += blank;
        if (dataGroup.startsWith("<fm>")) dataGroup = blank + dataGroup;
        String[] dLines = dataGroup.split("\\<fm\\>");
        String[] newLines = new String[dLines.length];
        String[] junk  = null;
        String[] sjunk = null;
        int em=0, es=0, maxMv=1, maxSv=1;
        for (int i=0; i < dLines.length ; i++) {
            if (dLines[i].endsWith("<vm>")) dLines[i] += blank;                     //  ## TEST ##
            if (dLines[i].startsWith("<vm>")) dLines[i] = blank + dLines[i];        //  ## TEST ##
            junk = dLines[i].split("\\<vm\\>");
            em = junk.length;
            if (em > maxMv) maxMv = junk.length;
            for (int j=0 ; j < em ; j++) {
                if (junk[j].endsWith("<sm>")) junk[j] += blank;                 //  ## TEST ##
                if (junk[j].startsWith("<sm>")) junk[j] = blank + junk[j];      //  ## TEST ##
                sjunk = junk[j].split("\\<sm\\>");
                es = sjunk.length;
                if (es > maxSv) maxSv = es;
            }
            junk = null;
            sjunk= null;
        }
        String dItem="", marker="";
        for (int l=0 ; l < dLines.length ; l ++) { newLines[l] = ""; }
        for (int l=0 ; l < dLines.length ; l ++) {
            for (int m = 1; m <= maxMv; m++) {
                newLines[l] += marker;
                marker = "";
                for (int s = 1; s <= maxSv; s++) {
                    dItem = u2Commons.sExtract(dLines[l], 1, m, s);
                    if (dItem.equals(blank)) dItem = "";
                    newLines[l] += marker + dItem;
                    marker = "<sm>";
                }
                marker = "<vm>";
            }
            marker = "";
        }
        StringBuilder sb = new StringBuilder();
        for (int l=0 ; l < dLines.length ; l ++) {
            sb.append(newLines[l]);
            if ((l+1) != dLines.length) { sb.append("<fm>"); }
        }
        dItem = "";
        marker= "";
        dLines = null;
        newLines = null;
        return sb.toString();
    }

}
