package com.unilibre.core;
/**
 * Copyright UniLibre on 2015. ALL RIGHTS RESERVED
 **/


//  testing with isWebs=true for HttpInOut

import asjava.uniclientlibs.UniDynArray;
import com.unilibre.commons.APImsg;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.u2Commons;
import com.unilibre.commons.uCommons;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.events.Attribute;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import static org.apache.commons.lang3.StringEscapeUtils.escapeJson;
import static org.apache.commons.lang3.StringEscapeUtils.escapeXml11;


public class DataConverter {

    protected static DocumentBuilderFactory domFactory = null;
    protected static DocumentBuilder domBuilder = null;
    private final static String blank = "[*END*]";
    private static ArrayList<String> lineTags = new ArrayList<>();
    private static ArrayList<String> lineVals = new ArrayList<>();
    private static ArrayList<String> lineTemp = new ArrayList<>();
    private static ArrayList<String> lineLocn = new ArrayList<>();
    private static ArrayList<String> holdVals = new ArrayList<>();
    private static ArrayList<String> holdTags = new ArrayList<>();
    private static ArrayList<String> lines = new ArrayList<>();
    private static ArrayList<String> outpt = new ArrayList<>();
    private static ArrayList<String> UniConstructor;
    private static ArrayList<String> doneThese = new ArrayList<>();
    private static ArrayList<String> rsContainers = new ArrayList<>();      // recordset containers
    private static ArrayList<String> heads = new ArrayList<>();
    private static ArrayList<String> items = new ArrayList<>();
    private static ArrayList<String> arrLineage = new ArrayList<>();
    private static ArrayList<String> dlineage = new ArrayList<>();
    private static ArrayList<String> csv = new ArrayList<>();
    private static ArrayList<Element> elements = new ArrayList<>();
    private static ArrayList<String> names = new ArrayList<>();
    private static ArrayList<String> previous = new ArrayList<>();
    private static ArrayList<Integer> nodes = new ArrayList<>();
    private static ArrayList<String> nNames = new ArrayList<>();
    private static String[] amsLines;
    private static String xmlLineage = "";
    private static String sep = "", trythese = ",;:]}/'-";
    private static int rsCounter = 1;           // recordset counter
    private static int iiCounter = 1;           // independent item counter

    private static boolean isCSV = false;
    private static boolean isJSON = false;
    private static boolean isXML = false;
    private static boolean backfill = false;
    private static boolean backfilled = false;

    public static String ResponseHandler(String status, String descr, String response, String esbFMT) {
        if (NamedCommon.isGUI) return response;
        if (NamedCommon.ReturnCodes.size() == 0) uCommons.SetupReturnCodes();
        if (response.startsWith("{") || response.startsWith("<")) {
            response = uConnector.Format(response, esbFMT);
        }
        // -------------------------------------------------------------------------------
        // IF you want json full body response message status structure, edit the template
        //      NB: if you do, rFuel needs @variables like @status, etc...
        // -------------------------------------------------------------------------------
        String replyMessage = response;
        boolean GoDirect = false;
        if (descr.equals("DONOTALTER")) {
            GoDirect = true;
            descr = "";
        }

        if (descr.equals("")) {
            descr = NamedCommon.ReturnCodes.get(Integer.valueOf(status));
        }

        if (NamedCommon.debugging && !esbFMT.equals("")) uCommons.uSendMessage("   .) message requested response format [" + esbFMT + "]");

//        if (NamedCommon.StructuredResponse.contains(NamedCommon.task) || NamedCommon.isWebs) {
        if (NamedCommon.StructuredResponse.contains(NamedCommon.task)) {

            if (!response.equals("")) {
                if (response.startsWith("{") || response.startsWith("[")) {
                    if (NamedCommon.debugging) uCommons.uSendMessage("got a json object.");
                    if (replyMessage.startsWith("{\"body\":")) {
                        replyMessage = response;
                    } else {
//                        replyMessage = "{\"body\": {\"status\": " + status + ",\"message\": \"" + descr + "\",\"response\": " + response + "}}";
                        replyMessage = "{\"body\":{" + "\"response\":" + response + "," + "\"message\":\"" + descr + "\"," + "\"status\":" + status + "}}";
                    }
                } else {
                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) got an xml (non-json) object.");

                    if (replyMessage.startsWith(NamedCommon.xmlProlog)) {
                        replyMessage = replyMessage.substring(NamedCommon.xmlProlog.length(), replyMessage.length());
                        response = replyMessage;
                    } else {
                        if (replyMessage.startsWith("<?xml")) {
                            replyMessage = replyMessage.substring(replyMessage.indexOf("><")+1, replyMessage.length());
                            response = replyMessage;
                        }
                    }

                    if (replyMessage.toLowerCase().contains("<status>") && GoDirect) {
                        replyMessage = NamedCommon.xmlProlog + replyMessage;
                    } else {
                        if (esbFMT.toUpperCase().equals("JSON")) {
                            replyMessage = "{\"body\":{" + "\"response\":\"" + response + "\"," + "\"message\":\"" + descr + "\"," + "\"status\":" + status + "}}";
                        } else {
                            replyMessage = NamedCommon.xmlProlog + "<body><status>" + status + "</status>" + "<message>" + descr + "</message><response>" + response + "</response></body>";
                        }
                    }

                }
            } else {
                if (APImsg.APIget("format").toUpperCase().equals("JSON") || esbFMT.equals("JSON")) {
                    if (NamedCommon.debugging) uCommons.uSendMessage("got a json object.");
                    if (NamedCommon.debugging) uCommons.uSendMessage("Trying to handle a json ERR response: "+response);
                    replyMessage = "{\"body\":{" +
                            "\"response\":\"" + response + "\"," +
                            "\"message\":\"" + descr + "\"," +
                            "\"status\":" + status +
                            "}}";
                    // ------------------------------------------------------------------------------
//                    replyMessage = response;

                } else {
                    if (NamedCommon.debugging) uCommons.uSendMessage("got an xml (non-json) object.");
                    replyMessage = NamedCommon.xmlProlog + "<body><status>" + status +
                            "</status>" + "<message>" + descr + "</message><response>" +
                            response + "</response></body>";

                }
            }
        }
        if (replyMessage.equals("")) replyMessage = descr;
        replyMessage = uConnector.Format(replyMessage, esbFMT);
        NamedCommon.uStatus = status;
        return replyMessage;
    }

    public static String HandleCSVtoXML(String csvReply) {
        String[] csvList = String.valueOf(" " + csvReply).split(";");
        int nbrRows = csvList.length;
        String reply = NamedCommon.xmlProlog;
        reply = reply + "<list>\n";
        reply = reply + "</list>";
        return reply;
    }

    public static String CSVtoXML(String rfOut, ArrayList<String> template) {
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) CSVtoXML() ");
        String reply = "";
        if (domFactory == null) {
            try {
                domFactory = DocumentBuilderFactory.newInstance();
                //
                domFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                domFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                //
                domBuilder = domFactory.newDocumentBuilder();
            } catch (FactoryConfigurationError exp) {
                System.out.println(exp.toString());
                System.exit(0);
            } catch (ParserConfigurationException exp) {
                System.out.println(exp.toString());
                System.exit(0);
            } catch (Exception exp) {
                System.out.println(exp.toString());
                System.exit(0);
            }
        }

        heads.clear();
        items.clear();
        arrLineage.clear();
        csv.clear();
        dlineage.clear();
        elements.clear();
        names.clear();
        previous.clear();
        nodes.clear();
        nNames.clear();
        arrLineage = new ArrayList<String>(Arrays.asList(xmlLineage.split("\\r?\n")));
        csv = new ArrayList<String>(Arrays.asList(rfOut.split("\\r?\n")));

        if (csv.size() < 2) {
            NamedCommon.Zmessage = "CSVtoXML() end-of-line characters have been lost - cannot process ";
            NamedCommon.ZERROR = true;
            uCommons.uSendMessage("   .) CSVtoXML() ********************** ERROR **************************");
            uCommons.uSendMessage("   .) " +  NamedCommon.Zmessage);
            uCommons.uSendMessage("   .) CSVtoXML() *******************************************************");
            return "";
        }

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("   .) CSVtoXML().rfOut   = [" + rfOut + "]");
            uCommons.uSendMessage("   .) CSVtoXML().csvSize = [" + csv.size() + "]");
        }

        String[] lineZero = rfOut.split("\\r?\n");
        lineZero = lineZero[0].split(sep);
        int eoh = lineZero.length;

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("   .) CSVtoXML().lineZero = [built!]");
            uCommons.uSendMessage("   .) CSVtoXML().    Size = [" + lineZero.length+ "]");
        }

        String heading = "";

        for (int t = 0; t < eoh; t++) {
            heading = ScrubHeading(lineZero[t]);
            heads.add(heading);
            names.add(heading);
            nodes.add(0);
            previous.add("");
            nNames.add("");
        }
        lineZero = null;

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("   .) CSVtoXML().csvSize = ["+csv.size()+"]");
            if (csv.size() > 0) {
                uCommons.uSendMessage("   .) CSVtoXML().csv(0) = ["+csv.get(0)+"]");
                for (int it=1 ; it < csv.size() ; it++) {
                    uCommons.uSendMessage("   .) CSVtoXML().csv("+it+") = ["+csv.get(it)+"]");
                }
            }
        }

        int eot = csv.size();
        for (int i = 1; i < eot; i++) {
            items.add(csv.get(i));
            if (i < arrLineage.size()) {
                dlineage.add(arrLineage.get(i).replace("<tm>", sep));
            } else {
                dlineage.add("");
            }
        }

        if (NamedCommon.debugging) {
            uCommons.uSendMessage("   .) CSVtoXML().itemsSize = ["+items.size()+"]");
            if (items.size() > 0) {
                uCommons.uSendMessage("   .) CSVtoXML().items(0) = ["+items.get(0)+"]");
                for (int it=1 ; it < items.size() ; it++) {
                    uCommons.uSendMessage("   .) CSVtoXML().items("+it+") = ["+items.get(it)+"]");
                }
            }
        }

        int nbrItems = items.size();

        Element parentElement = null;
        Element childElement = null;
        Attribute childAttribute = null;

        int lvl = -1;
        int deepest = 0;
        boolean prevHead = false;
        boolean noRepeat;
        String value = "", lineage = "";
        String av = "", mv = "", sv = "";

        Document newDoc = domBuilder.newDocument();
        boolean debug = NamedCommon.debugging, rsFlag = false, iiFlag = false;
        iiCounter = 1;
        for (int i = 0; i < nbrItems; i++) {
            String[] iArr = (items.get(i) + sep + "eol").split(sep);
            String[] lArr = (dlineage.get(i) + sep + "eol").split(sep);
            if (NamedCommon.debugging) uCommons.uSendMessage("     .) CSVtoXML().datCtr " + (iArr.length - 1));
            if (NamedCommon.debugging) {
                uCommons.uSendMessage("     .) CSVtoXML().iArr(0) " + iArr[0]);
                for (int it=1 ; it < iArr.length; it++) {
                    uCommons.uSendMessage("     .) CSVtoXML().iArr("+it+") " + iArr[it]);
                }
            }
            int datCtr = iArr.length - 1;
            lvl = -1;
            for (int j = 0; j < datCtr; j++) {
                heading = heads.get(j);
                if (NamedCommon.debugging) uCommons.uSendMessage("       .) CSVtoXML().iArr("+j+") " + iArr[j]);
                rsFlag = false;
                iiFlag = false;
                if (j < iArr.length) {
                    value = iArr[j];
                    if (isJSON) value = escapeJson(value);
                    if (isXML && NamedCommon.escXML) value = escapeXml11(value);
                } else {
                    value = "";
                }

                if (j < lArr.length) { lineage = lArr[j]; } else { lineage = ""; }

                if (heading.startsWith("*") || heading.startsWith("#")) {
                    prevHead = true;
                    if (heading.startsWith("#")) {
                        rsFlag = true;
                        if (rsContainers.indexOf(heading) < 0) {
                            rsContainers.add(heading);
                            rsCounter = 1;
                        }
                    }
                    heading = heading.substring(1, heading.length());
                    lvl++;
                    if (nNames.indexOf(heading) > -1 && !rsFlag) continue;
                    if (lvl > deepest) deepest = lvl;
//                    if (!rsFlag) nNames.set(lvl, heading);
                    nNames.set(lvl, heading);
                    if (lvl == 0) {
                        if (debug) System.out.println("  add root " + "  @ [" + lvl + "]  " + heading);
                        try {
                            parentElement = newDoc.createElement(ScrubHeading(heading));
                        } catch (DOMException de) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "ERROR: your container name [" + heading + "] has an illegal character(s). Please fix and re-submit";
                            return "";
                        }

                        newDoc.appendChild(parentElement);
                        elements.add(lvl, parentElement);
                    } else {
                        if (debug) System.out.println("  add node " + "  @ [" + lvl + "]  " + heading);
                        try {
                            childElement = newDoc.createElement(ScrubHeading(heading));
                        } catch (DOMException de) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "ERROR: your container name [" + heading + "] has an illegal character(s). Please fix and re-submit";
                            return "";
                        }
                        if (rsFlag) {
                            childElement.setAttribute("id", String.valueOf(rsCounter));
                            rsCounter++;
                        }
                        parentElement = elements.get(lvl - 1);
                        parentElement.appendChild(childElement);
                        elements.add(lvl, childElement);
                    }
                    nodes.set(j, lvl);
                    if (!rsFlag) continue;
                    names.set(j, heading);
                }
                if (heading.startsWith("%")) {
                    iiFlag = true;
                    heading = heading.replace("%", "");
                    names.set(j, heading);
                }

                if (prevHead && j < 2) prevHead = false;
                if (i == 0) nodes.set(j, lvl);
                int posx = names.indexOf(heading);
                if (posx < 0) {
                    String ohShit = "here";
                    uCommons.uSendMessage("ERROR: " + heading + " not found in names array - skipping.");
                    continue;
                }
                lvl = nodes.get(posx);

                String chk = previous.get(posx);
                noRepeat = false;
                noRepeat = ((value.equals(chk)) && i > 0);      // Bug Fix
                noRepeat = (noRepeat && lvl <= deepest);
                if (noRepeat) continue;

                // -------------------------------------------------------------

                if (prevHead && i > 0) {
                    lvl = nodes.get(posx);
                    prevHead = false;
                    try {
                        childElement = newDoc.createElement(ScrubHeading(nNames.get(lvl)));
                    } catch (DOMException de) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "ERROR: your container name [" + heading + "] has an illegal character(s). Please fix and re-submit";
                        return "";
                    }
                    if (lvl == 0) {
                        for (int x = posx; x < previous.size(); x++) { previous.set(x, ""); }
                        continue;
                    } else {
                        parentElement = elements.get(lvl - 1);
                    }
                    parentElement.appendChild(childElement);
                    if (debug) System.out.println("**add  new " + "  @ [" + lvl + "]  " + parentElement.getTagName() + "." + nNames.get(lvl));
                    elements.set(lvl, childElement);
                    for (int x = posx; x < previous.size(); x++) { previous.set(x, ""); }
                }

                lvl = nodes.get(posx);
                if (debug) System.out.println(" container " + "  @ [" + lvl + "]  " + nNames.get(lvl) + "." + heading + " = " + value);

                if (!rsFlag) {
                    if (NamedCommon.showLineage && !lineage.equals("")) {
                        try {
                            childElement = newDoc.createElement(ScrubHeading(heading));
                        } catch (DOMException de) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "ERROR: your container name [" + heading + "] has an illegal character(s). Please fix and re-submit";
                            return "";
                        }
                        String[] lParts = lineage.split("\\_");
                        if (lParts.length == 3) {
                            av = lParts[0];
                            if (av.toUpperCase().equals("N")) av = "1";
                            mv = lParts[1];
                            if (mv.toUpperCase().equals("N")) mv = "1";
                            sv = lParts[2];
                            if (sv.toUpperCase().equals("N")) sv = "1";

                            childElement.setAttribute("AV", av);
                            childElement.setAttribute("MV", mv);
                            childElement.setAttribute("SV", sv);
                        } else {
                            childElement.setAttribute("Lineage", "Literal");
                            if (iiFlag) {
                                childElement.setAttribute("id", String.valueOf(iiCounter));
                                iiCounter++;
                            }
                        }
                        lParts = null;
                        childElement.appendChild(newDoc.createTextNode(value));
                        parentElement = elements.get(lvl);
                        parentElement.appendChild(childElement);
                    } else {
                        try {
                            childElement = newDoc.createElement(ScrubHeading(heading));
                        } catch (DOMException de) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "ERROR: your container name [" + heading + "] has an illegal character(s). Please fix and re-submit";
                            return "";
                        }
                        if (iiFlag) {
                            childElement.setAttribute("id", String.valueOf(iiCounter));
                            iiCounter++;
                        }
                        childElement.appendChild(newDoc.createTextNode(value));
                        parentElement = elements.get(lvl);
                        parentElement.appendChild(childElement);
                    }
                    previous.set(posx, value);
                }
            }
            iArr = null; lArr = null;
        }
        TransformerFactory tranFactory = TransformerFactory.newInstance();
        Transformer aTransformer = null;
        try {
            aTransformer = tranFactory.newTransformer();
        } catch (TransformerConfigurationException e) {
            e.printStackTrace();
        }

        aTransformer.setOutputProperty(OutputKeys.INDENT, "yes");
        aTransformer.setOutputProperty(OutputKeys.METHOD, "xml");
        aTransformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");

        Source src = new DOMSource(newDoc);
        reply = "<<FAIL>> ERROR in csv2xml transformation.";
        try {
            Writer output = new StringWriter();
            aTransformer.transform(new DOMSource(newDoc), new StreamResult(output));
            reply = output.toString();
        } catch (TransformerException e) {
            e.printStackTrace();
        }

        if (NamedCommon.debugging) uCommons.uSendMessage("       .) CSVtoXML().reply " + reply);
        return reply;
    }

    private static String ScrubHeading(String heading) {
        String oldstr, repstr;
        for (int i = 0; i < NamedCommon.escChars.length; i++) {
            oldstr = NamedCommon.escChars[i][0];
            repstr = NamedCommon.escChars[i][1];
            if (oldstr.equals("")) continue;
            if (oldstr.equals(repstr)) continue;
            while (heading.contains(oldstr)) { heading = heading.replace(oldstr, repstr); }
        }
        return heading;
    }

    public static String ShowAsXML(String[] inStr) {
        String XMLString = "";
        if (inStr[0].length() > 0) {
            String spc = "   ";
            XMLString = XMLString + spc + "<row>\n";
            String[] header;
            String[] values;
            header = inStr[0].split(",");
            values = inStr[1].split(",");
            int iCtr = 0;
            int nbrHdrs = header.length;
            int nbrVals = values.length;
            if (nbrHdrs != nbrVals) {
                uCommons.uSendMessage("Headers and Values are not consistent");
            } else {
                for (iCtr = 0; iCtr < nbrHdrs; iCtr++) {
                    String hdr = header[iCtr];
                    String val = values[iCtr];
                    XMLString = XMLString + spc + spc + "<" + hdr + ">" + val + "</" + hdr + ">\n";
                }
            }
            XMLString = XMLString + spc + "</row>\n";
            header = null;
            values = null;
        }
        return XMLString;
    }

    public static String Replacements(String inVal) {

        if (NamedCommon.ZERROR) return NamedCommon.Zmessage;
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().Replacements");
        backfilled = false;
        isJSON = false;
        isXML = false;
        isCSV = false;

        boolean SysErr = false;
        String tmpl, subs, valu, indx;
        String[] curDetails = {"", "", "", "", "", ""};
        ArrayList<String> ucIndex = new ArrayList<>();

        backfill = false;
        String tplName, tmpLine;
        if (NamedCommon.Templates.indexOf(NamedCommon.Template) < 0) {
            NamedCommon.DataList.add(0, "");
            NamedCommon.DataLineage.add(0, "");
            NamedCommon.SubsList.add(0, "");
            NamedCommon.TmplList.add(0, NamedCommon.Template);
            NamedCommon.Templates.add(0, NamedCommon.Template);
            NamedCommon.AsocList.add(0, false);
        }
        int nbrTPs = NamedCommon.Templates.size();
        for (int tt = 0; tt < nbrTPs; tt++) {
            tplName = NamedCommon.Templates.get(tt);
            lines = ReadTemplate(tplName);
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

        UniConstructor = new ArrayList<>();
        doneThese = new ArrayList<>();
        int nbrItems = NamedCommon.TmplList.size();
        int uPos, aRepeats, mRepeats, sRepeats;
        // ----------------------------------------------------
        // -------- Build the UniConstructor ------------------
        // ----------------------------------------------------
        for (int ix = 0; ix < nbrItems; ix++) {
            tmpl = NamedCommon.TmplList.get(ix);
            subs = NamedCommon.SubsList.get(ix).trim();
            if (!subs.equals("")) {
                valu = NamedCommon.DataList.get(ix);
                valu = valu.replace("\t", "<tab>");
                indx = tmpl + "\t" + subs;
                uPos = ucIndex.indexOf(indx);
                aRepeats = 0;mRepeats = 0;sRepeats = 0;
                if (uPos < 0) {
                    ucIndex.add(indx);
                    uPos = ucIndex.indexOf(indx);
                    if (uPos < 0) {
                        uCommons.uSendMessage(indx + " has not been built. Stopping now.");
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
            xmlLineage = "";
            String tmplName="", tempLine="", payload="";
            int nbrTemplates = NamedCommon.Templates.size();
            for (int tt = 0; tt < nbrTemplates; tt++) {
                tmplName = NamedCommon.Templates.get(tt);
                if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().Replacements.tmplName " + tmplName);
                lines = ReadTemplate(tmplName);

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
                        NamedCommon.Zmessage = "Error in (CSV) template ["+tmplName+"]: " + nChk.size() + " nodes do not match with " + eChk.size() + " elements";
                        NamedCommon.ZERROR = true;
                    }
                    nChk.clear();
                    eChk.clear();
                    if (NamedCommon.ZERROR) return NamedCommon.Zmessage;
                }

                // ----------- build payload for the template -----------
                payload = BuildPayLoad(lines, tmplName);
                outpt.add(payload);

                if (NamedCommon.debugging) {
                    uCommons.uSendMessage("--------------------------------------------------------------------------");
                    int l = 0;
                    while (l < lines.size()) {
                        if (NamedCommon.debugging) uCommons.uSendMessage(l + " >> " + lines.get(l));
                        l++;
                    }
                    uCommons.uSendMessage("payload: " + payload);
                    uCommons.uSendMessage("--------------------------------------------------------------------------");
                }

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
                                fPos = NamedCommon.Templates.indexOf(includeTmpl);
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

            if (isCSV) {
                if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().CSV-to-XML()");
                String xmlProlog = "";
                answer = "";
                sep = ",";          // default BUT cannot use with e.g. MD2,
                lines.clear();      // not used in CSVtoXML anymore
                boolean okay = false;
                int s;
                for (int o = 0; o < outpt.size(); o++) {
                    workFld = outpt.get(o);
                    while (workFld.startsWith("\n")) { workFld = workFld.substring(1, workFld.length()); }
                    s = 0;
                    while (s < trythese.length()) {
                        sep = trythese.substring(s, s + 1);
                        if (workFld.split(sep).length < 0) { s++; } else { okay = true; break; }
                    }
                    if (!okay) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "ERROR: Cannot find a string separator for the data!";
                        return NamedCommon.Zmessage;
                    }

                    workFld = CSVtoXML(workFld.replace("<tm>", sep), lines);

                    if (NamedCommon.ZERROR) return NamedCommon.Zmessage;
                    xmlProlog = workFld.split("\\r?\\n")[0];
                    workFld = workFld.substring(xmlProlog.length() + 1, workFld.length());
                    while (workFld.startsWith("\n")) {
                        workFld = workFld.substring(1, workFld.length());
                    }
                    answer += workFld;
                }
                answer = xmlProlog + answer;
            }
        }
        answer = ScrubData(answer, "\t");
        answer = ScrubData(answer, "\r");
        answer = ScrubData(answer, "\n");
        answer = ScrubData(answer, "\\ \\ ");
        answer = answer.trim();
        return answer;
    }

    private static String ScrubData(String valu, String chr) {
        valu = valu.replaceAll(chr, "");
        return valu;
    }

    private static String BuildPayLoad(ArrayList<String> lines, String tmplName) {
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
                // IF the import creates an array, place the [ and the ] in the driving template
//                payLoad = "[";
                if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().BuildPayload.repeatingVals()");
                if (NamedCommon.debugging) uCommons.uSendMessage("      .) DataConverter().BuildPayload.repeatingVals(BEFORE).payLoad - " + payLoad);
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
                        payLoad += thisLoad.trim();
//                        thisLoad = thisLoad.replaceAll("\\r?\\n", "");
                        if (!payLoad.endsWith(","))  payLoad +=  ",";
                        thisLoad = "";
                        if (NamedCommon.debugging) uCommons.uSendMessage("      .) DataConverter().BuildPayload.repeatingVals(AFTER).payLoad - " + payLoad);
                        // -------------------------------------------------------------------------
                    }
                }
                // IF the import creates an array, place the [ and the ] in the driving template
//                payLoad += "]";
                payLoad = payLoad.replace(",]", "]");
                tmpData=""; avData=""; mvData=""; svData=""; thisLine="";
                if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().BuildPayload.repeatingVals() - " + payLoad);
            } else {
                payLoad = thisLoad;
            }
            doneThese.add(tmpl);
        }
        if (payLoad.equals("") && doneThese.indexOf(tmplName) < 0) {
            // Probably a placeholder template for %import% template(s)
            for (int l=0; l < lines.size(); l++) { payLoad += lines.get(l) + "\n"; }
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().BuildPayload - " + payLoad);
        return payLoad;
    }

    private static ArrayList<String> ReadTemplate(String template) {
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().ReadTemplate - " + template);

        ArrayList<String> lines = new ArrayList<>();
        String tmpl = NamedCommon.BaseCamp + "/templates/" + template;
        try {
            String content = new String(Files.readAllBytes(Paths.get(tmpl)));
            lines = new ArrayList<>(Arrays.asList(content.split("\\r?\\n")));
        } catch (IOException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "[ABORT] Cannot find " + tmpl;
            uCommons.uSendMessage(NamedCommon.Zmessage);
            lines.add("ERROR!!!");
        }
        return lines;
    }

    private static String Substitutions(ArrayList<String> lines, String thisTemplate, int repeat) {
        // thisTemplate - the template from the TemplList   //
        // lines        - array of lines from thisTemplate  //
        backfill=false;
        if (lines.get(0).trim().toLowerCase().equals("backfill")) {
            backfill=true;
            lines.remove(0);
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().Substitutions.thisTemplate - " + thisTemplate);
        String xmlLine = "", theTag = "", datum = "", xmlRequest = "", sPart = "", lineage = "";
        lineTags.clear();
        lineVals.clear();
        lineTemp.clear();
        lineLocn.clear();
        boolean splitData, assocFLAG = false;
        String prevSVline;

        int maxA = 0, maxM = 0, maxS = 0;
        int assocCnt = 1;
        int nbrLines = lines.size(), fPos = 0;
        for (int ll = 0; ll < nbrLines; ll++) {
            xmlLine = lines.get(ll);
            if (!xmlLine.contains("$")) {
                xmlRequest += xmlLine + "\r\n";
                xmlLineage += "\r\n";
            } else {
                while (xmlLine.contains("$")) {
                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().xmlLine " + xmlLine);
                    lineTags.clear();
                    lineVals.clear();
                    lineTemp.clear();
                    lineLocn.clear();

                    String holdLine = xmlLine;

                    int nbrTags = 0;
                    if (!isCSV) {
                        int[] dPos = new int[500];
                        for (int d = 0; d < dPos.length; d++) { dPos[d] = 0; }
                        int dpIdx = 0;
                        boolean isFirst = true;
                        String dataGroup = "";
                        while (holdLine.contains("$")) {
                            theTag = uCommons.FieldOf(holdLine, "\\$", 2);
                            if (theTag.equals("")) continue;
                            theTag = "$" + theTag + "$";
                            fPos = NamedCommon.SubsList.indexOf(theTag);
                            if (fPos  < 0) {
                                holdLine = "";
                            } else {
                                datum = NamedCommon.DataList.get(fPos);
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
                            String[] dataResult = dataGroup.split("\\<fm\\>");
                            for (int d = 0; d < nbrTags; d++) {
                                fPos = dPos[d];
                                dataGroup = dataResult[d];
                                NamedCommon.DataList.set(fPos, dataGroup);
                            }
                            dataResult = null;
                        }
                        dPos = null;
                        dataGroup = "";
                        holdLine = xmlLine;
                    } else {
//                        nbrTags = StringUtils.countOccurrencesOf(xmlLine, "$");
                        nbrTags = uCommons.NumberOf(xmlLine, "$");
                        nbrTags = nbrTags / 2;
                    }
                    int nbrReplacements = 0, nbrAVs = 0, nbrMVs = 0, nbrSVs = 0, tNbr = 0, rChk = 0;
                    int parcelCount=0, tagCount=0;
                    // -----------------------------------------------------------------------------
                    // Loop for each tag in THIS LINE only !!!
                    // -----------------------------------------------------------------------------
                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().Substitutions ---- Collect data for each tag\n\n");
                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().nbrTags " + nbrTags);
                    for (int tag = 0; tag < nbrTags; tag++) {
                        theTag = uCommons.FieldOf(holdLine, "\\$", 2);
                        if (theTag.equals("")) continue;    // a repeat of a tag previously encountered - such as assocNbr
                        theTag = "$" + theTag + "$";
                        if (NamedCommon.debugging) uCommons.uSendMessage("      .) DataConverter().theTag " + theTag);
                        assocFLAG = false;

                        if (backfill) {
                            for (int lll = 0; lll < nbrLines; lll++) {
                                if (lines.get(lll).contains(theTag)) Backfiller(theTag);
                            }
                        }

                        holdLine = holdLine.replace(theTag, "@@@");
                        boolean found = false;
                        boolean exitSW = false;
                        NamedCommon.isAssoc = false;
                        while (!exitSW) {
                            fPos = NamedCommon.SubsList.indexOf(theTag);
                            if (fPos >= 0) {
                                if (NamedCommon.TmplList.get(fPos).equals(thisTemplate)) {
                                    datum = NamedCommon.DataList.get(fPos);
                                    NamedCommon.isAssoc = NamedCommon.AsocList.get(fPos);
                                    lineage = NamedCommon.DataLineage.get(fPos);
                                    found = true;
                                    exitSW = true;
                                } else {
                                    int nbrItems = NamedCommon.SubsList.size();
                                    fPos++;
                                    datum = "[" + theTag + "] is not mapped";
                                    lineage = "Error";
                                    for (int ii = fPos; ii < nbrItems; ii++) {
                                        if (NamedCommon.SubsList.get(ii).equals(theTag)) {
                                            if (NamedCommon.TmplList.get(ii).equals(thisTemplate)) {
                                                datum = NamedCommon.DataList.get(ii);
                                                NamedCommon.isAssoc = NamedCommon.AsocList.get(ii);
                                                lineage = NamedCommon.DataLineage.get(ii);
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
                                            uCommons.eMessage = "ERROR:  '=assoc' recordset indicators are $assocTotal$ or $assocNbr$. ";
                                            uCommons.eMessage += "You have [" + theTag + "] - please correct and re-run";
                                            uCommons.uSendMessage(uCommons.eMessage);
                                    }
                                } else {
                                    datum = "[" + theTag + "] is not mapped";
                                    lineage = "Error";
                                }
                                exitSW = true;
                            }
                        }
                        if (NamedCommon.debugging) uCommons.uSendMessage("      .) DataConverter().(1)datum " + datum);

                        if (!found) {
                            uCommons.uSendMessage("DataConverter error: " + theTag + " from the csv was not found in the template");
                            continue;
                        }
                        if (datum.toLowerCase().equals("!x!")) datum = "";

//                        sPart = datum;
//                        while (sPart.indexOf(NamedCommon.FMark + NamedCommon.FMark) > -1) {
//                            sPart = sPart.replace(NamedCommon.FMark + NamedCommon.FMark, NamedCommon.FMark + " " + NamedCommon.FMark);
//                        }
//                        while (sPart.indexOf(NamedCommon.VMark + NamedCommon.VMark) > -1) {
//                            sPart = sPart.replace(NamedCommon.VMark + NamedCommon.VMark, NamedCommon.VMark + " " + NamedCommon.VMark);
//                        }
//                        while (sPart.indexOf(NamedCommon.SMark + NamedCommon.SMark) > -1) {
//                            sPart = sPart.replace(NamedCommon.SMark + NamedCommon.SMark, NamedCommon.SMark + " " + NamedCommon.SMark);
//                        }
//                        datum = sPart;

                        lineTags.add(theTag);
                        lineVals.add(datum);
                        lineTemp.add(xmlLine);
                        lineLocn.add(lineage);
                        holdTags.add(theTag);
                        holdVals.add(datum);
                        if (NamedCommon.debugging) uCommons.uSendMessage("      .) DataConverter().(2)datum " + datum);
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
                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().Substitutions ---- Handle Split n values\n\n");
                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().tNbr " + tNbr);
                    for (int yy = 0; yy < tNbr; yy++) {
                        int lnCtr = 0;
                        String tmpxml = "";

                        theTag = lineTags.get(yy);
                        xmlLine = lineTemp.get(yy);
                        datum = lineVals.get(yy);
                        lineage = lineLocn.get(yy);
                        if (amsLines[lnCtr].equals("")) amsLines[lnCtr] = xmlLine;

                        if (!thisTemplate.equals(NamedCommon.Template)) {
                            // this is for normal templates as well as %import% templates
                            splitData = false;
                        } else {
                            // this is for csv templates - try to make sure these are not used anymore
                            splitData = (lineage.toUpperCase().contains("N"));
                            splitData = (splitData || theTag.equals("$assocNbr$"));
                            splitData = (splitData && !backfilled);
                            splitData = (splitData && !datum.contains(NamedCommon.FMark));
                        }

                        if (NamedCommon.debugging) uCommons.uSendMessage("      .) DataConverter().splitData =   " + lineage.toUpperCase().contains("N") + " || " + theTag.equals("$assocNbr$") + "  && !"  + backfilled);
                        if (NamedCommon.debugging) uCommons.uSendMessage("      .) DataConverter().theTag   " + theTag + "  ==>  " + datum + "  splitting is "  + splitData);

                        done = false; // doneA = false; doneM = false; doneS = false;
                        if (!splitData) {
                            if (NamedCommon.showLineage) {
                                if ((lineage.length() - 2) == lineage.replaceAll("\\-", "").length()) {
                                    String[] lparts = lineage.split("\\-");
                                    if (lparts[0].toUpperCase().equals("N")) lparts[0] = "1";
                                    if (lparts[1].toUpperCase().equals("N")) lparts[1] = "1";
                                    if (lparts[2].toUpperCase().equals("N")) lparts[2] = "1";
                                    if (lparts[0].equals("0")) {
                                        lparts[1] = "0";
                                        lparts[2] = "";
                                    }
                                    if (!isCSV) {
                                        thisPart = "<value " +
                                                "AV=\"" + lparts[0] + "\" " +
                                                "MV=\"" + lparts[1] + "\" " +
                                                "SV=\"" + lparts[2] + "\"" +
                                                ">" + datum + "</value>";
                                        datum = thisPart;
                                    }
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
                                        if (isXML && NamedCommon.escXML) escTemp = escapeXml11(escTemp);
                                        if (!escTemp.equals(escSave)) datum = u2Commons.sReplace(datum, repA, repM, repS, escTemp);
                                    }
                                }
                            }

                            // --------------------------------------------------------------------------------------------
                            if (outLines[lnCtr].equals(""))  outLines[lnCtr] = xmlLine;
                            outLines[lnCtr] = outLines[lnCtr].replace(theTag, datum);
                            amsLines[lnCtr] = amsLines[lnCtr].replace(theTag, lineage.replace("-", "_"));
                            if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().replacement-1 " + theTag + " --> " + datum);
                            int l = lnCtr + 1;
                            while (!outLines[l].equals("")) {
                                outLines[l] = outLines[l].replace(theTag, datum);
                                amsLines[l] = amsLines[l].replace(theTag, lineage.replace("-", "_"));
                                if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().replacement-2 " + theTag + " --> " + datum);
                                l++;
                            }
                            // --------------------------------------------------------------------------------------------

                        } else {
                            int AVcnt = u2Commons.sDcount(datum, "A");
                            if (NamedCommon.isAssoc && maxA > AVcnt) AVcnt = maxA;
                            for (int av = 1; av <= AVcnt; av++) {
                                String saveLineage = amsLines[lnCtr];
                                prevSVline = "";
                                thisPart = u2Commons.sExtract(datum, av, 0, 0);
                                int MVcnt = u2Commons.sDcount(thisPart, "M");
                                if (MVcnt > maxM) maxM = MVcnt;
                                if (NamedCommon.isAssoc && maxM > MVcnt) MVcnt = maxM;
                                for (int mv = 1; mv <= MVcnt; mv++) {
                                    String saveLine = "";
                                    if (prevSVline.equals("")) prevSVline = outLines[lnCtr];
                                    if (prevSVline.equals("") || assocFLAG) prevSVline = xmlLine;
                                    thisPart = u2Commons.sExtract(datum, av, mv, 0);
                                    int SVcnt = u2Commons.sDcount(thisPart, "S");
                                    if (SVcnt > maxS) maxS = SVcnt;
                                    if (NamedCommon.isAssoc && maxS > SVcnt) SVcnt = maxS;
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
                                        if (NamedCommon.showLineage && !lineage.equals("Error")) {
                                            String[] lparts = lineage.split("\\-");
                                            if (lparts[0].toUpperCase().contains("N")) lparts[0] = String.valueOf(av);
                                            if (lparts[1].toUpperCase().contains("N")) lparts[1] = String.valueOf(mv);
                                            if (lparts[2].toUpperCase().contains("N")) lparts[2] = String.valueOf(sv);
                                            if (lparts[0].equals("0")) {
                                                lparts[1] = "0";
                                                lparts[2] = "";
                                            }
                                            thisLineage = lparts[0] + "_" + lparts[1] + "_" + lparts[2];
                                            if (!isCSV) {
                                                thisPart = "<value " +
                                                        "AV=\"" + lparts[0] + "\" " +
                                                        "MV=\"" + lparts[1] + "\" " +
                                                        "SV=\"" + lparts[2] + "\"" +
                                                        ">" + thisPart + "</value>";
                                            }
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
                                        if (isXML && NamedCommon.escXML) thisPart = escapeXml11(thisPart);

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

                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().Substitutions ---- Handle repeating values <vm> <sm>\n\n");
                    if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().nbrReplacements " + nbrReplacements);
                    if (outLines.length > 0) {
                        for (int yy = 0; yy <= outLines.length; yy++) {
                            holdLine = outLines[yy];
                            if (holdLine == "") break;
                            if (NamedCommon.debugging)
                                uCommons.uSendMessage("   .) DataConverter().holdLine " + holdLine);
                            if (sep.equals("")) sep = ",";
                            if (isCSV) {
                                holdItems = holdLine.split(sep);
                            } else {
                                holdItems = holdLine.split("<tm>");
                            }
                            boolean chkIt = false;
                            for (int h = 0; h < holdItems.length; h++) {
                                holdItem = holdItems[h];
                                chkIt = (holdItem.startsWith("$") && holdItem.endsWith("$"));
                                if (!chkIt) {
                                    if (NamedCommon.debugging)
                                        uCommons.uSendMessage("      .) DataConverter().holdItem [  OK ] " + holdItem);
                                } else {
                                    if (NamedCommon.debugging)
                                        uCommons.uSendMessage("      .) DataConverter().holdItem [ MOD ] " + holdItem + "  set to \"\"");
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
                                    tpos = NamedCommon.SubsList.indexOf(holdItem);
                                    if (tpos < 0) {
                                        datum = "";
                                    } else {
                                        datum = NamedCommon.DataList.get(tpos);
                                    }
                                    holdLine = holdLine.replace(holdItem, datum);
                                    if (NamedCommon.debugging)
                                        uCommons.uSendMessage("      .) DataConverter().(b)holdItem " + holdItem);
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
        if (xmlRequest.contains("DEF(")) xmlRequest = DefaultsHandler(xmlRequest);
        if (xmlRequest.contains("FRQ(")) xmlRequest = FrequencyHandler(xmlRequest);
        if (xmlRequest.contains("SUM(")) xmlRequest = SumHandler(xmlRequest);
        if (NamedCommon.debugging) uCommons.uSendMessage("   .) DataConverter().Substitutions.xmlRequest - \n" + xmlRequest);
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

    private static void Backfiller (String thisTag) {
        if (NamedCommon.debugging) uCommons.uSendMessage("      >> Backfiller("+thisTag+")");
        int pos = NamedCommon.SubsList.indexOf(thisTag);
        if (pos < 0) return;
        String pVal = "", aVal = "", mVal = "";
        int maxAV = 0, maxMV = 0, maxSV = 0;
        int posAV = 0, posMV = 0, posSV = 0;
        //
        // find the maximum nbr attributes in DataList first !
        //
        for (int posx = 0; posx < NamedCommon.DataList.size(); posx++) {
            pVal = NamedCommon.DataList.get(posx);
            posAV = u2Commons.sDcount(pVal, "A");
            if (posAV > maxAV) maxAV = posAV;
        }
        // -------------------------------------------------------------------
        UniDynArray tmpDyn = new UniDynArray();
        String datum;
        String nVal = "", lastKnownValue;
        for (int aa = 1; aa <= maxAV; aa++) {
            for (int posx = 0; posx < NamedCommon.DataList.size(); posx++) {
                pVal = NamedCommon.DataList.get(posx);
                aVal = u2Commons.sExtract(pVal, aa, 0, 0);
                posMV = u2Commons.sDcount(aVal, "M");
                if (posMV == 0) posMV = 1;
                if (posMV > maxMV) maxMV = posMV;
                for (int mm = 1; mm <= posMV; mm++) {
                    mVal = u2Commons.sExtract(aVal, aa, mm, 0);
                    posSV = u2Commons.sDcount(mVal, "S");
                    if (posSV == 0) posSV = 1;
                    if (posSV > maxSV) maxSV = posSV;
                }
            }
            //
            // Backfill for the max mv and sv in this attribute.
            //
            pVal = NamedCommon.DataList.get(pos);
            lastKnownValue="";
            for (int mm = 1; mm <= maxMV; mm++) {
                for (int ss = 1; ss <= maxSV; ss++) {
                    datum = u2Commons.sExtract(pVal, aa, mm, ss);
                    if (datum.equals("")) {
                        datum = lastKnownValue;
                        if (datum.equals("")) datum = " ";
                    }
                    if (!lastKnownValue.equals(datum)) lastKnownValue = datum;
                    tmpDyn.insert(aa, mm, ss, datum);
                }
            }
        }
        nVal = uCommons.UV2SQLRec(NamedCommon.uID, tmpDyn);
        nVal = uCommons.FieldOf(nVal, NamedCommon.IMark, 2);
        NamedCommon.DataList.set(pos, nVal);
        pVal = "";
        aVal = "";
        mVal = "";
        lastKnownValue = "";
        tmpDyn = null;
        datum  = "";
        nVal   = "";
    }

    private static String DefaultsHandler(String inString) {
        String outLines = "", thisLine = "", tmp1 = "", tmp2 = "", tmp3 = "", chkv = "";
        ArrayList<String> lines = new ArrayList<String>(Arrays.asList(inString.split("\\r?\\n")));
        int nbrLines = lines.size(), brkt = 0;
        for (int ln = 0; ln < nbrLines; ln++) {
            thisLine = lines.get(ln);
            if (thisLine.contains("DEF(")) {
                tmp1 = uCommons.FieldOf(thisLine, "DEF\\(", 1);
                tmp2 = uCommons.FieldOf(thisLine, "DEF\\(", 2);
                brkt = tmp2.indexOf(")") + 1;
                tmp3 = tmp2.substring(brkt, tmp2.length());
                chkv = tmp2.substring(0, (brkt - 1));

                if (chkv.substring(0, 1).equals(" ")) {
                    chkv = chkv.trim();
                } else {
                    chkv = chkv.split(" ")[0];
                }
                thisLine = tmp1 + chkv + tmp3;
            }
            if (thisLine.trim().length() > 0) outLines += thisLine + "\r\n";
        }
        return outLines;
    }

    private static String FrequencyHandler(String inString) {
        String tmp = "", tmp1 = "", tmp2 = "", tmp3 = "", lineOut = "", dtd = "";
        String id = "", freq = "", make = "", value = "", ssum = "";
        boolean sum;
        Double calcVal;
        int gPos = 0;
        tmp = inString;
        ArrayList<String> frqGroup = new ArrayList<String>();
        frqGroup.clear();
        ArrayList<Double> frqTotals = new ArrayList<Double>();
        frqTotals.clear();
        ArrayList<String> lines = new ArrayList<String>(Arrays.asList(inString.split("\\r?\\n")));
        Properties p;
        int nbrLines = lines.size();
        String thisLine = "", outLine = "", outLineA = "", outLineB = "";
        for (int ln = 0; ln < nbrLines; ln++) {
            thisLine = lines.get(ln);
            if (thisLine.contains("FRQ(") && thisLine.toUpperCase().contains(("FREQ="))) {
                tmp1 = uCommons.FieldOf(thisLine, "FRQ\\(", 2);
                tmp2 = uCommons.FieldOf(tmp1, "\\)", 1);

                p = uCommons.DecodeMessage(tmp2);
                id = p.getProperty("ID", "zUPL");
                freq = p.getProperty("FREQ", "-").toUpperCase();
                make = p.getProperty("MAKE", "-").toUpperCase();
                value = p.getProperty("VALUE", "0");
                dtd = p.getProperty("DTD", "String");
                ssum = p.getProperty("SUM", "false");
                sum = ssum.equals("true");
                // 1. make everything Annual
                int mult = 1;
                switch (freq) {
                    case "W":
                        mult = 52;
                        break;
                    case "F":
                        mult = 26;
                        break;
                    case "4":
                        mult = 13;
                        break;
                    case "M":
                        mult = 12;
                        break;
                    case "Q":
                        mult = 4;
                        break;
                    case "H":
                        mult = 2;
                        break;
                    case "A":
                        mult = 1;
                        break;
                    case "Y":
                        mult = 1;
                        break;
                    default:
                        mult = 0;
                        break;
                }
                calcVal = Double.valueOf(value);
                calcVal = calcVal * mult;
                // 2. bring everything back to a base of "make"
                int div = 1;
                switch (make) {
                    case "W":
                        div = 52;
                        break;
                    case "F":
                        div = 26;
                        break;
                    case "4":
                        div = 13;
                        break;
                    case "M":
                        div = 12;
                        break;
                    case "Q":
                        div = 4;
                        break;
                    case "H":
                        div = 2;
                        break;
                    case "A":
                        div = 1;
                        break;
                    case "Y":
                        div = 1;
                        break;
                    default:
                        div = 0;
                        break;
                }
                calcVal = calcVal / div;

                if (!sum) {
                    tmp1 = uCommons.FieldOf(thisLine, "FRQ\\(", 2);
                    tmp2 = uCommons.FieldOf(tmp1, "\\)", 2);
                    tmp1 = uCommons.FieldOf(thisLine, "FRQ\\(", 1);
                    outLineA = tmp1.replace("FRQ(", "");
                    outLineB = tmp2.substring(1, tmp2.length());
                    outLine = outLineA + calcVal + outLineB;
                    thisLine = outLine;
                } else {
                    gPos = frqGroup.indexOf(id);
                    if (gPos < 0) {
                        frqGroup.add(id);
                        frqTotals.add(0.00);
                        gPos = frqGroup.indexOf(id);
                    }
                    double dblVal = frqTotals.get(gPos) + calcVal;
                    frqTotals.set(gPos, dblVal);
                    thisLine = "";
                }
            }
            if (thisLine.contains("FRQ(") && thisLine.toUpperCase().contains(("=SHOW"))) {
                String newLine = "";
                tmp1 = uCommons.FieldOf(thisLine, "FRQ\\(", 1);
                tmp2 = uCommons.FieldOf(thisLine, "FRQ\\(", 2);
                tmp3 = uCommons.FieldOf(tmp2, "\\)", 1);
                p = uCommons.DecodeMessage(tmp3);
                id = p.getProperty("ID", "zUPL");
                gPos = frqGroup.indexOf(id);
                if (gPos < 0) {
                    lineOut = "ABORT - ID[\"+id+\"] is missing. \"+thisLine";
                    uCommons.uSendMessage(lineOut);
                }
                tmp3 = uCommons.FieldOf(tmp2, "\\)", 2);
                double dblVal = frqTotals.get(gPos);
                String cnv = "";
                switch (dtd) {
                    case "String":
                        cnv = String.valueOf(dblVal);
                        break;
                    case "int":
                        cnv = String.valueOf(dblVal).split("\\.")[0];
                        break;
                    default:
                        cnv = String.valueOf(dblVal);
                }
                newLine = tmp1 + cnv + tmp3;
                thisLine = newLine;
            }
            if (thisLine.trim().length() > 0) {
                if (!thisLine.trim().equals("null")) {
                    lineOut += thisLine + "\r\n";
                }
            }
        }
        return lineOut;
    }

    private static String SumHandler(String inString) {
        String outLines = "", thisLine = "", tmp1 = "", tmp2 = "", tmp3 = "";
        String id = "", value = "";
        ArrayList<String> frqGroup = new ArrayList<String>();
        frqGroup.clear();
        ArrayList<Double> frqTotals = new ArrayList<Double>();
        frqTotals.clear();
        ArrayList<String> lines = new ArrayList<String>(Arrays.asList(inString.split("\\r?\\n")));
        Double calcVal;
        int gPos = 0;
        Properties p;
        int nbrLines = lines.size();
        for (int ln = 0; ln < nbrLines; ln++) {
            thisLine = lines.get(ln);
            if (thisLine.contains("SUM(") && !thisLine.toUpperCase().contains(("=SHOW"))) {
                tmp1 = uCommons.FieldOf(thisLine, "SUM\\(", 2);
                tmp2 = uCommons.FieldOf(tmp1, "\\)", 1);
                p = uCommons.DecodeMessage(tmp2);
                id = p.getProperty("ID", "zUPL");
                value = p.getProperty("VALUE", "0");
                gPos = frqGroup.indexOf(id);
                if (gPos < 0) {
                    frqGroup.add(id);
                    frqTotals.add(0.00);
                    gPos = frqGroup.indexOf(id);
                }
                double dblVal = frqTotals.get(gPos) + Double.valueOf(value);
                frqTotals.set(gPos, dblVal);
                thisLine = "";
            }
            if (thisLine.contains("SUM(") && thisLine.toUpperCase().contains(("=SHOW"))) {
                String newLine = "";
                tmp1 = uCommons.FieldOf(thisLine, "SUM\\(", 1);
                tmp2 = uCommons.FieldOf(thisLine, "SUM\\(", 2);
                tmp3 = uCommons.FieldOf(tmp2, "\\)", 1);
                p = uCommons.DecodeMessage(tmp3);
                id = p.getProperty("ID", "zUPL");
                gPos = frqGroup.indexOf(id);
                if (gPos < 0) {
                    outLines = "ABORT - ID[" + id + "] is missing. " + thisLine;
                    uCommons.uSendMessage(outLines);
                }
                tmp3 = uCommons.FieldOf(tmp2, "\\)", 2);
                double dblVal = frqTotals.get(gPos);
                newLine = tmp1 + dblVal + tmp3;
                thisLine = newLine;
            }
            if (thisLine.trim().length() > 0) outLines += thisLine + "\r\n";
        }
        return outLines;
    }

}
