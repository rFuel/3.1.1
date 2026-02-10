package com.unilibre.core;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniobjects.*;
import com.unilibre.commons.*;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.w3c.dom.*;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class microservice {

    // Invoked by a ThreadManager                                               //
    // -------------------------------------------------------------------------//
    //      Converts payload from Json to Xml                                   //
    //      Then unpacks the message into ArrayList of containers and values    //
    //      Then unpacks microsvc into ArrayList of tag-names and subrtns       //
    //      Then matches tag-names to containers - creating a Data Pool         //
    //      Then calls subrtns in order of appearance                           //
    //       :: Returns any data changed by the subrtn back into Data Pool      //
    //       :: Subroutines begin with an S                                     //
    //       :: uRequests begin with a 'c' create or 'u' update                 //
    //          :: uRequests are dumped to a dynamic file called uREQUEST       //
    //          :: a U2 phantom called uHARNESS manages these requests          //
    //          :: this procss responds with a 200 OK ... always                //
    //          :: uRequests do not return values to the Data Pool              //
    // ---------------------------------------------------------------------------

    // public static boolean multiSW = false;
    // public static UniFile uRequests = null;
    private static ArrayList<String> srtns;
    private static ArrayList<String> sArgs;
    private static ArrayList<String> npool;
    private static ArrayList<String> containers;
    private static ArrayList<String> values;
    public static ArrayList<ArrayList<String>> valuesV2;
    private static ArrayList<String> vpool;
    private static ArrayList<String> mpool;
    private static ArrayList<ArrayList<String>> dpool;
    private static ArrayList<String> microsvc;
    private static ArrayList<String> tempList;
    private static ArrayList<String> constants;
    private static ArrayList<String> litValues;
    //    private static String u2File;
    private static String parent;
    private static String ReturnString;
    private static String tname;
    private static String inTask;
    private static String retVal;
    private static String plf;
    private static String mscDir;
    private static String mscat;
    private static String payload = "";
    private static String tName;
    private static String esbFMT;
    private static String realCstrings;
    private static String pid = NamedCommon.pid;
    private static int nextLIT = 0;
    private static int writeCNT = 0;
    private static int ver = 1;
    private static boolean restart = true;
    private static boolean reqFile = false;
    private static BufferedWriter mntRequests = null;
    private static long startAction = 0;
    private static long finishAction = 0;
    private static double laps = 0;
    private static double div = 1000000000.00;


    public static String RESTput(String[] decoder) {

        realCstrings = "";
        NamedCommon.sentU2 = false;
        inTask = decoder[0];
        mscat = decoder[4];
        payload = decoder[5];
        tname = decoder[6];
        esbFMT = decoder[7];
        mscDir = decoder[8];
        plf = decoder[9];
        retVal = "";

        if (tname.equals(NamedCommon.task)) {
            tName = mscat;
        } else {
            tName = tname;
        }

        srtns = new ArrayList<>();
        sArgs = new ArrayList<>();

        npool = new ArrayList<String>();
        constants = new ArrayList<String>();
        values = new ArrayList<String>();
        vpool = new ArrayList<String>();
        mpool = new ArrayList<String>();
        dpool = new ArrayList<ArrayList<String>>();

        tempList = new ArrayList<>();
        containers = new ArrayList<String>();
        litValues = new ArrayList<String>();
        ReturnString = "";

        boolean okay = true;
        if (!NamedCommon.sConnected && NamedCommon.InMount.equals("")) {
            if (!SourceDB.ConnectSourceDB().contains("<<PASS>>")) {
                okay = false;
                NamedCommon.ZERROR = true;
            }
        }

        if (!NamedCommon.InMount.equals("")) uCommons.uSendMessage("Shared mount present.");
        if (NamedCommon.InMount.equals("") && NamedCommon.protocol.equals("u2cs")) {
            if (NamedCommon.uRequests == null) {
                uCommons.uSendMessage("   >. Open uREQUESTS");
                if (!OpenRequests()) return "";
            }
            reqFile = true;
        }

        // Version 2 of Writer function
        String tmp=APImsg.APIget("version");
        if (!tmp.equals("")) ver = Integer.parseInt(tmp);

        if (okay) {
            if (!NamedCommon.ZERROR) {
                if (NamedCommon.debugging) uCommons.uSendMessage("   >. UnpackPayload()");
                UnpackPayload();
            }
            if (!NamedCommon.ZERROR) {
                if (NamedCommon.debugging) uCommons.uSendMessage("   >. UnpackMicroservice()");
                UnpackMicroservice();
            }
            if (!NamedCommon.ZERROR) {
                if (NamedCommon.debugging) uCommons.uSendMessage("   >. MatchNodesToValues()");
                MatchNodesToValues();
            }
            if (!NamedCommon.ZERROR) {
                if (NamedCommon.debugging) uCommons.uSendMessage("   >. MpoolSubstitutions()");
                ApplyMpoolSubstitutions();
            }
            if (!NamedCommon.ZERROR) {
                if (NamedCommon.debugging) uCommons.uSendMessage("   >. InteractU2Host()");
                InteractU2Host();
            }
            if (NamedCommon.ZERROR) {
                uCommons.uSendMessage("***");
                uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
                uCommons.uSendMessage("***");
            }
        }

        if (NamedCommon.debugging) uCommons.uSendMessage("microservice.finished(*)");
        if (!NamedCommon.ZERROR && !ReturnString.equals("") && NamedCommon.replyReqd) {
            int vPos = vpool.indexOf(ReturnString);
            retVal = dpool.get(vPos).get(0);
        } else {
            if (!NamedCommon.isWebs || NamedCommon.ZERROR) retVal = NamedCommon.Zmessage;
        }

        if (!retVal.equals("") && !NamedCommon.ZERROR && NamedCommon.debugging)
            uCommons.uSendMessage("Returning [" + retVal + "] to consumer");
        return retVal;
    }

    private static void ApplyMpoolSubstitutions() {
        String[] mapParts;
        String mapLine, aPart, mPart, sPart, newLine;

        int nbrItems = mpool.size(), repls = 0;
        for (int m = 0; m < nbrItems; m++) {
            mapLine = mpool.get(m);
            if (mapLine.length() < 1) continue;
            if (!mapLine.contains("$")) continue;
            mapParts = mapLine.split("\\_");
            if (mapParts.length < 4) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "Error in " + mapLine + " the data mapping is not complete.";
                return;
            }
            aPart = mapParts[2];
            mPart = mapParts[3];
            sPart = mapParts[4];

            mapParts[2] = SubsCheck(mapLine, aPart);
            mapParts[3] = SubsCheck(mapLine, mPart);
            mapParts[4] = SubsCheck(mapLine, sPart);

            newLine = mapParts[0];
            for (int t = 1; t < mapParts.length; t++) { newLine += "_" + mapParts[t]; }
            if (!mapLine.equals(newLine)) {
                mpool.set(m, newLine);
                repls++;
            }
        }

    }

    private static String SubsCheck(String mapLine, String aPart) {
        String ans = "";
        int nbrChck, vPos = -1;
        try {
            nbrChck = Integer.valueOf(aPart);
        } catch (NumberFormatException nfe) {
            vPos = vpool.indexOf(aPart);
            if (vPos < 0) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "Micro-Service [" + mscat + "] Invalid mapping  - \"" + mapLine +
                        "\"   " + aPart + " MUST exist in the payload.";
                return aPart;
            } else {
                try {
                    nbrChck = Integer.valueOf(dpool.get(vPos).get(0));
                    aPart = dpool.get(vPos).get(0);
                } catch (NumberFormatException nfe2) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Micro-Service [" + mscat + "] Variable  - " + aPart + " is non-numeric.";
                }
            }
        }
        return aPart;
    }

    private static void UnpackPayload() {
        String xml = "";
        switch (plf.toUpperCase()) {
            case "JSON":
                xml = json2xml(payload, "payload");
                break;
            case "XML":
                xml = payload;
                break;
            default:
                xml = "";
        }

        containers = new ArrayList<>();
        values = new ArrayList<String>();
        if (!xml.equals("")) {
            Document doc = null;
            try {
                DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
                //
                docFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl",true);
                docFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd",false);
                //
                DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
                InputSource xis = new InputSource(new StringReader(xml));
                doc = docBuilder.parse(xis);
            } catch (Exception e) {
                NamedCommon.Zmessage = "***  Malformed XML " + e.getMessage();
                NamedCommon.ZERROR = true;
                return;
            }
            doc.getDocumentElement().normalize();
            Element root = doc.getDocumentElement();
            parent = root.getTagName();
            if (root.hasAttributes()) GrabAttributes(root.getAttributes());
            if (root.hasChildNodes()) {
                if (ver == 1) {
                    GrabNodes(root.getChildNodes());
                } else {
                    valuesV2 = new ArrayList<ArrayList<String>>();
                    GrabNodesV2(root.getChildNodes());
                    for (int c=0 ; c < containers.size(); c++) {
                        if (valuesV2.get(c).size() == 0) {
                            containers.remove(c);
                            valuesV2.remove(c);
                            c--;
                        }
                    }
                }
            }
        }
    }

    private static void GrabNodes(NodeList nodeList) {
        String psave, tmp;
        int instance = 0;
        for (int count = 0; count < nodeList.getLength(); count++) {
            Node tempNode = nodeList.item(count);
            if (tempNode.getNodeType() == Node.ELEMENT_NODE) {

                tmp = parent + "." + tempNode.getNodeName();
//                for (int c=0 ; c < containers.size(); c++) { if (containers.get(c).equals(tmp)) instance = c; }
                instance = Collections.frequency(containers, tmp);
                containers.add(tmp);

                tmp = tempNode.getNodeValue();
                if (tmp == null) tmp = tempNode.getTextContent();
                values.add("[" + instance + "]" + tmp);

                psave = parent;
                parent += "." + tempNode.getNodeName();
                if (tempNode.hasAttributes()) GrabAttributes(tempNode.getAttributes());
                if (tempNode.hasChildNodes() && instance == 0) GrabNodes(tempNode.getChildNodes());
                parent = psave;
            }
        }
    }

    private static void GrabNodesV2 (NodeList nodeList) {
        String context="", value="";
        for (int i = 0; i < nodeList.getLength(); ++i) {
            Node node = nodeList.item(i);
            context = node.getNodeName();
            if (context.equals("#text")) {
                value = node.getNodeValue();
                int pos = containers.indexOf(parent);
                int instance = valuesV2.get(pos).size();
                if (valuesV2.get(pos).size() == 0) valuesV2.set(pos, new ArrayList<>());
                valuesV2.get(pos).add(value);
                continue;
            }
            if (node.hasChildNodes()) {
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    parent = parent + "." + node.getNodeName();
                    if (containers.indexOf(parent) < 0) {
                        containers.add(parent);
                        int pos = containers.indexOf(parent);
                        valuesV2.add(pos, new ArrayList<>());
                    }
                    GrabNodesV2(node.getChildNodes());
                    parent = parent.replace("."+node.getNodeName(), "");
                    continue;
                }
            }
        }
    }

    private static void GrabAttributes(NamedNodeMap attributes) {
        String tmp = "";
        ArrayList<String> tmpArr = new ArrayList<>();
        int fPos, instance = 0, lpCnt = attributes.getLength();
        // get attributes names and values
        for (int i = 0; i < lpCnt; i++) {
            Node node = attributes.item(i);
            tmp = parent + "." + attributes.item(i).getNodeName();
//            UpdateContainerAndValue(node, tmp);
            containers.add(tmp);
            values.add("[" + instance + "]" + attributes.item(i).getNodeValue());
        }
    }

    public static void XmlToArrayList(Element rootElement, String name, int inst) {
        Node node;
        String tmp;
        int fPos, instance;
        ArrayList<String> tmpArr = new ArrayList<>();
        NamedNodeMap nodeMap = rootElement.getAttributes();
        int nbrNodes = nodeMap.getLength();
        for (int n = 0; n < nbrNodes; n++) {
            tmp = parent + "." + nodeMap.item(n).getNodeName();
            containers.add(tmp);
            tmp = inst + nodeMap.item(n).getNodeValue();
            values.add(tmp);
//            UpdateContainerAndValue(nodeMap.item(n), tmp);

        }

        NodeList children = rootElement.getChildNodes();
        int nbrChildren = children.getLength();
        for (int i = 0; i < nbrChildren; i++) {
            node = children.item(i);
            switch (node.getNodeType()) {
                case Node.ATTRIBUTE_NODE:
                    ParseNode(node, i, name, inst);
                    break;
                case Node.ELEMENT_NODE:
                    ParseNode(node, i, name, inst);
                    break;
                case Node.TEXT_NODE:
                    break;  // ignore them
                default:
                    System.out.println(i + " UNKNOWN NODE TYPE " + node.getNodeType());
                    break;
            }
        }
    }

    private static void ParseNode(Node node, int i, String name, int inst) {
        String tmp, psave;
        int instSave, isave;
        if (node.getNodeType() == Node.ATTRIBUTE_NODE) GetAttributes(node, inst);
        int fPos, instance;
        ArrayList<String> tmpArr = new ArrayList<>();

        if (node.getNodeType() == Node.ELEMENT_NODE) {
            if (node.getNodeName().equals(name) || name.equals("*")) {
                tmp = node.getOwnerDocument().getNodeName();
                tmp = parent + "." + node.getNodeName();
                if (containers.indexOf(tmp) < 0) {
                    containers.add(tmp);
                    values.add("[0]");
                }
//                UpdateContainerAndValue(node, tmp);

                psave = parent;
                parent = parent + "." + node.getNodeName();
                GetAttributes(node, inst);
                int grandchildren = node.getChildNodes().getLength();
                if (node.hasChildNodes() && grandchildren > 1) {
                    instance = 0;
                    for (int c=0 ; c < containers.size(); c++) {
                        if (containers.get(c).equals(tmp)) instance = c;
                    }
//                    instance = Collections.frequency(containers, tmp);
                    if (containers.indexOf(tmp) < 0) {
                        containers.add(tmp);
                        tmp = "[" + instance + "]" + node.getNodeValue();
                        values.add(tmp);
                    }
//                    UpdateContainerAndValue(node,tmp);
                    instSave = inst;
                    isave = i;
                    psave = parent;
                    parent = parent + "." + node.getNodeName();
                    XmlToArrayList((Element) node, name, instance);
                    parent = psave;
                    i = isave;
                    inst = instSave;
                } else {
                    NodeList lNodes = node.getChildNodes();
                    tmp = parent + "." + node.getNodeName();
                    int nbrParts = lNodes.getLength();
                    for (int j = 0; j < nbrParts; j++) {
                        Node xNode = lNodes.item(j);
                        if (xNode.getNodeType() == xNode.TEXT_NODE || xNode.getNodeType() == xNode.CDATA_SECTION_NODE) {
                            if (containers.indexOf(tmp) < 0) {
                                containers.add(tmp);
                                tmp = inst + xNode.getNodeValue();
                                values.add(tmp);
                            }
//                            UpdateContainerAndValue(xNode, tmp);
                        }
                    }
                }
                parent = psave;
            }
        }
    }

    private static void GetAttributes(Node node, int inst) {
        String tmp;
        NamedNodeMap nodeMap = node.getAttributes();
        int nbrNodes = nodeMap.getLength();
        int fPos, instance;
        ArrayList<String> tmpArr = new ArrayList<>();
        for (int n = 0; n < nbrNodes; n++) {
            tmp = parent + "." + nodeMap.item(n).getNodeName();
            if (containers.indexOf(tmp) < 0) {
                containers.add(tmp);
                tmp = inst + nodeMap.item(n).getNodeValue();
                values.add(tmp);
            }
//            UpdateContainerAndValue(node, tmp);
        }
    }

    private static void UpdateContainerAndValue(Node tempNode, String nodename) {
        String val = "";
        int fPos, instance;
        for (int c=0 ; c < containers.size(); c++) {
            if (containers.get(c).equals(nodename)) instance = c;
        }
//        instance = Collections.frequency(containers, nodename);
        containers.add(nodename);
        npool.add(nodename);
        fPos = npool.indexOf(nodename);
        values.add(fPos, "");
//            dpool.add(fPos, "");
        val = tempNode.getNodeValue();
        if (val == null) val = tempNode.getTextContent();
        if (val == null) val = "";
        values.set(fPos, val);
    }

    private static void UnpackMicroservice() {
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";
        String line = uCommons.ReadDiskRecord(mscDir + mscat);
//        if (line.equals("")) line = BuildNewMSV();
        if (NamedCommon.ZERROR) return;
        String[] arr = line.split("\\r?\\n");
        String chr, nd, dbspec, var, srtn, node, fld;
        String mu = "", rtnVal = "", literal, chkLn, proto;
        int gInt, instance;

        microsvc = new ArrayList<String>(Arrays.asList(arr));
        int mscLength = microsvc.size();
        for (int l = 0; l < mscLength; l++) {
            line = microsvc.get(l);
            chkLn= line.replaceAll("\\ ", "");
            if (chkLn.equals(""))   continue;

            chr = line.substring(0, 1);
            if (chr.equals("#"))    continue;
            if (chr.equals("!")) {
                chkLn = line.substring(1, line.length());
                proto = uCommons.FieldOf(chkLn, ",", 1);
                if (!proto.equals(NamedCommon.protocol)) continue;
                line = line.substring((chr+proto).length(), line.length());
                chr = "";
            }

//            line = (line + ",~,~,~,~,~,~,~,~,~");
            line += ", , , , , , , , , ";
            String[] part = line.split(",");
            if (part.length < 5) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "Error in msv line " + line + " - not all fields are defined.";
                return;
            }
            rtnVal = uCommons.DynamicSubs(part[1]);
            nd = uCommons.DynamicSubs(part[2]);
            dbspec = uCommons.DynamicSubs(part[3]);
            var = uCommons.DynamicSubs(part[4]);
            srtn = uCommons.DynamicSubs(part[5]);

            rtnVal = rtnVal.replaceAll("\\ ", "");
            nd = nd.replaceAll("\\ ", "");
            dbspec = dbspec.replaceAll("\\ ", "");
            var = var.replaceAll("\\ ", "");

            fld = (mu + rtnVal + nd + var + srtn).replaceAll("\\ ", "");
            if (fld.length() < 1) continue;

            if (!nd.startsWith("payload.") && !nd.equals("") && plf.toUpperCase().equals("JSON")) nd = "payload." + nd;
            instance = containers.indexOf(nd);
            if (instance < 0 && !nd.trim().equals("")) {
                uCommons.uSendMessage("   >> WARNING. msv node " + nd + " is not in the inbound payload");
                uCommons.uSendMessage("      Please check the spelling, upper case / lower case, etc.");
                if (plf.toUpperCase().equals("JSON")) {
                    uCommons.uSendMessage("      Remember that rFuel prefixes all containers with \"payload.\".");
                }
            }
            instance = 0;
            switch (chr) {
                case "#":
                    continue;
                case "!":
                    // e.g. line = !this is a literal
                    // literal = "this is a literal"
                    nextLIT++;
                    nd = "isLITERAL-" + nextLIT;
                    var = "$lit-" + nextLIT + "$";
                    AddToPool(var, "in-memory", nd, instance);
                    literal = line.substring(1, line.length());
                    // exand literal to include @DATE, @TIME & @NOW
                    constants.add(var);
                    litValues.add(literal);
                default:
                    if (rtnVal.toUpperCase().equals("Y")) ReturnString = var;
                    node = nd;
                    if (srtn.replaceAll("\\ ", "").equals("")) {
                        AddToPool(var, dbspec, node, instance); // vpool, mpool, npool
                    } else {
                        if (!line.contains("(")) {
                            line = part[5];
                            // ## Test ##
                            if (line.contains("|")) {
                                srtn = line.split("\\|")[0];
                                line = line.substring(srtn.length() + 1, line.length());
                            } else {
                                if (line.indexOf(" ") > 0) {
                                    srtn = line.split(" ")[0];
                                    line = line.substring(srtn.length() + 1, line.length());
                                } else {
                                    srtn = line;
                                    line = "";
                                }
                            }
                            // ## END Test ##
                        } else {
                            line = srtn.split("\\(")[1];
                            line = line.split("\\)")[0];
                            srtn = srtn.split("\\(")[0];
                            String[] vlist = line.split("\\ ");
                            gInt = vlist.length;
                            for (int vl = 0; vl < gInt; vl++) {
                                var = vlist[vl];
                                AddToPool(var, dbspec, "", instance);
                            }
                        }
                        if (!srtn.equals("~")) {
                            srtns.add(srtn);
                            sArgs.add(line);
                        }
                        if (!var.equals("")) AddToPool(var, dbspec, node, instance);
                    }
            }
        }
    }

    private static String BuildNewMSV() {
        /* --------------- new micro-service --------------- */
        String root = parent + ".";
        String template = "#Multi,Lvl,Node,DBMap,Var,Srtn,Comments\n";
        String lvl = "";
        String insNode = "";
        String insVar = "";
        String insLn = "";
        String line = "";
        int rootlx = root.length(), lpCnt = containers.size();
        for (int i = 0; i < lpCnt; i++) {
            insNode = containers.get(i);
            insVar = insNode;
            if (insNode.startsWith(root)) {
                insVar = insNode.substring(rootlx, insNode.length());
            }
            insLn = "," + lvl + "," + insNode + ",," + insVar + ",,";
            template += insLn + "\n";
        }
        template += ",,,,,ucat-t-cImporter @VPOOL @MPOOL @DPOOL\n";
        try {
            BufferedWriter bw = uCommons.GetOSFileHandle(mscDir + mscat + "");
            if (bw == null) bw = uCommons.CreateFile(mscDir, mscat, "");
            if (bw != null) {
                bw.write(template);
                bw.flush();
                bw.close();
            }
            String eMsg1 = "<<FAIL>> Cannot find the Micro-Service: " + mscat;
            uCommons.uSendMessage(eMsg1);
            String eMsg2 = "<<ASSIST>> rFuel has built it BUT it needs to be 'mapped'"
                    + " before it can be used.";
            uCommons.uSendMessage(eMsg2);
            line = template;
        } catch (IOException e) {
            NamedCommon.Zmessage = "Auto-Template: failed write to micro-service catalog directory. " + e.getMessage();
            NamedCommon.ZERROR = true;
        }
        return line;
    }

    private static void MatchNodesToValues() {
        String tmp;
        int maxOccur = 0, itmp, nPos, nbrLoops=0;
        if (ver == 1) {
            nbrLoops = values.size();
            for (int v = 0; v < nbrLoops; v++) {
                tmp = values.get(v);
                if (tmp.startsWith("[")) {
                    tmp = values.get(v);
                    tmp = tmp.replaceAll("\\[", "");
                    tmp = tmp.split("\\]")[0];
                    itmp = Integer.valueOf(tmp);
                    if (itmp > maxOccur) maxOccur = itmp;
                }
            }
        } else {
            maxOccur = valuesV2.get(0).size();
        }

        if (ver == 1) maxOccur++;

        String node="", val="";
        ArrayList<String> dvals;
        nbrLoops = vpool.size();
        for (int v = 0; v < nbrLoops; v++) {
            dvals = new ArrayList<>();
            for (int vv = 0; vv < maxOccur; vv++) { dvals.add(""); }
            dpool.add(dvals);
        }

        int nbrContainers = containers.size();
        nbrLoops = npool.size();
        for (int nv = 0; nv < nbrLoops; nv++) {
            node = npool.get(nv);
            nPos = containers.indexOf(node);
            if (nPos > -1) {
                if (ver == 1) {
                    for (int v = nPos; v < nbrContainers; v++) {
                        if (containers.get(v).equals(node)) {
                            val = values.get(v);
                            tmp = val;
                            if (tmp.startsWith("[")) {
                                tmp = tmp.replaceAll("\\[", "");
                                tmp = tmp.split("\\]")[0];
                                itmp = Integer.valueOf(tmp);
                            } else {
                                itmp = 0;
                            }
                            val = val.substring(val.indexOf("]") + 1, val.length());
                            dpool.get(nv).set(itmp, val);
                        }
                    }
                } else {
                    for (int v=0; v < valuesV2.get(nv).size(); v++) {
                        dpool.get(nv).set(v, valuesV2.get(nPos).get(v));
                    }
                }
            }
        }
    }

    private static void InteractU2Host() {
        if (reqFile) {
            if (NamedCommon.debugging) uCommons.uSendMessage("      >. reqFile is true ");
            if (NamedCommon.uRequests != null) {
                if (NamedCommon.debugging) uCommons.uSendMessage("      >. NamedCommon.uRequests != null");
                if (!NamedCommon.uRequests.isOpen()) {
                    if (NamedCommon.debugging) uCommons.uSendMessage("      >. !NamedCommon.uRequests.isOpen()");
                    NamedCommon.uRequests = u2Commons.uClose(NamedCommon.uRequests);
//                    NamedCommon.uRequests = null;
                    if (!OpenRequests()) return;
                    if (NamedCommon.debugging) uCommons.uSendMessage("      >. NamedCommon.uRequests checked and isOpen");
                }
                if ( NamedCommon.uRequests != null) {
                    if (NamedCommon.debugging) uCommons.uSendMessage("      >. POST-check NamedCommon.uRequests != null ");
                    if (!NamedCommon.uRequests.getFileName().equals("uREQUESTS")) {
                        try {
                            NamedCommon.uRequests.close();
                        } catch (UniFileException e) {
                            // will be caught in OpenRequest
                        }
                        NamedCommon.uRequests = null;
                        if (!OpenRequests()) return;
                    }
                }
                if (NamedCommon.debugging) uCommons.uSendMessage("      >. POST-check NamedCommon.uRequests is Okay. ");
            } else {
                if (!OpenRequests()) return;
            }
        } else {
            if (NamedCommon.debugging) uCommons.uSendMessage("      >. reqFile is false ***************************");
        }

        if (NamedCommon.debugging) uCommons.uSendMessage("      >. starting uCat()");
        int gInt = srtns.size();
        String srName, msType, msThrd, msItem;
        int argCnt;
        if (NamedCommon.debugging) uCommons.uSendMessage("      >. POST-check [" + gInt + "] srtn(s)");
        for (int sr = 0; sr < gInt; sr++) {
            if (!NamedCommon.ZERROR) {
                srName = srtns.get(sr);
                if (NamedCommon.debugging) uCommons.uSendMessage("      >. handle srtn [" + srName + "]");
                argCnt = sArgs.get(sr).split("\\ ").length;
                String[] msFrame = new String[5];
                msFrame = srName.split("-");
                msType = msFrame[0].toUpperCase();
                msThrd = msFrame[1].toUpperCase();
                msItem = msFrame[2];
                tempList = BuildCallStrings(sArgs.get(sr));
                NamedCommon.replyReqd = false;
                switch (msType) {
                    case "SUBR":
                        startAction = System.nanoTime();
                        uSub(srName, argCnt);
                        finishAction = System.nanoTime();
                        laps = (finishAction - startAction) / div;
                        uCommons.uSendMessage("      >. Subr " + srName + " round-trip " + laps + " second.");
                        NamedCommon.replyReqd = true;
                        break;
                    case "EXEC":
                        startAction = System.nanoTime();
                        uCmd(msItem, argCnt);
                        finishAction = System.nanoTime();
                        laps = (finishAction - startAction) / div;
                        uCommons.uSendMessage("      >. Exec call round-trip " + laps + " second.");
                        NamedCommon.replyReqd = true;
                        break;
                    case "UCAT":
                        uCat(msItem, sr);
                        NamedCommon.replyReqd = false;
                        break;
                    case "JAVA":
                        // innovation: flowable hookpoint.
                        NamedCommon.replyReqd = true;
                        break;
                    case "UPLF":
                        switch (msItem.toUpperCase()) {
                            case "CAT":
                                String theVal = uCommons.UplFunction("=cat|" + sArgs.get(sr), "");
                                // how do i put this value into tempList and dpool ???
                                break;
                        }
                        break;
                    default:
                        NamedCommon.Zmessage = "Unknown micro-service type of " + msType;
                        NamedCommon.ZERROR = true;
                }
                if (NamedCommon.ZERROR) {
                    StringBuilder sbMsg = new StringBuilder();
                    int mlx = NamedCommon.Zmessage.length();
                    int asciiOf;
                    String letter;
                    char chr;
                    boolean skip=false;
                    for (int ch=0; ch  <mlx; ch++) {
                        letter  = NamedCommon.Zmessage.substring(ch,(ch+1));
                        chr = letter.charAt(0);
                        asciiOf = (int) chr;
                        if (asciiOf >= 32 && asciiOf <= 126) {
                            sbMsg.append(chr);
                            skip=false;
                        } else {
                            if (!skip) sbMsg.append("\r\n");
                            skip=true;
                        }
                    }
                    NamedCommon.Zmessage = sbMsg.toString();
                }
            }
        }
    }

    private static void uCmd(String srName, int argCnt) {
        String tmp = "", exe = srName + " ", var = "";
        String[] lTmp;
        argCnt = tempList.size();
        // tempList examples   $key$=value<tm>
        for (int a = 0; a < argCnt; a++) {
            tmp = tempList.get(a);
            if (tmp.startsWith("const=")) {
                lTmp = tmp.split("=");
                tmp = " " + lTmp[1];
            } else {
                lTmp = tmp.split("=");
                var = lTmp[0];
                tmp = "";
                int pos = vpool.indexOf(var);
                if (pos > -1) {
                    int lpCnt = dpool.get(pos).size();
                    for (int dp=0; dp < lpCnt; dp++) { tmp += " " + dpool.get(pos).get(dp); }
                    if (lpCnt > 1) tmp = "\"" + tmp.substring(1, tmp.length()) + "\"";
                } else {
                    tmp = "";
                }
            }
            exe += tmp;
        }

        exe = exe.trim();
        exe = exe.replaceAll("\\ \\ ", " ");
        if (NamedCommon.debugging) uCommons.uSendMessage("Execute: " + exe);
        // this is only for UVCS - will need to innovate for other protocols

        if (u2Commons.uniExec(exe)) {
            String output = u2Commons.uniExecResp;
            if (!output.equals("") && !output.equals("0") && !output.startsWith("200")) {
                NamedCommon.Zmessage = "Error during execution of " + srName + "  >>  " + output;
                NamedCommon.ZERROR = true;
            }
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Cannot create UniCommand: " + exe;
            return;
        }
//        try {
//            UniCommand ucmd = NamedCommon.uSession.command();
//            ucmd.setCommand(exe);
//            try {
//                ucmd.exec();
//                String output = ucmd.response();
//                if (!output.equals("") && !output.equals("0") && !output.startsWith("200")) {
//                    NamedCommon.Zmessage = "Error during execution of " + srName + "  >>  " + output;
//                    NamedCommon.ZERROR = true;
//                }
//            } catch (UniCommandException e) {
//                NamedCommon.ZERROR = true;
//                NamedCommon.Zmessage = "Cannot execute " + exe + "   " + e.getMessage();
//            }
//        } catch (UniSessionException e) {
//            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
//            NamedCommon.ZERROR = true;
//            NamedCommon.Zmessage = "Cannot create UniCommand: " + e.getMessage();
//        }
    }

    private static String rSub(String srname, int argCnt) {
        String[] msFrame = srname.split("-");
        String ans = "", tmp;
        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        if (NamedCommon.sConnected) {
            String[] realArgs = new String[argCnt];
            for (int f=0; f < argCnt; f++) {
                realArgs[f] = "";
                realArgs[f] = (tempList.get(f)+"~~~").split("=")[1].replaceAll("\\~\\~\\~","");
            }
            ans = u2Commons.rSub(srname, realArgs);
            ArrayList<String> rtnArr = new ArrayList<>(Arrays.asList(ans.split("<tm>")));
            if (NamedCommon.debugging) uCommons.uSendMessage("   .) Update tempList");
            String resp;
            for (int f=0; f < argCnt; f++) {
                tmp = tempList.get(f);
                if (NamedCommon.debugging) uCommons.uSendMessage("      > f: " + f + "  tmp: " + tmp);
                if (f >= rtnArr.size()) {
                    resp = "";
                } else {
                    resp = rtnArr.get(f);
                }
                tmp = tmp.split("=")[0] + "=" + resp;
                tempList.set(f, tmp);
                tmp = "";
            }
        }
        return ans;
    }

    private static void uSub(String srname, int argCnt) {
        if (NamedCommon.protocol.equals("real")) {
            PrepareCallStrings(srname, null, argCnt, tempList);
            retVal = rSub(srname, argCnt);
            ReturnValuesToDpool(srname, null, tempList, argCnt);
            return;
        }
        String[] msFrame = srname.split("-");
        String srtn = msFrame[2].replaceAll("\\ ", "");
        if (!NamedCommon.sConnected) SourceDB.ConnectSourceDB();
        if (NamedCommon.sConnected) {
            UniSubroutine uSubr;
            int tries = 0;
            restart = true;
            while (restart) {
                uSubr = null;
                if (!NamedCommon.ZERROR) uSubr = CreateSubrHook(srtn, argCnt);
                if (!NamedCommon.ZERROR) PrepareCallStrings(srtn, uSubr, argCnt, tempList);
                if (!NamedCommon.ZERROR) CallSubroutine(srtn, uSubr);
                if (!NamedCommon.ZERROR) {
                    if (restart) {
                        tries++;
                        if (tries > 3) {
                            NamedCommon.ZERROR = true;
                            NamedCommon.Zmessage = "UniSubr ERROR: " + srtn + " unknown failure cause.";
                            restart = false;
                        } else {
                            SourceDB.DisconnectSourceDB();
                            uCommons.uSendMessage("SourceDB TimeOut(): re-try # ");
                            SourceDB.ConnectSourceDB();
                        }
                    } else {
                        if (!NamedCommon.ZERROR) ReturnValuesToDpool(srname, uSubr, tempList, argCnt);
                    }
                } else {
                    restart = false;
                }
            }
        }
    }

    private static void uCat(String msItem, int sr) {
        if (NamedCommon.debugging) uCommons.uSendMessage("      >. Process uCat.");
        if (NamedCommon.debugging) uCommons.uSendMessage("      >. Will try to use ["+NamedCommon.uRequests.getFileName()+"]");
        String tempQue = "";
        if (NamedCommon.isWebs) {
            if (JMSConsumer.tempDest == null) {
                tempQue = "";
            } else {
                tempQue = JMSConsumer.tempDest.toString();
                if (tempQue.contains("queue://")) tempQue = uCommons.FieldOf(tempQue, "//", 2);
                uCommons.uSendMessage(">>>");
                uCommons.uSendMessage("rFuel batabase agent will reply to " + tempQue);
                uCommons.uSendMessage(">>>");
            }
            tempList.add("reply=" + tempQue);
        } else {
            tempList.add("reply=" + NamedCommon.reply2Q);
        }
        tempList.add("corrl=" + NamedCommon.CorrelationID);
        tempList.add("mscat=" + msItem);
        tempList.add("msfmt=" + esbFMT);
        tempList.add("dacct=" + APImsg.APIget("dacct"));
        StringBuilder sb = new StringBuilder("");
        int nbrLoops = tempList.size();
        for (int tl = 0; tl < nbrLoops; tl++) { sb.append(tempList.get(tl) + "\r\n"); }
        String rec = String.valueOf(sb);
        String key = "";
        NamedCommon.sentU2 = true;
        if (reqFile) {
            if (NamedCommon.debugging) uCommons.uSendMessage("      >. Writing to uREQUESTS " + rec.length() + " bytes.");
            NamedCommon.uRequests = u2Commons.uClose(NamedCommon.uRequests);

            String exists = "~~";
            if (!NamedCommon.uRequests.getFileName().equals("uREQUESTS")) {
                try {
                    NamedCommon.uRequests.close();
                } catch (UniFileException e) {
                    // will be caught in OpenRequest
                }
                NamedCommon.uRequests = null;
                if (!OpenRequests()) return;
            }
            while (!exists.equals("")) {
                writeCNT++;
                key = pid + ":" + writeCNT;
                exists = u2Commons.uRead(NamedCommon.uRequests, key).toString();
                if (!exists.equals("") && NamedCommon.debugging) uCommons.uSendMessage("      >. " + key + " is in use.");
            }
            if (!NamedCommon.ZERROR) {
                if (NamedCommon.debugging) uCommons.uSendMessage("      >. " + key + " safe to use as @ID");
                u2Commons.uWriter(NamedCommon.uRequests, key, rec);
                if (!NamedCommon.ZERROR && NamedCommon.debugging) uCommons.uSendMessage("Finished writing");
                NamedCommon.zID = key;
            }
        } else {
            if (NamedCommon.debugging) uCommons.uSendMessage("uREQUEST is NOT open");
            if (NamedCommon.protocol.equals("real")) {
                String id = "<" + NamedCommon.pid + ">_" + NamedCommon.CorrelationID;
                rec = rec.replaceAll("\\r\\n", "[[fm]]");
                rec = rec.replaceAll("\\=", "<is>");
                rec = rec.replaceAll("\\{", "<bo>");
                rec = rec.replaceAll("\\}", "<bc>");
                String cStr = "{WRI}{file=uREQUESTS}{item=" + id + "}{data=" + rec + "}";
                String junk = u2Commons.MetaBasic(cStr);
                if (junk.equals("ok")) {
                    cStr = "{EXE}{exec=uHARNESS \"! port=" + id + "\"}";
                    junk = u2Commons.MetaBasic(cStr);
                    cStr = "";
                } else {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = junk;
                }
            } else {
                String fname = NamedCommon.CorrelationID;
                key = fname;
                uCommons.uSendMessage("Creating " + NamedCommon.InMount + fname + ".temp");
                mntRequests = uCommons.CreateFile(NamedCommon.InMount, fname, ".temp");
                if (mntRequests == null) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Cannot create file: " + fname;
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    return;
                }
                String fqname = NamedCommon.InMount + "/" + fname;
                key = fqname;
                int lpCnt = tempList.size();
                for (int ll = 0; ll < lpCnt; ll++) {
                    try {
                        mntRequests.write(tempList.get(ll));
                        mntRequests.newLine();
                    } catch (IOException e) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "Write failure to " + fqname + " " + e.getMessage();
                        return;
                    }
                }
                try {
                    mntRequests.flush();
                    mntRequests.close();
                } catch (IOException e) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Close failure on " + fqname + " " + e.getMessage();
                    return;
                }
                if (!NamedCommon.ZERROR) {
                    String Ffrom = NamedCommon.InMount + "/" + fname + ".temp";
                    String Fto = NamedCommon.InMount + "/" + fname + ".send";
                    if (!uCommons.RenameFile(Ffrom, Fto)) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "ABORT. " + Fto + " already exists.";
                        uCommons.uSendMessage(NamedCommon.Zmessage);
                    }
                }
            }
        }
        if (!NamedCommon.ZERROR) uCommons.uSendMessage(key + " sent for processing");
    }

    private static boolean OpenRequests() {
//        if (NamedCommon.uRequests.isOpen()) return true;
        try {
            NamedCommon.uRequests = NamedCommon.uSession.openFile("uREQUESTS");
            if (NamedCommon.debugging) uCommons.uSendMessage("uREQUESTS is OPEN");
        } catch (UniSessionException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = e.getMessage();
            if (NamedCommon.Zmessage.contains("RPC")) NamedCommon.ConnectionError = true;
            uCommons.uSendMessage("OpenRequests() ERROR: " + NamedCommon.Zmessage);
        }
        return (!NamedCommon.ZERROR);
    }

    private static UniSubroutine CreateSubrHook(String srName, int argCnt) {
        if (NamedCommon.debugging) uCommons.uSendMessage("Connecting with " + srName);
        UniSubroutine sr = null;
        while (true) {
            try {
                sr = NamedCommon.uSession.subroutine(srName, argCnt);
                break;
            } catch (UniSessionException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "UniSession ERROR: " + srName + " " + e.getMessage();
                    uCommons.uSendMessage(tName + " " + NamedCommon.Zmessage);
                    break;
                }
            }
        }
        return sr;
    }

    private static void PrepareCallStrings(String srName, UniSubroutine uSubr, int argCnt, ArrayList<String> tempList) {
        String tmp = "", var = "";
        for (int a = 0; a < argCnt; a++) {
            // tempList examples
            //  $key$=value <tm>
            //  $key$=      <tm>
            //  value       <tm>
            tmp = tempList.get(a) + "<tm>";
            if (tmp.contains("=")) {
                var = " " + tmp.split("=")[0];
                tmp = tmp.split("=")[1];
            } else {
                var = " ";
            }
            if (NamedCommon.protocol.equals("real")) {
                realCstrings += tmp;
            } else {
                tmp = tmp.replace("<tm>", "");
                SetSubArg(srName, uSubr, a, tmp);
            }
            if (NamedCommon.debugging) uCommons.uSendMessage("      > [" + a + "]" + var + " = " + tmp);
            if (NamedCommon.ZERROR) break;
        }
    }

    private static void CallSubroutine(String srName, UniSubroutine uSubr) {
        while (true) {
            try {
                uSubr.call();
                String reply = uSubr.getArg(0);
                if (!reply.equals("") && !reply.equals("0") && !reply.startsWith("200")) {
                    NamedCommon.Zmessage = reply;
                    NamedCommon.ZERROR = true;
                }
                restart = false;
                break;
            } catch (UniSubroutineException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    NamedCommon.Zmessage = "UniSubr ERROR: " + srName + " call failed " + e.getMessage();
                    NamedCommon.ZERROR = true;
                    break;
                }
            }
        }
    }

    private static void ReturnValuesToDpool(String srName, UniSubroutine uSubr, ArrayList<String> tempList, int argCnt) {
        String val, key, kvp;
        int vpx;
        for (int a = 0; a < argCnt; a++) {
            if (NamedCommon.protocol.equals("u2cs")) {
                val = GetSubArg(srName, uSubr, a);
            } else {
                val = (tempList.get(a)+"~~~").split("=")[1].replaceAll("\\~\\~\\~", "");
            }
            if (NamedCommon.ZERROR) break;
            key = tempList.get(a) + "=<junk>";
            if (key.contains("=")) {
                key = key.split("=")[0];
                kvp = key + "=" + val;
                tempList.set(a, kvp);
                vpx = vpool.indexOf(key);
                if (vpx >= 0) dpool.get(vpx).set(0, val);
            }
        }
    }

    private static String GetSubArg(String srName, UniSubroutine uSubr, int a) {
        String ans = "";
        while(true) {
            try {
                ans = uSubr.getArg(a);
                break;
            } catch (UniSubroutineException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "UniSubr ERROR: " + srName + " " + e.getMessage();
                    uCommons.uSendMessage(tName + " " + "UniSubr ERROR: " + srName + " returning arg# " + a + " to dpool");
                    uCommons.uSendMessage(tName + " " + NamedCommon.Zmessage);
                    break;
                }
            }
        }
        return ans;
    }

    private static void SetSubArg(String srName, UniSubroutine uSubr, int a, String s) {
        while (true) {
            try {
                uSubr.setArg(a, s);
                break;
            } catch (UniSubroutineException e) {
                if (!u2Commons.TestAlive()) {
                    SourceDB.ReconnectService();
                } else {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "UniSubr ERROR: " + srName + " " + e.getMessage();
                    uCommons.uSendMessage(tName + " " + "UniSubr ERROR: " + srName + " on arg# " + a);
                    uCommons.uSendMessage(tName + " " + NamedCommon.Zmessage);
                    break;
                }
            }
        }
    }

    private static void AddToPool(String var, String dbspec, String node, int instance) {
        if (var.equals("")) {
            uCommons.uSendMessage("MSV ERROR: there is no $variable$ name for " + dbspec);
            uCommons.uSendMessage("         : the data will NOT be written for this line.");
        }
        if (!var.startsWith("@") && !var.startsWith("!")) {
            if (vpool.indexOf(var) < 0) {
                vpool.add(var);
                mpool.add(dbspec);
                if (!node.equals("")) npool.add(node);
            }
        }
    }

    private static ArrayList<String> BuildCallStrings(String callString) {
        // callString e.g. $v1$ $v2$ $v3$ $v4$ ...
        NamedCommon.SubsList = new ArrayList<>();
        NamedCommon.DataList = new ArrayList<>();
        String tmp1, tmp2, tmp3;
        ArrayList<String> lTemp = new ArrayList<String>();
        String[] callArr = new String[]{};
        if (callString.contains("|")) {
            callArr = callString.split("\\|");
        } else {
            callArr = callString.split("\\ ");
        }
        int nbrVars = callArr.length, tInt;
        for (int tl = 0; tl < nbrVars; tl++) { lTemp.add(""); }
        for (int tl = 0; tl < nbrVars; tl++) {
            tmp1 = callArr[tl];
            if (tmp1.startsWith("@")) {
                switch (tmp1) {
                    case "@uRTN.CODE":
                        lTemp.set(tl, "@uRTN.CODE=");
                        break;
                    case "@VPOOL":
                        lTemp.set(tl, "vpool=" + Array2uvDyn(vpool));
                        break;
                    case "@MPOOL":
                        lTemp.set(tl, "mpool=" + Array2uvDyn(mpool));
                        break;
                    case "@DPOOL":
                        lTemp.set(tl, "dpool=" + Arr2D2uvDyn(dpool, mpool));
                        break;
                    default:
                        uCommons.uSendMessage("Unknown CallString argument " + tmp1);
                }
            } else {
                if (tmp1.startsWith("!")) {
                    lTemp.set(tl, "const=" + tmp1.substring(1, tmp1.length()));
                } else {
                    if (tmp1.startsWith("\"")) {
                        lTemp.set(tl, "const=" + tmp1);
                    } else {
                        tInt = vpool.indexOf(tmp1);
                        if (tInt > -1) {
                            tmp2 = dpool.get(tInt).get(0);
                        } else {
                            tmp2 = tmp1;
                        }
                        lTemp.set(tl, vpool.get(tInt) + "=" + tmp2);
                    }
                }
            }
            String[] junkArr = new String[2];
            for (int ja = 0; ja < 2; ja++) { junkArr[ja] = ""; }
            junkArr = (lTemp.get(tl) + " ").split("\\=");

            if (junkArr[1].equals(" ")) junkArr[1] = "";
            NamedCommon.SubsList.add(junkArr[0]);
            NamedCommon.DataList.add(junkArr[1]);
        }

        nbrVars = APImsg.GetU2PSize();
        String u2props = "props=";
        String sep = "";
        if (nbrVars > 0) {
            for (int pp = 0; pp < nbrVars; pp++) {
                u2props += sep + "msg_" + APImsg.APIgetU2PKey(pp) + "=" + APImsg.APIgetU2PVal(pp);
                sep = "\t";
            }
        }

        u2props += sep + "msg_dacct=" + NamedCommon.datAct;
        sep = "\t"; // in case it did NOT go thru the loop
        u2props += sep + "msg_msgid=" + NamedCommon.MessageID;
        lTemp.add(u2props);
//        lTemp.add("dacct="+NamedCommon.datAct);
        return lTemp;
    }

    private static String Array2uvDyn(ArrayList<String> inVpool) {
        String datalist = "";
        String sep = "";
        int nbrItems = inVpool.size();
        for (int i = 0; i < nbrItems; i++) {
            datalist += sep + inVpool.get(i);
            sep = "\t";
        }
        return datalist;
    }

    private static String Arr2D2uvDyn(ArrayList<ArrayList<String>> inDpool, ArrayList<String> inMpool) {
        String datalist = "", ans;
        String sep = "", dMapper, av, mv, sv, rSep="<im>";
        int nbrItems = inDpool.size();
        String[] dMap = new String[5];
        String[] ansArr = new String[nbrItems];
        String n = "N";
        for (int i = 0; i < nbrItems; i++) {
            dMapper = inMpool.get(i);
            if (dMapper.equals("")) continue;
            dMapper += "_ _ _";
            if (dMapper.contains("_")) {
                dMap = dMapper.split("\\_");
                av = dMap[1].trim();
                mv = dMap[2].trim();
                sv = dMap[3].trim();

                ans = Array2uvDyn(inDpool.get(i));

                ansArr = ans.split("\t");
                int nbrAns = ansArr.length;
                rSep = "<im>";
                if (nbrAns > 0) {
                    datalist += sep + ansArr[0];
                    for (int j = 1; j < nbrAns; j++) {
                        if (!ansArr[j].equals("")) {
                            if (av.toUpperCase().equals(n)) rSep = "<fm>";
                            if (mv.toUpperCase().equals(n)) rSep = "<vm>";
                            if (sv.toUpperCase().equals(n)) rSep = "<sm>";
                            datalist += rSep + ansArr[j];
                        }
                    }
                } else {
                    datalist += sep;
                }
            } else {
                datalist += sep + "";
            }
            sep = "\t";
        }
        return datalist;
    }

    private static String json2xml(String payload, String rootname) {
        String xml = "";
        if (!payload.equals("")) {
            try {
                JSONObject j2xObject = new JSONObject(payload);
                xml = NamedCommon.xmlProlog + "<" + rootname + ">"
                        + XML.toString(j2xObject) + "</" + rootname + ">"
                ;
                j2xObject = null;
            } catch (JSONException je) {
                uCommons.uSendMessage(je.getMessage());
                NamedCommon.Zmessage = je.getMessage();
                NamedCommon.ZERROR = true;
            }
        }
        return xml;
    }

}
