package com.unilibre.core;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;

import java.io.BufferedWriter;
import java.util.Properties;

public class uRESTmanager_UNUSED {

    private static String mscDir = NamedCommon.BaseCamp + "/mscatalog/", mscat = "", payload = "";
    public static String uTask = "055", plf = "", esbFMT = "xml";
    private static boolean badMsg = false;

    public static String HandleMessage(String msg) {
        uCommons.uSendMessage("uRESTmanager.HandleMessage()");
        NamedCommon.Zmessage = "";
        int status = 200;
        String response = "";
//        if (!uManager.initialised) uManager.Initialise();
        DecodeInstruction(msg);
        if (!NamedCommon.ZERROR && !badMsg) {
            long start = System.nanoTime();
            if (NamedCommon.debugging) uCommons.uSendMessage("-------------------------------------------------------------");
            String[] args = new String[]{uTask + "2", esbFMT, mscDir, mscat, payload, "", "", "", "", ""};
            if (NamedCommon.threader) {
                ThreadManager.begin(args, null);
            } else {
                response = microservice.RESTput(args);
                if (!NamedCommon.Zmessage.equals("")) status = 500;
            }
            long end = System.nanoTime();
            String sTaken = uCommons.oconvM(String.valueOf((end - start)), "MD9");
//            uCommons.uSendMessage(NamedCommon.CorrelationID + " processed in " + sTaken + " seconds");
        } else {
            status = 500;
            if (badMsg) status = 412;
        }
        if (status != 200) {
//            response = "";
//        } else {
            String descr = NamedCommon.ReturnCodes.get(status);
            if (descr.equals("")) descr = "ReturnCode [" + status + "] not found.";
            if (NamedCommon.Zmessage.length() > 1) NamedCommon.Zmessage = " " + NamedCommon.Zmessage;
            response = DataConverter.ResponseHandler(String.valueOf(status), descr, NamedCommon.Zmessage, esbFMT);
        }
        if (NamedCommon.debugging) uCommons.uSendMessage("uRESTmanager.finished(*)");
        return response;
    }

    private static void KeepStats(String corrID, String sTaken) {
        String filename = corrID.split("-")[0] + ".stats";
        String fdir = MessageProtocol.basecamp + "logs/" + filename;

        String oldVal = uCommons.ReadDiskRecord(fdir + filename);

        BufferedWriter bStats = null;
        bStats = uCommons.GetOSFileHandle(fdir + filename);
        if (bStats == null) bStats = uCommons.CreateFile(fdir, filename, "");
        if (bStats != null) {
            //
        }
    }

    private static void DecodeInstruction(String instr) {
        badMsg = false;
        String inMsg = "payload=";
        instr = instr.replaceAll("\r?\n", "");

        inMsg = instr.substring(instr.indexOf(inMsg) + inMsg.length(), instr.length());

        String mqFields = instr.replace(inMsg, "");

        while (inMsg.indexOf("  ") > -1) {
            inMsg = inMsg.replaceAll("\\ \\ ", " ");
        }

        inMsg = inMsg.replaceAll("\\\t", "");
        if (inMsg.indexOf("=") > -1) {
            inMsg = inMsg.replaceAll("\\=", "\t");
        }

        if (inMsg.indexOf(" ") > -1) {
            inMsg = inMsg.replaceAll("\\ ", "\b");
        }

        Properties mProps = uCommons.DecodeMessage(mqFields);
        String mTask = mProps.getProperty("TASK", "-X-");
        String ppty = mProps.getProperty("PPTY", "rFuel.properties");
        esbFMT = mProps.getProperty("FORMAT", "xml");
        mscat = mProps.getProperty("MSCAT", "-X-");
        plf = mProps.getProperty("PLFMT", "AUTO").toUpperCase();
        payload = inMsg;

        // one day... allow mscat definitions in the message !!
        if ((mTask + mscat).contains("-X-")) {
            badMsg = true;
            return;
        }

        if (payload.indexOf("\t") > -1) {
            payload = payload.replaceAll("\\\t", "=");
        }
        if (payload.indexOf("\b") > -1) {
            payload = payload.replaceAll("\\\b", " ");
        }
        if (!mTask.equals(uTask)) {
            NamedCommon.Zmessage = "Wrong task [" + mTask + "] sent to " + uTask + " processor";
            NamedCommon.ZERROR = true;
            return;
        }
        if (mscat.equals("0")) {
            NamedCommon.Zmessage = ("Unknown MSCat in message ");
            NamedCommon.ZERROR = true;
            return;
        }
        if (payload.equals("0")) {
            NamedCommon.Zmessage = ("Unknown payload in message ");
            NamedCommon.ZERROR = true;
            return;
        }
        switch (plf) {
            case "XML":
                if (!payload.substring(0, 3).contains("<")) {
                    NamedCommon.Zmessage = ("XML was expected as message payload.");
                    NamedCommon.ZERROR = true;
                    return;
                }
                break;
            case "LIXI":
                if (!payload.substring(0, 3).contains("<")) {
                    NamedCommon.Zmessage = ("LIXI was expected as message payload.");
                    NamedCommon.ZERROR = true;
                    return;
                }
                break;
            case "JSON":
                if (!payload.substring(0, 3).contains("{")) {
                    NamedCommon.Zmessage = ("JSON was expected as message payload.");
                    NamedCommon.ZERROR = true;
                    return;
                }
                break;
            default:
                if (payload.substring(0, 3).contains("<")) {
                    plf = "XML";
                } else {
                    if (payload.substring(0, 3).contains("{")) {
                        plf = "JSON";
                    } else {
                        NamedCommon.Zmessage = ("Unknown format of message payload.");
                        NamedCommon.ZERROR = true;
                        return;
                    }
                }
        }

        Properties runProps = uCommons.LoadProperties(ppty);
        uCommons.SetCommons(runProps);
    }

}
