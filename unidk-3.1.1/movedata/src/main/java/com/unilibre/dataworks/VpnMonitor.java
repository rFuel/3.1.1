package com.unilibre.dataworks;

import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SourceDB;
import com.unilibre.commons.uCommons;

import java.util.Properties;

public class VpnMonitor {

    private static String prefix = "";

    private static final String slash     = "/";
    private static final String ioTest    = prefix + "/upl/data/ins/vpn_monitor.txt";
    private static final String smsMsg    = prefix + "/upl/data/sms/sms.txt";
    private static final String slowFetch = prefix + "/upl/conf/SLOWfetch";
    private static final String slowBurst = prefix + "/upl/conf/SLOWburst";
    private static boolean allOkay=true, smsSent=false;

    private static void CheckBlobStore() {
        // The assumption is that /upl/data/ins is a mount of some kind to a remote disk

        int passCnt=0, max = 20;
        boolean forever = true;
        while (forever) {
            allOkay = true;
            // ---------------------------------------------------------------------
            if (!uCommons.FileExists(ioTest)) {
                if (!uCommons.WriteDiskRecord(ioTest, "")) {
                    if (allOkay) HandleCondition("WriteDiskRecord", "VPN write failure on blobstore");
                    allOkay = false;
                }
            }
            //
            if(SourceDB.ConnectSourceDB().contains("<<FAIL>>")) {
                HandleCondition("SourceDB", "is disconnected");
                allOkay = false;
            } else {
                uCommons.Sleep(5);
                SourceDB.DisconnectSourceDB();
            }
            // ---------------------------------------------------------------------
            if (allOkay) {
                if (uCommons.FileExists(slowFetch)) uCommons.DeleteFile(slowFetch);
                if (uCommons.FileExists(slowBurst)) uCommons.DeleteFile(slowBurst);
                if (smsSent) {
                    System.out.println("VPN and UV are available");
                    smsSent = false;
                }
                passCnt++;
                if ((passCnt % max) == 0) uCommons.uSendMessage("Heartbeat. "+String.valueOf(passCnt)+" checks of VPN and UV");
            }
            uCommons.Sleep(5);
            if (uCommons.FileExists(ioTest)) uCommons.DeleteFile(ioTest);
        }
    }

    private static void HandleCondition(String eClass, String eMessage) {
        if (smsSent) return;
        uCommons.WriteDiskRecord(smsMsg, "Comms issue:\n"+eClass+"\n"+eMessage);
        if (!uCommons.FileExists(smsMsg)) {
            // fatal condition - cannot recover.
            uCommons.nixExecute("rfuel -stop", true);
            System.exit(1);
        }
        // Slow the fetch and burst
        uCommons.WriteDiskRecord(slowFetch, "300");
        uCommons.WriteDiskRecord(slowBurst, "300");
        System.out.println("VPN is down");
        smsSent = true;
    }

    public static void main(String[] args) {
        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
            NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
        }

        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);

        if (NamedCommon.VPN) {
            CheckBlobStore();
        }
    }

}
