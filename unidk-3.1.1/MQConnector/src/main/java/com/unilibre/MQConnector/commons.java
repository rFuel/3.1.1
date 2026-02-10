package com.unilibre.MQConnector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;

public class commons {

    public static boolean ZERROR = false;
    public static String pid = ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0];
    public static int pCounter = 1;
    public static int cCounter = 1;
    public static String ERRmsg = "", inputQueue="";
    private static int ackMode=0;

    public static void Sleep (int mSecs) {
        try {
            Thread.sleep(mSecs);
        } catch (InterruptedException e) {
            uSendMessage("Thread Sleep error");
            uSendMessage(e.getMessage());
        }
    }

    public static void uSendMessage(String msg) {
        System.out.println(msg);
    }

    public static String ReadDiskRecord(String infile) {
        String rec = "";
        BufferedReader BRin = null;
        FileReader fr = null;
        try {
            fr = new FileReader(infile);
            BRin = new BufferedReader(fr);
            String line = null;
            try {
                line = BRin.readLine();
            } catch (IOException e) {
                uSendMessage("read FAIL on " + infile);
                uSendMessage(e.getMessage());
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    uSendMessage("read FAIL on " + infile);
                    uSendMessage(e.getMessage());
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                uSendMessage("File Close FAIL on " + infile);
                uSendMessage(e.getMessage());
            }
        } catch (IOException e) {
            uSendMessage("-------------------------------------------------------------------");
            uSendMessage("File Access FAIL :: " + infile);
            uSendMessage("-------------------------------------------------------------------");
        }
        return rec;
    }

    public static void SetAckMode(int mode) { ackMode = mode; }

    public static int GetAckMode() { return ackMode; }


}

