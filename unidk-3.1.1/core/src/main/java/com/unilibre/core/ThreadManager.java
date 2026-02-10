package com.unilibre.core;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;

import java.util.Date;

public class ThreadManager {

    // Invoked by a uManager                                                    //
    // -------------------------------------------------------------------------//
    // 1. Recieve a payload.                                                    //
    // 2. Fire-off a Thread to microservice Execute                             //

    private static long lStartTime;
    public static boolean runflag = true;
    private static String message;
    public static String[] Decoder;
    private static String tName;
    private static String inTask, esbFmt;

    public static void begin(String[] decoder, String inMessage) {
        uCommons.uSendMessage("ThreadManager.begin(*)");
        message = inMessage;
        lStartTime = new Date().getTime();
        inTask = decoder[0];
        String mscDir = "", mscat = "", payload = "", tname = "", qNbr = "", inQue = "";

        qNbr = decoder[2];
        inQue = decoder[3];
        mscat = decoder[4];
        payload = decoder[5];
        tname = decoder[6];
        esbFmt = decoder[7];
        mscDir = decoder[8];
        Decoder = decoder;
        tName = tname + "_" + mscat;

        NamedCommon.tName = tName;
        Thread thisThread;

        uCommons.uSendMessage(tName + " " + " create thread");
        thisThread = new Thread(new ThreadStart());
        thisThread.setName(tName);
        uCommons.uSendMessage(tName + " " + "  start thread");
        thisThread.start();
    }

    static class ThreadStart implements Runnable {

        private volatile boolean running = true;

        private void Terminate(String tName) {
            uCommons.uSendMessage(tName + " " + " finish thread " + tName);
            long ldiff = (new Date().getTime() - lStartTime);
            uCommons.eMessage = "Total Time : " + ldiff + " Milliseconds for thread execution";
            uCommons.uSendMessage(tName + " " + uCommons.eMessage);
            int idx = NamedCommon.threadslots.indexOf(tName);
            NamedCommon.threadslots.remove(idx);
            running = false;
            runflag = false;
        }

        public void run() {
            runflag = true;
            while (running) {
                String tName = Thread.currentThread().getName();
                Execute(tName);
                Terminate(tName);
            }
        }

        private void Execute(String tName) {
            NamedCommon.threadslots.add(tName);
            switch (inTask) {
                case "0550":
                    responder.HandleMessage(Decoder, message);
                    break;
                case "0552":
                    microservice.RESTput(Decoder);
                    break;
            }
        }

    }

    public static void main(String[] args) {
        Runnable threadObj = new ThreadStart();

        Thread tObj1 = new Thread(threadObj);
        tObj1.setName("TheName");
        tObj1.start();
    }

}