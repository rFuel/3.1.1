package com.unilibre.core;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import java.util.ArrayList;
import java.util.Date;

public class ThreadManager {

    private static Thread[] threadslots = null;
    private static ArrayList<Thread> threads = null;
    private static ArrayList<String> tStatus = null;
    private static int m_NumberOfThreads = 2;
    private static long lStartTime;
    private static String thisCmd="";

    public static void begin(String inCmd) {
        thisCmd = inCmd;
        lStartTime = new Date().getTime();
        int i, nIndex = m_NumberOfThreads;
        String simpleName = "@DUMMY";
        threadslots = new Thread[nIndex];
        tStatus = new ArrayList<String> ();
        tStatus.add(simpleName);
        int myCtr=0, tFnd=0;
        boolean stopFlg=false;
        while (!stopFlg) {
            for (i = 0; i < nIndex; i++) {
                simpleName = "UniThread" + i;
                if (tStatus.indexOf(simpleName) < 0) {
                    tStatus.add(simpleName);
                    tFnd = tStatus.indexOf(simpleName);
                    uCommons.eMessage = ">>>processing [" + simpleName + "]"+
                            " in slot # "+ tFnd;
                    uCommons.uSendMessage(uCommons.eMessage);
                    threadslots[i] = new Thread(new ThreadStart());
                    threadslots[i].setName(simpleName);
                    threadslots[i].start();
                    try {
                        threadslots[i].join();
                    } catch (InterruptedException e) {
                        uCommons.uSendMessage("Issue with " + simpleName);
                        uCommons.uSendMessage(">>>>> Thread join failure <<<<< " + e.toString());
                    }
                } else {
                    uCommons.uSendMessage("thread slot[" + i + "] is in use");
                }
            }
            myCtr++;
            if (myCtr > 5) {
                stopFlg = true;
            } else {
                try {
                    Thread.sleep(3000);
                    uCommons.uSendMessage("--------------------------------------------------------");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class ThreadStart implements Runnable {

        private volatile boolean running = true;

        public void terminate(String tName) {
            uCommons.uSendMessage("<<<terminating thread: "+tName);
            long ldiff = (new Date().getTime() - lStartTime);
            int tFnd = tStatus.indexOf(tName);
            if (tFnd > 0) {
                tStatus.remove(tFnd);
            } else {
                uCommons.uSendMessage("---> cannot find "+tName);
            }
            uCommons.eMessage = "Total Time : " + ldiff + " Milliseconds for thread execution";
            uCommons.uSendMessage(uCommons.eMessage);
            running = false;
        }

        public void run() {
            while (running) {
                String tName = Thread.currentThread().getName();
                //uCommons.uSendMessage("   run() thread   [" + tName + "]");
                uCommons.uSendMessage("   execute ["+thisCmd+"]");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // execute thisCmd here
                terminate(tName);
            }
        }
    }
}