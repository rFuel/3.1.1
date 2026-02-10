package com.unilibre.kafka.dev;

import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kStream;

import java.util.ArrayList;
import java.util.UUID;

public class ThreaderThread {

    public static ArrayList<String> threadslots = new ArrayList<>();
    private static ArrayList<String> kEvents = new ArrayList<>();
    private static String task = "", IMark="<im>";

    public static void begin(String proc, ArrayList<String> updates) {
        //
        // version 1 accepts an arraylist of updates
        // the assumption is that the updates are in rFuel message format.
        //
        String tName;
        task = "";

        switch (proc) {
            case "SQL":
                task = "022";
                kEvents = updates;
                break;
            case "LOG":
                task = "001";
                break;
            default:
                uCommons.uSendMessage("Invalid thead task. Ignoring this process");
                return;
        }
        if (task.equals("")) {
            uCommons.uSendMessage("Unknown thread task. Ignoring this process");
        }
        Thread thisThread = new Thread(new ThreadStart());
        tName = String.valueOf(UUID.randomUUID());
        thisThread.setName(tName);          // identify this thread //
        thisThread.start();                 // invoke run()         //
        kEvents.clear();
        kEvents = null;
    }

    static class ThreadStart implements Runnable {

        private volatile boolean running = true;

        @Override
        public void run() {
            if (kEvents.size() < 1) return;
            while (running) {
                String ThreadName = Thread.currentThread().getName();
                try {
                    Execute(ThreadName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Terminate(ThreadName);
            }
        }

        private void Execute(String ThreadName) throws Exception {
            threadslots.add(ThreadName);
            switch (task) {
                case "022":
                    int eoi = kEvents.size();
                    if (eoi < 1) return;
                    String line, timedate, account, file, itemId;
                    String[] lparts;
                    for (int e=0; e < eoi; e++) {
                        line = kEvents.get(e);
                        lparts = uStrings.gSplit2Array(line, IMark);
                        timedate = lparts[0].replaceAll("\\.","");
                        account  = lparts[1];
                        file     = lparts[2];
                        itemId   = lparts[3];
                        uCommons.uSendMessage(timedate + IMark + account + IMark + file + IMark + itemId);
                        kStream.ThreadDo(line);
                    }
                    break;
                case "001":
                    break;
                default:
                    return;
            }
            running = false;
        }

        private void Terminate(String ThreadName) {
            int idx = threadslots.indexOf(ThreadName);
            if (idx > -1) threadslots.remove(idx);
        }
    }
}
