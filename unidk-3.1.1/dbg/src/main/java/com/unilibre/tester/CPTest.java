package com.unilibre.tester.tester;

import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;

import java.io.IOException;
import java.util.Date;

public class CPTest {
    CPTest() { };
    static Thread[] threads = null;
    static int m_NumberOfThreads = 4;
    static String numberOfThreads = "4";
    static String _hostName = "54.153.142.191";
    static String _userName = "rfuel";
    static String _passWord = "un1l1br3";
    static String _accountPath="/data/uv/RFUEL";
    static String _dataSourceType = "UNIVERSE";
    static boolean goodResponse = false;
    static String response = null;
    static String _minPoolSize = "2";
    static String _maxPoolSize = "16";

    public static void main(String args[]) throws IOException, Throwable {
        if (_hostName == null) {
            _hostName = inputString("Host Name:");
        }
        if (_userName == null) {
            _userName = inputString("User Name:");
        }
        if (_passWord == null) {
            _passWord = inputString("User Password:");
        }
        if (_dataSourceType.equals("Unknown")) {
            goodResponse = false;
            while (goodResponse == false) {
                response =
                        inputString("Is Server UniVerse" + " or UniData (V/D):");
                if (response.equalsIgnoreCase("V")) {
                    _dataSourceType = "UNIVERSE";
                    goodResponse = true;
                } else if (response.equalsIgnoreCase("D")) {
                    _dataSourceType = "UNIDATA";
                    goodResponse = true;
                } else {
                    System.out.println("... Enter V or D");
                }
            }
        }
        if (_accountPath == null) {
            _accountPath = inputString("Account Path:");
        }
        if (_minPoolSize == null) {
            _minPoolSize = inputString("Min Pool Size:");
        }
        if (_maxPoolSize == null) {
            _maxPoolSize = inputString("Max Pool Size:");
        }
        if (numberOfThreads == null) {
            numberOfThreads =
                    inputString("Number of Simultaneous Connections : ");
        }

        m_NumberOfThreads = Integer.parseInt(numberOfThreads);
        long lStartTime = new Date().getTime();

        System.out.println(" ");
        System.out.println("Program Starting");
        System.out.println(" ");
        System.out.println("Create the threads ...");
        ConcurrentTest2();
        System.out.println("Join (run) the threads");
        System.out.println("Round 1: -------------------------------------------------------------------------------");
        for (int i = 0; i < m_NumberOfThreads; i++) { threads[i].join(); }

        System.out.println("Round 2: -------------------------------------------------------------------------------");
        for (int i = 0; i < m_NumberOfThreads; i++) { threads[i].join(); }

        System.out.println("Program Finished");
        long ldiff = (new Date().getTime() - lStartTime);
        System.out.println("Total Time : " + ldiff + " Milliseconds");
        System.out.println("Press enter to stop");
        System.in.read();
    }

    public static String inputString(String msg) throws IOException {
        String userInput;
        byte bArray[] = new byte[128];
        int bytesRead;
        System.out.print(msg);
        bytesRead = System.in.read(bArray);
        userInput = new String(bArray, 0, bytesRead);
        userInput = userInput.trim();
        return (userInput);
    }

    private static void ConcurrentTest2() {
        int nIndex = m_NumberOfThreads;
        threads = new Thread[nIndex];
        for (int i = 0; i < nIndex; i++) {
            Thread t = new Thread(new ThreadStart());
            threads[i] = t;
        }
        for (int i = 0; i < nIndex; i++) {
            threads[i].setName("T" + (i + 1));      // sets the thread name
            threads[i].start();                     // java starts a branch for the thread to run in
        }
    }

    static class ThreadStart implements Runnable {
        UniSession us1 = null;
        UniJava j = null;
        long tFrom = System.currentTimeMillis(), tTo = 0;
        long div = 100000;
        int laps = 0, dbgCnt =0;

        private void DoCounter(String msg) {
            tTo = System.currentTimeMillis();
            laps = (int) (tTo - tFrom);
            tFrom = tTo;
            System.out.println(Thread.currentThread().getName() + "     " + dbgCnt + "  " + laps + " " + msg);
            dbgCnt++;
        }

        public void run() {
            try {
//                UniJava.setIdleRemoveThreshold(20000);
//                UniJava.setIdleRemoveExecInterval(20000);
                j = new UniJava();
// you can comment out below lines and set the ConnectionPooling using
// Configuration file called "uoj.properties". This way you do not have to
// change the code. Put "uoj.properties" file in current working directory.
// Installation contains "uoj.properties" file.
                UniJava.setUOPooling(true);
                Integer minSize = Integer.decode(_minPoolSize);
                Integer maxSize = Integer.decode(_maxPoolSize);
                UniJava.setMinPoolSize(minSize.intValue());
                UniJava.setMaxPoolSize(maxSize.intValue());
// it will create trace file (uoj_trace.log)in current working directory
                UniJava.setPoolingDebug(false);
                us1 = j.openSession();
                us1.setHostName(_hostName);
                us1.setUserName(_userName);
                us1.setPassword(_passWord);
                us1.setAccountPath(_accountPath);
                System.out.println("Connecting: " + _hostName + "  " + _accountPath);
                if (_dataSourceType.equals("UNIDATA")) {
                    us1.setConnectionString("udcs");
                } else {
                    us1.setConnectionString("uvcs");
                }
                us1.connect();

                DoCounter("UOJ-setup&connected");

                UniCommand cmd = us1.command();
                cmd.setCommand("SELECT uDELTA.LOG SAMPLE 100");
                cmd.exec();
                String s = cmd.response();
//                System.out.println(" Response from UniCommand : SSELECT BLAH  : " + s.replaceAll("\\r?\\n", ""));
                UniSelectList sl = us1.selectList(0);

                UniString ur = null;

                DoCounter("Unicommand-UniSelectList");

                UniFile uf = us1.open("uDELTA.LOG");

                UniString s2;
                while (!sl.isLastRecordRead()) {
                    s2 = sl.next();
                    if (!s2.equals("")) {

                        DoCounter("UniSelectList.next()");

                        uf.setRecordID(s2);
                        ur = uf.read();

                        DoCounter("uf.read("+s2+")");
                        dbgCnt = dbgCnt -2;


                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }

//                        System.out.println(" Record ID : " + s2);
                    }
                }
            } catch (UniSessionException ex) {
                if (us1 != null && us1.isActive()) {
                    try {
                        j.closeSession(us1);
                    } catch (UniSessionException e) {
// TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    us1 = null;
                }
                ex.printStackTrace();
                System.out.println(ex.getMessage());
            } catch (UniCommandException e) {
// TODO Auto-generated catch block
                e.printStackTrace();
            } catch (UniSelectListException e) {
// TODO Auto-generated catch block
                e.printStackTrace();
            } catch (UniFileException e) {
                e.printStackTrace();
            } finally {
                if (us1 != null && us1.isActive()) {
                    System.out.println("");
                    System.out.println(Thread.currentThread().getName() + " : Connection Passed in Test Program");
                    System.out.println("============================================================================");
                    System.out.println("============================================================================");
                    try {
                        j.closeSession(us1);
                    } catch (UniSessionException e) {
// TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}