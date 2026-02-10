package com.unilibre.tester.tester;

import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSessionException;

import java.io.IOException;
import java.util.Date;

public class ConPoolTest {
    ConPoolTest() { };
    static Thread[] threads = null;
    static int m_NumberOfThreads = 0;
    static String numberOfThreads="";
    static String _hostName = "54.153.142.191";
    static String _userName = "rfuel";
    static String _passWord = "un1l1br3";
    static String _accountPath = "/data/uv/RFUEL";
    static String _dataSourceType = "UNIVERSE";
    static boolean goodResponse = false;
    static String response = "";
    static String _minPoolSize="1";
    static String _maxPoolSize="10";

    public static void main(String args[]) throws IOException, Throwable {
        m_NumberOfThreads = 5;
        long lStartTime = new Date().getTime();
        ConcurrentTest2();
        for (int i = 0; i < m_NumberOfThreads; i++) {
            threads[i].join();
        }
//        long ldiff = (new Date().getTime() - lStartTime);
//        System.out.println("Total Time : " + ldiff + " Milliseconds");
//        System.in.read();
    }

    private static void ConcurrentTest2() {
        int nIndex = m_NumberOfThreads;
        threads = new Thread[nIndex];
        for (int i = 0; i < nIndex; i++) {
            Thread t = new Thread(new ThreadStart());
            threads[i] = t;
        }
        for (int i = 0; i < nIndex; i++) {
            threads[i].setName("MyThreadProc" + (i + 1));
            threads[i].start();
        }
    }

    static class ThreadStart implements Runnable {
        UniSession us1 = null;
        UniJava j = null;

        public void run() {
            try {
                j = new UniJava();
                UniJava.setUOPooling(true);
                Integer minSize = Integer.decode(_minPoolSize);
                Integer maxSize = Integer.decode(_maxPoolSize);
                UniJava.setMinPoolSize(minSize.intValue());
                UniJava.setMaxPoolSize(maxSize.intValue());
                UniJava.setPoolingDebug(true);
                us1 = j.openSession();
                us1.setHostName(_hostName);
                us1.setUserName(_userName);
                us1.setPassword(_passWord);
                us1.setAccountPath(_accountPath);
                if (_dataSourceType.equals("UNIDATA")) {
                    us1.setConnectionString("udcs");
                } else {
                    us1.setConnectionString("uvcs");
                }
                us1.connect();
//                UniCommand cmd = us1.command();
//                cmd.setCommand("COUNT CLIENT");
//                cmd.exec();
//                String s = cmd.response();
//                System.out.println(" Response from UniCommand : COUNT CLIENT " + s);
//                UniSelectList sl = us1.selectList(0);
//                try {
//                    String sFM = us1.getMarkCharacter(UniTokens.FM);
//                    int bb = 0;
//                } catch (UniConnectionException e1) {
//                    e1.printStackTrace();
//                }
//                while (!sl.isLastRecordRead()) {
//                    UniString s2 = sl.next();
//                    if (!s2.equals("")) {
//                        System.out.println(" Record ID : " + s2);
//                    }
//                }
            } catch (UniSessionException ex) {
                if (us1 != null && us1.isActive()) {
                    try {
                        j.closeSession(us1);
                    } catch (UniSessionException e) {
                        e.printStackTrace();
                    }
                    us1 = null;
                }
                ex.printStackTrace();
                System.out.println(ex.getMessage());
//            } catch (UniCommandException e) {
//                e.printStackTrace();
//            } catch (UniSelectListException e) {
//                e.printStackTrace();
            } finally {
                if (us1 != null && us1.isActive()) {
                    System.out.println("==========================================================================");
                    System.out.println( Thread.currentThread().getName() + " : Connection Passed in Test Program");
                    System.out.println("==========================================================================");
                    try {
                        j.closeSession(us1);
                    } catch (UniSessionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
 }
