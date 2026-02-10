package com.unilibre.kafka.dev;

import asjava.uniclientlibs.UniDataSet;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;
import org.apache.kafka.clients.producer.Producer;

import java.util.concurrent.ThreadLocalRandom;

public class uDeltaTEST implements Runnable {

    public static String IMark = "<im>", FMark = "<fm>", VMark = "<vm>", SMark = "<sm>";
    private String host, path, user, pword, protocol, sMaxpool, broker, topic, processor;
    private String command  = "SELECT uDELTA.LOG SAMPLE 25";
    private int tNumber, maxpool, sleeper;
    private boolean secure;
    private UniJava uj;
    private UniSession us;
    private Producer kProducer;

    public uDeltaTEST(String[] args, int tno) {
        this.tNumber    = tno;
        this.uj         = null;
        this.us         = null;
        this.kProducer  = null;
        this.secure     = false;
        //
        this.host       = args[0];
        this.path       = args[1];
        this.user       = args[2];
        this.pword      = args[3];
        this.protocol   = args[6];
        this.sMaxpool   = args[8];
        this.topic      = args[13];
        this.broker     = args[14];

        try {
            maxpool = Integer.valueOf(sMaxpool);
        } catch (NumberFormatException nfe) {
            maxpool = 2;
        }
        if (args[7].toLowerCase().equals("true")) this.secure = true;
        this.sleeper = ThreadLocalRandom.current().nextInt(1, 7) * 1000;
        // T-{thread number}  P-{pid}
        this.processor = "T-" + tNumber;

    }

    //    @Override
    public void run() {
        boolean eop = false, eof = false;
        int cnt = 0, pcnt = 0, selCnt = 0, lastCnt = 0, lockCnt = 0;
        UniString id, uvRec;
        UniFile uf;
        UniDataSet uDset;
        try {
            uj = new UniJava();
            us = null;
            UniJava.setUOPooling(true);
            UniJava.setMinPoolSize(1);
            UniJava.setMaxPoolSize(maxpool);
            if (secure) {
                us = uj.openSession(UniObjectsTokens.SECURE_SESSION);
            } else {
                us = uj.openSession();
            }
            us.setHostName(host);
            us.setAccountPath(path);
            us.setUserName(user);
            us.setPassword(pword);
            us.setConnectionString(protocol);
            us.connect();
            us.setDefaultReleaseStrategy(4);
            System.out.println(processor + ":  is connected");
            UniCommand uc = us.command();
            uc.setCommand(command);
            uf = us.open("uDELTA.LOG");
            uf.setLockStrategy(0);
//            uDset = new UniDataSet();

            while (!eop) {
                uc.exec();
                selCnt = uc.getAtSelected();
                if (selCnt < 1) {
                    if (lastCnt < 1) eop = true;
                    continue;
                }

                while (!eof) {
                    cnt++;
                    id = us.selectList(0).next();
                    if (us.selectList(0).isLastRecordRead()) eof = true;
                    if (id.toString().equals("")) {
                        if (cnt >= selCnt) {
                            eof = true;
                            continue;
                        }
                    }
                    if (uf.isRecordLocked(id)) continue;
                    try {
                        if (uf.isRecordLocked(id)) continue;
                        uf.setRecordID(id);
                        uvRec = uf.read(id);
                        uf.deleteRecord(id);
                    } catch (UniFileException e) {
                        continue;
                    }

//                    System.out.println(processor + " read in  [" + id.toString() + "]");

//                    uDset.append(id);
                    pcnt++;
                }
//                uf.deleteRecord(uDset);
                us.selectList(0).clearList();
                try {
                    Thread.sleep(sleeper);
                } catch (InterruptedException e) {
                }
                lastCnt = selCnt;
                System.out.println(processor + ":  logged & delted " + pcnt + " events");
                eof = false;
                eop = true;
            }
            uf.close();
        } catch (UniSessionException e) {
            System.out.println(processor + " : UniSessionException:  " + e.getMessage());
            if (us != null && us.isActive()) {
                try {
                    uj.closeSession(us);
                } catch (UniSessionException uniSessionException) {
                }
                us = null;
            }
            return;
        } catch (UniCommandException e) {
            System.out.println(processor + " : UniCommandException:  " + e.getMessage());
        } catch (UniSelectListException e) {
            System.out.println(processor + " : UniSelectListException:  " + e.getMessage());
        } catch (UniFileException e) {
            System.out.println(processor + " : UniFileException:  " + e.getMessage());
        } finally {
            if (us != null && us.isActive()) {
//                System.out.println(processor + " : closing connection");
                try {
                    uj.closeSession(us);
                } catch (UniSessionException e) {
                }
                System.out.println(processor + " : is finished.");
            }
        }
        us = null;
        uj = null;
        uDset = null;
    }

}
