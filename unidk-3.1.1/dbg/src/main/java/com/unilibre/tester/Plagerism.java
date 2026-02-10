package com.unilibre.tester.tester;

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;

public class Plagerism {

    public static void main(String[] args) {
        String dbhost = "54.153.142.191";
        String dbpath = "/data/uv/RFUEL";
        String dbuser = "rfuel";
        String passwd = "un1l1br3";
        UniJava uJava = new UniJava();
        UniSession uSession;

        try {
            uSession = uJava.openSession(0);
            uSession.setHostName(dbhost);
            uSession.setAccountPath(dbpath);
            uSession.setUserName(dbuser);
            uSession.setPassword(passwd);
            uSession.setConnectionString("uvcs");
            uSession.connect();
            try {
                UniFile Customer = uSession.open("CUSTOMER");
                String id = "100435";
                try {
                    Customer.setRecordID(id);
                    UniString usRec = Customer.read();
                    UniDynArray rec = new UniDynArray (usRec);
                    if (rec.extract(25).toString().equals("S")) {
                        rec.replace(25, "J");
                        Customer.setRecord(rec);
                        Customer.write();
                    }
                } catch (UniFileException e) {
                    System.out.println(id + " not found in file " + Customer.getFileName());
                }
            } catch (UniSessionException e) {
                System.out.println("CUSTOMER open failure");
            }
        } catch (UniSessionException e) {
            throw new RuntimeException(e);
        }
    }
}
