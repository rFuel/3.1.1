package com.unilibre.rmi;

import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniObjectsTokens;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSessionException;
import com.unilibre.commons.APImsg;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class rmiClient {

    private static final String svrLocation = "rmi://192.168.4.39:8155/rfuel";
    private static rmiInterface stubb;

    public static void main(String[] args) {}

    private static void GetStubb() {
        try {
            stubb =  (rmiInterface) Naming.lookup(svrLocation);
        } catch (NotBoundException nbe) {
            System.out.println();
            System.out.println("NotBoundException [" + nbe.getMessage() + "] is not registered in RMI.");
            System.out.println("*** -------------------------------------------------------------- ***");
            System.out.println("*** check that the rmi end-point NAME is the same as the dbServer  ***");
            System.out.println("*** -------------------------------------------------------------- ***");
            System.out.println();
        } catch (MalformedURLException mue) {
            System.out.println();
            System.out.println("MalformedURLException " + mue.getMessage());
            System.out.println("*** -------------------------------------------------------------- ***");
            System.out.println("*** check that the url starts with 'rmi:' with a valid IP and Port ***");
            System.out.println("*** -------------------------------------------------------------- ***");
            System.out.println();
        } catch (RemoteException re) {
            System.out.println();
            System.out.println("RemoteException " + re.getMessage());
            System.out.println();
            System.out.println("-------------------------------------------------------");
            System.out.println("There is no service runing at [" + svrLocation + "]");
            System.out.println("-------------------------------------------------------");
        }
    }

    public static String ReadAnItem(String file, String item, String a, String m, String s) {
        NamedCommon.ZERROR = true;
        if (stubb == null) GetStubb();
        if (stubb == null) return "";
        if (!NamedCommon.sConnected) ConnectSource();
        if (!NamedCommon.sConnected) return "";
        NamedCommon.ZERROR = false;
        return "";
    }

    private static void ConnectSource() {
        String connector = APImsg.APIget("shost");
        if (connector.equals("")) {
            connector = "rFuel.properties";
        }
        String conMsg = "SourceDB.ConnectSourceDB.start(" + connector + ")    ";
        conMsg += NamedCommon.dbhost + ":" + NamedCommon.dbPort + "   " + NamedCommon.dbpath;
        uCommons.uSendMessage(conMsg);
        String status = "<<PASS>>", host = "";

        NamedCommon.uJava = null;
        NamedCommon.uJava = new UniJava();
        NamedCommon.uSession = null;
        NamedCommon.uSession = new UniSession();
        int contype = 0;
        if (NamedCommon.CPL) {
            NamedCommon.uJava.setPoolingDebug(NamedCommon.PoolDebug);
            NamedCommon.uJava.setIdleRemoveThreshold(60000);    // max session idle time  = 60 seconds
            NamedCommon.uJava.setIdleRemoveExecInterval(15000); // look for idle sessions = 15 seconds
            NamedCommon.uJava.setOpenSessionTimeOut(3000);      // wait 3 seconds for a session to open
            NamedCommon.uJava.setUOPooling(true);

            uCommons.uSendMessage("     Using CPL connection ---------------");
            if (NamedCommon.debugging) uCommons.uSendMessage("   ). minPoolSize " + NamedCommon.minPoolSize);
            if (NamedCommon.debugging) uCommons.uSendMessage("   ). maxPoolSize " + NamedCommon.maxPoolSize);
            if (NamedCommon.debugging) uCommons.uSendMessage("   ). PoolDebug   " + NamedCommon.PoolDebug);
            if (NamedCommon.debugging) uCommons.uSendMessage("   ). Secure      " + NamedCommon.uSecure);

            try {
                int minSize = Integer.valueOf(NamedCommon.minPoolSize);
                int maxSize = Integer.valueOf(NamedCommon.maxPoolSize);
                NamedCommon.uJava.setMinPoolSize(minSize);
                NamedCommon.uJava.setMaxPoolSize(maxSize);
            } catch (NumberFormatException nfe) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "CPL pool sizes MUST be integers - please check rFuel.properties";
                uCommons.uSendMessage(NamedCommon.Zmessage);
                uCommons.uSendMessage("check: minpoolsize and maxpoolsize");
                return;
            }
            if (NamedCommon.uSecure) contype = UniObjectsTokens.SECURE_SESSION;
            if (NamedCommon.debugging) uCommons.uSendMessage("   ). SecConCode  " + contype);
        } else {
            if (NamedCommon.debugging) uCommons.uSendMessage("     Using SEAT connection --------------");
        }
        try {
            NamedCommon.uSession = NamedCommon.uJava.openSession(contype);
            if (NamedCommon.debugging) uCommons.uSendMessage("   ). uJava object created ");
        } catch (UniSessionException e) {
            uCommons.uSendMessage("<<FAIL>>  [ABORT] Cannot open a session");
            NamedCommon.ZERROR = true;
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            return;
        }

    }

}
