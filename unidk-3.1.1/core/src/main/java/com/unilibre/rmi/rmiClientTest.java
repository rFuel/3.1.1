package com.unilibre.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class rmiClientTest {

    private static final String svrLocation = "rmi://192.168.4.39:8155/rfuel";
    private static rmiInterface stubb;

    public static void main(String[] args) {
        System.out.println("Before running a dbClient - the dbServer MUST be running !");
        System.out.println("----------------------------------- [dbClient]");
        rmiCheck();
    }

    private static void rmiCheck() {

        // is the service up and running ?

        if (stubb == null) stubb = GetStubb();
        if (stubb == null) return;

        try {

            System.out.println(stubb.CheckFile("uv", "MULEBANK", "TRAN"));
            System.out.println(stubb.Executor("uv", "SPLIT.FILE"));

        } catch (RemoteException e) {
            System.out.println();
            System.out.println("RemoteException " + e.getMessage());
            System.out.println();
            System.out.println("-------------------------------------------------------");
            System.out.println("The service is not runing at [" + svrLocation + "]");
            System.out.println("-------------------------------------------------------");
        }

    }

    public static String ReadAnItem(String file, String item, String a, String m, String s) {

        return "";
    }

    private static rmiInterface GetStubb() {
        try {
            return (rmiInterface) Naming.lookup(svrLocation);
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
        return null;
    }

}
