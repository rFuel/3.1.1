package com.unilibre.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.registry.*;
import java.rmi.RemoteException;

public class rmiServer extends rmiRemoteMethods {

    // the svrLocation AND the svcPort should be encrypted and decrypted when used.
    // by encrypting them, the rmi registry is as secured as you can make it.
    private static final String SVC_LOCATION = "rmi://192.168.4.39:8155/rfuel";
    private static final int SVC_PORT = 8155;
    // ----------------------------------------------------------------------------

    private static boolean sysErr=false;

    protected rmiServer() throws RemoteException {
        super();
    }

    private static void StartService() {
        try {

            LocateRegistry.createRegistry(SVC_PORT);
            System.out.println("Remote registry created. ---------------------");

            rmiInterface skeleton = new rmiRemoteMethods();
            System.out.println("skeleton is available. -----------------------");

            Naming.rebind(SVC_LOCATION, skeleton);
            System.out.println("Service is bound -----------------------------");

        } catch (RemoteException e) {
            System.out.println("RemoteException " + e.getMessage());
            sysErr = true;
        } catch (MalformedURLException mfe) {
            System.out.println("MalformedURLException " + mfe.getMessage());
            sysErr = true;
        }
    }

    public static void main(String[] args) {

        System.out.println("----------------------------------- [dbServer]");
        StartService();

        if (sysErr) {
            System.exit(1);
        } else {
            System.out.println();
            System.out.println("----------------------------------------------");
            System.out.println("Ready at [" + SVC_LOCATION + "]");
            System.out.println("----------------------------------------------");
        }
    }

}
