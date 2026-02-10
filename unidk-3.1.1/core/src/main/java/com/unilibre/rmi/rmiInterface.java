package com.unilibre.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface rmiInterface extends Remote {

    public String CheckFile(String host, String account, String filename) throws RemoteException;

    public String Executor(String host, String command) throws RemoteException;

    // and so on ....
}
