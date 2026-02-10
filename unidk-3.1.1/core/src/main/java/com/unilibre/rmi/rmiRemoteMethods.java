package com.unilibre.rmi;

import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSubroutine;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

public class rmiRemoteMethods extends UnicastRemoteObject implements rmiInterface {

    private UniJava uoj;
    private ArrayList<UniSession> uSessions      = new ArrayList<>();
    private ArrayList<UniFile> uniFiles          = new ArrayList<>();
    private ArrayList<UniSubroutine> subroutines = new ArrayList<>();
    private ArrayList<String> accounts           = new ArrayList<>();
    private ArrayList<String> openFiles          = new ArrayList<>();

    protected rmiRemoteMethods() throws RemoteException {
        super();
        uoj = new UniJava();
    }

    public String CheckFile(String host, String account, String filename) {
        String ans = "CheckFile: " + host+ " >> " + account + " >> " + filename;
        System.out.println("CheckFile - Reply with:  " + ans);
        return ans;
    }

    public String Executor(String host, String command)  {
        String ans = " Executor: " + host + " >> " + command;
        System.out.println("Executor - Reply with:  " + ans);
        return ans;
    }

    public void ConnectSource() {

    }

}
