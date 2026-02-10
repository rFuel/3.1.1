package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import asjava.uniclientlibs.UniLogger;
import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniObjectsTokens;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSessionException;
import asjava.unirpc.UniRPCConnectionException;
import com.northgateis.reality.rsc.RSCConnection;
import com.northgateis.reality.rsc.RSCException;
import com.unilibre.cipher.uCipher;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.regex.Pattern;


public class SourceDB {

    public static boolean lostConnection = false;
    public static String message = "";

    private static int uTimeOut = 12 * 60 * 60;   // 12 Hour session timeout
    private static boolean dbTimeout = false;
    private static boolean showError = true;
    private static double div = 1000000000.00;
    private static long lastAction = 0;
    private static long rightNow = 0;
    private static double laps = 0;
    private static String IPregex="(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])";
    private static Pattern IPptn = Pattern.compile(IPregex);

    public static void PreLoadAES() {
        if (NamedCommon.AES) return;
        if (!NamedCommon.preLoadAES) {
            System.out.println(" ");
            System.out.println(" ");
            uCommons.uSendMessage("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*");
            uCommons.uSendMessage("Pre-loading encryption libraries .... this may take a while.");
        }
        String clearText = "test of the encryption factory and iteration vector";
        String encrText = uCipher.Encrypt(clearText);
        String decrText = uCipher.Decrypt(encrText);
        if (!decrText.equals(clearText)) {
            uCommons.uSendMessage("Encryption issue - cannot proceed");
            System.exit(1);
        }
        if (!NamedCommon.preLoadAES) {
            uCommons.uSendMessage("done.");
            uCommons.uSendMessage("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*");
            System.out.println(" ");
        }
        NamedCommon.preLoadAES = true;
    }

    public void start(String message) {
        // never used
        uCommons.Message2Properties(message);
        ConnectSourceDB();
        DisconnectSourceDB();
    }

    public static String ConnectSourceDB() {
        if (NamedCommon.sConnected) return "<<PASS>>";
        String fail = "<<FAIL>>", status = fail;
        Properties runProps;
        //
        // loop until the database connects .. or ... rfuel -stop
        //
        switch (NamedCommon.protocol) {
            case "u2cs":
                status = Connect_unics();
                break;
            case "real":
                status = Connect_Reality();
                break;
            case "u2mnt":
                status = Connect_uMount();
                break;
            case "u2sockets":
                status = Connect_uSocket();
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
                return "<<FAIL>>";
            default:
                status = Connect_unics();
                break;
        }
        if (NamedCommon.ZERROR) status = "<<FAIL>> ";
        if (status.equals("")) status = "<<PASS>>";
        NamedCommon.dbActive =  System.nanoTime();
        if (!status.contains("<<FAIL>>")) NamedCommon.sConnected = true;
        NamedCommon.IsAvailableU2 = NamedCommon.sConnected;
        if (NamedCommon.IsAvailableU2) PreLoadAES();
        return status;
    }

    public static String Connect_Reality() {
        String status = "<<FAIL>>";
        String dataAct= uCommons.FieldOf(NamedCommon.realac, ",", 1);
        String conMsg = "SourceDB.Connect_Reality() ";
        conMsg += "host: [" + NamedCommon.realhost+ "]  database: [" + NamedCommon.realdb + "]";
        conMsg += "   account: [" + dataAct + "]";
        uCommons.uSendMessage(conMsg);

        String h = NamedCommon.realhost;
        String d = NamedCommon.realdb;
        String u = NamedCommon.realuser;
        String a = NamedCommon.realac;

        NamedCommon.rcon = new RSCConnection(h,d,u,a);
        try {
            NamedCommon.rcon.connect();
            status = "<<PASS>>";
        } catch (RSCException e) {
            status = "<<FAIL>> " + e;
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = status;
            NamedCommon.sConnected = false;
        }
        conMsg = "Connection:   "+status;
        uCommons.uSendMessage(conMsg);
        return status;
    }

    public static String Connect_unics() {

        boolean holdSW = NamedCommon.debugging;
        showError = !NamedCommon.runSilent;                                         // @@@@ TEST @@@@
        NamedCommon.debugging = NamedCommon.PoolDebug;
        String connector = APImsg.APIget("shost");
        if (connector.equals("")) {
            connector = "rFuel.properties";
        }
        String conMsg = "SourceDB.ConnectSourceDB.start(" + connector + ")    ";

        // ------------------------ multi-source-systems ----------------------
//        if (!APImsg.APIget("shost").equals("")) {
//            String msgHost = "new host";
//            boolean useU2 = false;
//            Properties sProps = uCommons.LoadProperties(APImsg.APIget("shost"));
//            if (sProps.size() == 0) sProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/hosts/" + APImsg.APIget("shost"));
//            if (sProps.getProperty("u2host", "").equals("")) {
//                msgHost = APImsg.APIget("host");
//            } else {
//                msgHost = APImsg.APIget("u2host");
//                useU2 = true;
//            }
//            if (!msgHost.equals(NamedCommon.dbhost)) {
//                DisconnectSourceDB();
//                if (useU2) {
//                    NamedCommon.dbhost = APImsg.APIget("u2host");
//                    NamedCommon.dbpath = APImsg.APIget("u2path");
//                    NamedCommon.dbuser = APImsg.APIget("u2user");
//                    NamedCommon.passwd = APImsg.APIget("u2pass");
//                    NamedCommon.datAct = APImsg.APIget("u2acct");
//                    NamedCommon.databaseType = APImsg.APIget("dbtype");
//                    NamedCommon.protocol = APImsg.APIget("protocol");
//                } else {
//                    NamedCommon.dbhost = APImsg.APIget("host");
//                    NamedCommon.dbpath = APImsg.APIget("path");
//                    NamedCommon.dbuser = APImsg.APIget("user");
//                    NamedCommon.passwd = APImsg.APIget("pword");
//                    NamedCommon.datAct = APImsg.APIget("dacct");
//                    NamedCommon.databaseType = APImsg.APIget("dbtype");
//                }
//                msgHost = "";
//            }
//        }
        // --------------------------------------------------------------------

        conMsg += NamedCommon.dbhost + ":" + NamedCommon.dbPort + "   " + NamedCommon.dbpath;
        if (showError) uCommons.uSendMessage(conMsg);
        String status = "<<PASS>>", host = "";
        if (NamedCommon.sConnected) return status;

        NamedCommon.uJava = null;
        NamedCommon.uSession = null;

        NamedCommon.uJava = new UniJava();
        NamedCommon.uSession = new UniSession();
        UniLogger ul = new UniLogger(SourceDB.class);
        ul.isLoggable(Level.OFF);
        NamedCommon.uJava.setPoolingDebug(false);

        if (NamedCommon.debugging || NamedCommon.logLevel.toLowerCase().equals("5")) {
            uCommons.uSendMessage("rFuel is connecting to:-");
            uCommons.uSendMessage("   .) host " + NamedCommon.dbhost);
            uCommons.uSendMessage("   .) port " + NamedCommon.dbPort);
            uCommons.uSendMessage("   .) path " + NamedCommon.dbpath);
            uCommons.uSendMessage("   .) data " + NamedCommon.datAct);
            uCommons.uSendMessage("   .) user " + NamedCommon.dbuser);
            uCommons.uSendMessage("------------------------");
        }

        int contype = 0;

        // SET UP the uniJava Object -----------------------------------------------------------------------------------

        if (NamedCommon.CPL) {
            NamedCommon.uJava.setPoolingDebug(NamedCommon.PoolDebug);
            NamedCommon.uJava.setIdleRemoveThreshold(60000);    // max session idle time  = 60 seconds
            NamedCommon.uJava.setIdleRemoveExecInterval(15000); // look for idle sessions = 15 seconds
            NamedCommon.uJava.setOpenSessionTimeOut(3000);      // wait 3 seconds for a session to open
            NamedCommon.uJava.setUOPooling(true);

            if (showError) uCommons.uSendMessage("     Using CPL connection ---------------");
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
                if (showError) {
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    uCommons.uSendMessage("check: minpoolsize and maxpoolsize");
                }
                return "<<FAIL>>";
            }
            if (NamedCommon.uSecure) contype = UniObjectsTokens.SECURE_SESSION;
            if (NamedCommon.debugging) uCommons.uSendMessage("   ). SecConCode  " + contype);
        } else {
            if (NamedCommon.debugging) uCommons.uSendMessage("     Using SEAT connection --------------");
        }

        // SET UP the uniSession Object --------------------------------------------------------------------------------

        try {
            NamedCommon.uSession = NamedCommon.uJava.openSession(contype);
            if (NamedCommon.debugging) uCommons.uSendMessage("   ). uJava object created ");
        } catch (UniSessionException e) {
            NamedCommon.Zmessage = "<<FAIL>>  [ABORT] Cannot open a session";
            uCommons.uSendMessage("check: minpoolsize and maxpoolsize");
            if (showError) uCommons.uSendMessage(NamedCommon.Zmessage);
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
            return NamedCommon.Zmessage;
        }
        if (NamedCommon.SourceDBIP.equals("")) {
            if (IPptn.matcher(NamedCommon.dbhost).matches()) {
                NamedCommon.SourceDBIP = NamedCommon.dbhost;
                NamedCommon.showDNSresoltion = false;
            }
        }
        int wait4 = 0;
        int tries = 1;
        int port = 0;
        boolean abort = false, connected = false;
        while (!connected && !abort) {
            if (NamedCommon.showDNSresoltion || NamedCommon.showPID) {
                try {
                    NamedCommon.showPID = true;
                    uCommons.uSendMessage("Resolving " + NamedCommon.dbhost);
                    InetAddress inetAddress = InetAddress.getByName(NamedCommon.dbhost);
                    String ipAddress = inetAddress.getHostAddress();
                    uCommons.uSendMessage("Using IP  " + ipAddress);
                    NamedCommon.SourceDBIP = ipAddress;
                } catch (UnknownHostException e) {
                    if (showError) {
                        uCommons.uSendMessage("***********************************************************");
                        uCommons.uSendMessage("FATAL - cannot connect to SourceDB");
                        uCommons.uSendMessage("DB Error Type: Descr: " + e.getMessage());
                        uCommons.uSendMessage("***********************************************************");
                    }
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Connect_unics(): " + e.getMessage();
                }
            }

            if (NamedCommon.debugging) {
                uCommons.uSendMessage("   ). Create uSession object with :-");
                uCommons.uSendMessage("      ). dbhost  " + NamedCommon.dbhost);
                uCommons.uSendMessage("      ). dbport  " + NamedCommon.dbPort);
                uCommons.uSendMessage("      ). dbuser  " + NamedCommon.dbuser);
                uCommons.uSendMessage("      ). passwd  *********");
                uCommons.uSendMessage("      ). dbpath  " + NamedCommon.dbpath);
            }
            if (NamedCommon.SourceDBIP.equals("")) {
                NamedCommon.uSession.setHostName(NamedCommon.dbhost);
            } else {
                NamedCommon.uSession.setHostName(NamedCommon.SourceDBIP);
            }
            NamedCommon.uSession.setHostPort(Integer.parseInt(NamedCommon.dbPort));
            NamedCommon.uSession.setUserName(NamedCommon.dbuser);
            NamedCommon.uSession.setPassword(NamedCommon.passwd);
            NamedCommon.uSession.setAccountPath(NamedCommon.dbpath);
            String dbtype = NamedCommon.ServiceType;
            if (NamedCommon.ServiceType.equals("")) {
                if (NamedCommon.databaseType.equals("UNIVERSE")) {
                    dbtype = "uvcs";
                } else {
                    dbtype = "udcs";
                }
            }
            NamedCommon.uSession.setConnectionString(dbtype);
            if (NamedCommon.debugging) uCommons.uSendMessage("      ). UxCS  " + dbtype);
            try {
                NamedCommon.uSession.setTimeout(uTimeOut);
                if (NamedCommon.debugging) uCommons.uSendMessage("      ). T/out " + uTimeOut);
            } catch (UniSessionException e) {
                if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                status = e.getMessage();
                if (showError) uCommons.uSendMessage(status);
                abort = true;
            }

            if (!abort) {
                try {
                    lastAction = System.nanoTime();
                    NamedCommon.uSession.connect();
                    rightNow = System.nanoTime();
                    laps = (rightNow - lastAction) / div;
                    if ((laps * 1.25) > NamedCommon.ConnectAcceptable) {
                        if (NamedCommon.SourceDBIP.equals("")) {
                            InetAddress inetAddress = null;
                            try {
                                inetAddress = InetAddress.getByName(NamedCommon.dbhost);
                                String ipAddress = inetAddress.getHostAddress();
                                uCommons.uSendMessage("WARNING: SourceDB connect took more than " + NamedCommon.ConnectAcceptable + " seconds.");
                                uCommons.uSendMessage("WARNING: Will retry with IP");
                                NamedCommon.SourceDBIP = ipAddress;
                                connected = ReConnect();
                                if (!connected) {
                                    status = "<<FAIL>> SourceDB is connecting too slowly.";
                                    abort = true;
                                }
                            } catch (UnknownHostException e) {
                                // do nothing - too bad, it's slow and will probably crash
                            }
                        } else {
                            uCommons.uSendMessage("WARNING: SourceDB connection unacceptably slow. Issues are likely to occur.");
                            NamedCommon.SourceDBIP = "";
                        }
                    }
                    if (NamedCommon.debugging) {
                        port = NamedCommon.uSession.getHostPort();
                        host = NamedCommon.uSession.getHostName();
                        uCommons.uSendMessage("connected to " + host + " on port " + port);
                    }
                    connected = true;
                    abort = false;
                    if (NamedCommon.VOC == null) {
                        NamedCommon.sConnected = true;
                        u2Commons.OpenVOC();
                        if (NamedCommon.ZERROR) return "<<FAIL>>";
                    }
                } catch (UniSessionException e) {
                    NamedCommon.ZERROR = true;
                    if (showError) uCommons.uSendMessage("DB Error Type: [" + e.getErrorType() + "]   Code: [" + e.getErrorCode() + "]   Descr: " + e.getMessage());
                    abort = true;
                    tries = 0;
                }
            }
            tries--;
            if (tries <= 0) break;
        }
        if (abort) {
            if (showError) {
                uCommons.uSendMessage("ABORT: Failed to connect with SourceDB ------------------------");
            }
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "<<FAIL>> ABORT - source database is not available";
            status = NamedCommon.Zmessage;
            if (NamedCommon.ConnectionError) System.exit(1);
        }
        NamedCommon.showPID = false;
        NamedCommon.debugging = holdSW;
        return status;
    }


    public static boolean ReConnect() {
        boolean holdSW = NamedCommon.debugging;
        NamedCommon.debugging = NamedCommon.PoolDebug;

        if (NamedCommon.debugging) uCommons.uSendMessage("**");
        if (NamedCommon.debugging) uCommons.uSendMessage("SourceDB ReConnect service ----------------------------------------");

        String status = "";
        u2Commons.CloseAllFiles();
        dbTimeout = true;
        DisconnectSourceDB();
        dbTimeout = false;
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";
        status = ConnectSourceDB();

        if (status.contains("<<PASS>>")) {
            if (!NamedCommon.VOC.equals(null)) {
                u2Commons.uClose(NamedCommon.VOC);
                NamedCommon.VOC = null;
            }
            u2Commons.OpenVOC();
            // re-open the files.
            NamedCommon.uLoaded   = null;
            NamedCommon.uTake     = null;
            NamedCommon.uRequests = null;
            int nbrFiles = NamedCommon.OpenFiles.size();
            for (int f=0 ; f < nbrFiles ; f++) { NamedCommon.u2Handles.add(null); NamedCommon.fHandles.add(null); }
            String fname="";
            for (int of = 0; of < NamedCommon.OpenFiles.size(); of++) {
                try {
                    fname = NamedCommon.OpenFiles.get(of);
                    NamedCommon.U2File = NamedCommon.uSession.open(fname);
                    NamedCommon.u2Handles.set(of, NamedCommon.U2File);
                    if (fname.endsWith("LOADED"))  NamedCommon.uLoaded   = NamedCommon.U2File;
                    if (fname.endsWith("TAKE"))    NamedCommon.uTake     = NamedCommon.U2File;
                    if (fname.equals("uREQUESTS")) NamedCommon.uRequests = NamedCommon.U2File;
                    if (NamedCommon.debugging) uCommons.uSendMessage("re-opened " + NamedCommon.OpenFiles.get(of));
                } catch (UniSessionException e) {
                    if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
                    NamedCommon.u2Handles.set(of, null);
                }
            }
            for (int of = 0; of < NamedCommon.OpenFiles.size(); of++) {
                if (NamedCommon.u2Handles.get(of) == null) {
                    NamedCommon.u2Handles.remove(of);
                    NamedCommon.OpenFiles.remove(of);
                    of--;
                }
            }
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Reconnect failure";
        }
        
        if (NamedCommon.debugging) uCommons.uSendMessage("**");

        NamedCommon.debugging = holdSW;
        NamedCommon.sConnected = status.contains("<<PASS>>");
        return NamedCommon.sConnected;
    }

    public static void ReconnectService() {
        boolean isAlive = false, showErr=true;
        NamedCommon.sConnected = false;
        NamedCommon.cERROR = true;
        if (!NamedCommon.runSilent) {
            System.out.println(" ");
            System.out.println(" ");
            uCommons.uSendMessage("********************************************************************");
            uCommons.uSendMessage("Connection with SourceDB has been lost. Resilience mode: restarting ");
        }
        NamedCommon.runSilent = true;
        lostConnection = true;
        Properties runProps;
        int tries = 0, backoff = 0;
        while (!isAlive) {
            tries ++;
            if (tries % 20 == 0) uCommons.uSendMessage("Waiting on SourceB connection.");

            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage = "";

            backoff += 5;
            uCommons.uSendMessage("wait "+uCommons.RightHash(String.valueOf(backoff), 3)+" seconds before re-trying");
            uCommons.Sleep(backoff);
            if (backoff >= 30) backoff = 0;
            isAlive = SourceDB.ReConnect();
        }
        lostConnection = false;
        NamedCommon.runSilent = false;
        NamedCommon.ConnectionError = false;
        uCommons.uSendMessage("Connection with SourceDB has been re-established.");
        uCommons.uSendMessage("********************************************************************");
        System.out.println(" ");
        System.out.println(" ");
        NamedCommon.cERROR = false;
    }

    private static String Connect_uMount() {
        String status = "";
//        NamedCommon.rcon.disconnect();
        return status;
    }

    private static String Connect_uSocket() {
        String status = "<<FAIL>>";
        String[] mainArgs = new String[]{String.valueOf(NamedCommon.mqPort), NamedCommon.mqHost};
        if (uSocketCommons.main(mainArgs)) {
            String ack = uSocketCommons.SocketReader(uSocketCommons.readerSocket);
            if (ack.equals("{STX}")) status = "<<PASS>>";
        }
        return status;
    }

    public static void SetDBtimeout(boolean flag) {
        dbTimeout = flag;
    }

    public static void DisconnectSourceDB() {

        boolean holdSW = NamedCommon.debugging;
        NamedCommon.debugging = NamedCommon.PoolDebug;

        if (!NamedCommon.sConnected) {
            if (NamedCommon.uSession != null) try { NamedCommon.uSession.disconnect(); NamedCommon.uSession = null; } catch (Exception e) { if (!NamedCommon.runSilent) uCommons.uSendMessage(e.getMessage()); }
            if (NamedCommon.uJava != null) try { NamedCommon.uJava.closeAllSessions(); NamedCommon.uJava = null;    } catch (Exception e) { if (!NamedCommon.runSilent) uCommons.uSendMessage(e.getMessage()); }
            return;
        }
        if (!dbTimeout && NamedCommon.CPL && !NamedCommon.masterStop) return;

        if (NamedCommon.protocol.equals("u2cs")) {
            if (NamedCommon.debugging) uCommons.uSendMessage("SourceDB.DisconnectSourceDB.start() " + NamedCommon.uSession.getHostName());
            if (!NamedCommon.uSession.isActive()) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "FATAL: the SourceDB connection has become inactive.";
                NamedCommon.ConnectionError = true;
                return;
            }
        }

        if (NamedCommon.VOC != null && NamedCommon.PointerFiles.size() > 0) u2Commons.CloseAllFiles();

        switch (NamedCommon.protocol) {
            case "u2cs":
                Disconnect_unics();
                break;
            case "real":
                Disconnect_Reality();
                break;
            case "u2mnt":
                Disconnect_uMount();
                break;
            case "u2sockets":
                Disconnect_uSocket();
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
        }

        NamedCommon.sConnected = false;
        NamedCommon.debugging = holdSW;
        if (NamedCommon.debugging) uCommons.uSendMessage("SourceDB.DisconnectSourceDB.end() ");
    }

    private static String Disconnect_Reality() {
        String status = "";
        uCommons.uSendMessage("SourceDB.Disconnect_Reality() -----------------------------------");
        if (NamedCommon.rcon != null) NamedCommon.rcon.disconnect();
        return status;
    }

    public static void Disconnect_unics() {
        if (!NamedCommon.sConnected) {
            uCommons.uSendMessage("SourceDB is not connected.");
            return;
        }
        try {
            int openfiles = NamedCommon.uSession.getNumOpenFiles();
            if (openfiles > 0) {
                uCommons.uSendMessage("=====================================================");
                uCommons.uSendMessage("WARNING: " + openfiles + " files are still open.");
                uCommons.uSendMessage("=====================================================");
            }
            if (NamedCommon.debugging || NamedCommon.masterStop) {
                uCommons.uSendMessage("-----------------------------------------------------");
                uCommons.uSendMessage("SourceDB connection is shutting down;");
            }
            if (NamedCommon.U2File != null) {
                NamedCommon.U2File = u2Commons.uClose(NamedCommon.U2File);
            }
            if (NamedCommon.uRequests != null) {
                NamedCommon.uRequests = u2Commons.uClose(NamedCommon.uRequests);
            }
            if (NamedCommon.VOC != null) {
                NamedCommon.VOC = u2Commons.uClose(NamedCommon.VOC);
            }
            if (NamedCommon.uLoaded != null) {
                NamedCommon.uLoaded = u2Commons.uClose(NamedCommon.uLoaded);
            }
            if (NamedCommon.uTake != null) {
                NamedCommon.uTake = u2Commons.uClose(NamedCommon.uTake);
            }

            NamedCommon.U2File = null;
            NamedCommon.VOC = null;
            NamedCommon.uRequests = null;
            NamedCommon.uLoaded = null;
            NamedCommon.uTake = null;

            if (NamedCommon.debugging) uCommons.uSendMessage("Closing uniSession");
            NamedCommon.uJava.closeSession(NamedCommon.uSession);
            if (NamedCommon.debugging) uCommons.uSendMessage("Closing uniJava");
            NamedCommon.uJava.closeAllSessions();
            if (NamedCommon.debugging) uCommons.uSendMessage("Disconnecting");
            NamedCommon.uSession.disconnect();
            NamedCommon.uSession = null;
            NamedCommon.uJava = null;
            if (NamedCommon.debugging || NamedCommon.masterStop) uCommons.uSendMessage("   ). uSession disconnected");
            if (NamedCommon.debugging || NamedCommon.masterStop) uCommons.uSendMessage("   ). uJava all sessions closed");
            if (NamedCommon.debugging || NamedCommon.masterStop) uCommons.uSendMessage("-----------------------------------------------------");
        } catch (UniSessionException | UniRPCConnectionException e) {
            if (!NamedCommon.runSilent) uCommons.uSendMessage("SourceDB: UniSession error on closing: " + e.getMessage());
            if (!NamedCommon.runSilent) uCommons.uSendMessage("SourceDB has been lost.");
            if (e.getMessage().contains(" RPC ")) NamedCommon.ConnectionError = true;
        }
        NamedCommon.sConnected = false;
        NamedCommon.IsAvailableU2 = false;
    }

    private static void Disconnect_uMount() {
    }

    private static void Disconnect_uSocket() {
        // send a good-bye transmission
        uSocketCommons.readerSocket = uSocketCommons.CloseComms(uSocketCommons.readerSocket);
        uSocketCommons.writerSocket = uSocketCommons.CloseComms(uSocketCommons.writerSocket);
    }

}
