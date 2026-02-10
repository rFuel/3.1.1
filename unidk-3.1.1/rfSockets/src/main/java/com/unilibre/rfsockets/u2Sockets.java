package com.unilibre.rfsockets;
/*  Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.cipher.uCipher;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.*;
import java.net.Socket;

public class u2Sockets {

    final static String etx = "<ETX>";
    private static int mqPort = 0;
    private static String mqHost = "";
    private static int writerPort;
    private static int readerPort;
    private static String inMsg = "";
    private static String inData = "";
    private static Socket mainSocket = null;
    public static Socket readerSocket = null;
    public static Socket writerSocket = null;
    private static PrintWriter outbound = null;
    private static BufferedReader inbound = null;
    private static InputStreamReader instream = null;
    private static boolean ZERROR = false;
    private static boolean clearText = true;
    private static boolean debugInfo = false;

    public static boolean main(String[] args) {
        mqPort = Integer.valueOf(args[0]);
        mqHost = args[1];
        return ConnectSession();
    }

    public static boolean ConnectSession() {
        // Connect -----------------------------------------
        if (!ZERROR) System.out.print("mainSocket ------------------------ " + mqPort + " ");
        if (!ZERROR) mainSocket = ConnectSocket(mqHost, mqPort);
        if (!ZERROR) printSocketInformation(mainSocket);
        // ------------------------------------------------
        // u2 will immediately direct us to a port-pair. --
        //    uniSockets is a UniLibre thing !! it uses BASIC programs (strns)
        //    to communicate across private sockets (need to try TLS)
        // ------------------------------------------------

        SocketWriter(mainSocket, "Hello Andy");

        int nextPort = mqPort;
        if (!ZERROR) {
            inMsg = SocketReader(mainSocket);
            // e.g. {58558} -----------------------------------
            inMsg = inMsg.replace("{", "");
            inMsg = inMsg.replace("}", "");
            try {
                nextPort = Integer.valueOf(inMsg);
            } catch (NumberFormatException e) {
                System.out.println("!! ABORT {82000} - invalid Port");
                return true;
            }

            if (nextPort <= mqPort) {
                System.out.println("!! ABORT {82010} - invalid Connection");
                return true;
            }
        }

        // set the port pairs (writer / reader) -----------
        if (!ZERROR) writerPort = nextPort;
        if (!ZERROR) readerPort = writerPort + 1;

        // plug java writer into u2 reader
        if (!ZERROR) System.out.print("writerSocket ---------------------- " + writerPort + " ");
        if (!ZERROR) writerSocket = ConnectSocket(mqHost, writerPort);
        if (!ZERROR && debugInfo) printSocketInformation(writerSocket);

        // plug java reader into u2 writer
        if (!ZERROR) System.out.print("readerSocket ---------------------- " + readerPort + " ");
        if (!ZERROR) readerSocket = ConnectSocket(mqHost, readerPort);
        if (!ZERROR && debugInfo) printSocketInformation(readerSocket);

        // -------------------------------------------------
        boolean okay = true;
        if (ZERROR) okay = false;
        return okay;
    }

    private static Socket ConnectSocket(String host, int port) {
        // Test to see if I can use the SocketFacory - YES I CAN !!
        SocketFactory socketFactory = SocketFactory.getDefault();
        Socket socket = null, s=null;
        try {
            s = socketFactory.createSocket(host, port);
            socket = new Socket(host, port);
            System.out.println("Status     PASS");
        } catch (IOException e) {
            System.out.println("Status     FAIL");
            System.out.println(e.getMessage());
            ZERROR = true;
        }
//        return socket;
        return s;
    }

    public static String SocketReader(Socket socket) {
        String inStr;
        String message = "";
        try {
            instream = new InputStreamReader(socket.getInputStream());
            inbound = new BufferedReader(instream);
            inStr = inbound.readLine();
            while (!inStr.equals(etx)) {
                if (!clearText) inStr = uCipher.Decrypt(inStr);
                message += inStr;
                inStr = inbound.readLine();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
            ZERROR = true;
        }
        if (!clearText && !message.substring(0, 1).equals("{")) message = uCipher.Decrypt(message);
        if (message.indexOf("<nl>") > 0) {
            message = message.replaceAll("<nl>", "\n");
        }
        return message;
    }

    public static void SocketWriter(Socket socket, String msg) {
        String send = msg.replaceAll("\n", "");
        System.out.println("Sending: [" + send + "]");
        if (!clearText) send = uCipher.Encrypt(send);
        send += "\n" + etx + "\n";
        try {
            outbound = new PrintWriter(socket.getOutputStream(), true);
            outbound.println(send);
        } catch (IOException e) {
            System.out.println(e.getMessage());
            ZERROR = true;
        }
    }

    public static Socket CloseComms(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
                socket = null;
            } catch (IOException e) {
                System.out.println(e.getMessage());
                ZERROR = true;
            }
        }
        return socket;
    }

    public static void printSocketInformation(Socket socket) {
        System.out.println(" ");
        System.out.println("Remote " + socket.getRemoteSocketAddress());
        System.out.println("Local  " + socket.getLocalSocketAddress());
    }

    public static void secureSocketTests() {
        int serverPort = 31438;
        String host = "54.153.142.191";
        System.out.println("Connecting to host " + host + " at port " + serverPort);

        SocketFactory socketFactory = SSLSocketFactory.getDefault();
        // SocketFactory socketFactory = SocketFactory.getDefault();

        Socket  s = null;
        try {
            s = socketFactory.createSocket(host, serverPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte buffer[] = new byte[4096];
        int bytesRead;

        try {
            // This server just echoes back what you send it.
            BufferedInputStream is = new BufferedInputStream(s.getInputStream());
            BufferedOutputStream os = new BufferedOutputStream(s.getOutputStream());
            while ((bytesRead = is.read(buffer)) > 0) {
                os.write(buffer, 0, bytesRead);
                os.flush();
            }
            s.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
