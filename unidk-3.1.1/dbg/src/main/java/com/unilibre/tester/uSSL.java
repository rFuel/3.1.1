package com.unilibre.tester.tester;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniobjects.UniSession;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;

import javax.net.ssl.*;
import java.io.*;
import java.security.*;
import java.security.cert.CertificateException;

public class uSSL {
    private static String u2Host;
    private static int u2Port;
    private static String u2Cmd;
    private static String answer = "";
    public static UniSession uSession;

    private static SSLSocket u2Connect(String[] args) {
        DecodeArgs(args);
        return jSSLServer();
    }

    private static SSLSocket jSSLServer() {
        // to make this work - must have a keystore - on the uv host        
        // to generate it, use cygwin for windows or putty for unix         
        //    keytool -genkey -alias signFiles -keystore examplestore       
        // ---------------------------------------------------------------- 
        String ksPath = NamedCommon.DevCentre+ "\\ssl\\host\\rfuel_unilibre_com_au.jks";
        String sslPwd = uCommons.ReadDiskRecord(ksPath+".password").replaceAll("\\r?\\n", "");
        if (NamedCommon.ZERROR) System.exit(0);
        if (sslPwd.startsWith("ENC(")) {
            sslPwd = sslPwd.substring(4, (sslPwd.length() - 1));
            sslPwd = uCipher.Decrypt(sslPwd);
        }
//        char[] sslPassword = "passw0rd".toCharArray();
        char[] sslPassword = sslPwd.toCharArray();
        if (NamedCommon.debugging) {
            System.setProperty("javax.net.debug", "ssl:handshake:verbose:keymanager:trustmanager");
            System.setProperty("java.security.debug", "access:stack");
        }

        KeyStore keyStore = null;
        InputStream is = null;
        KeyManagerFactory kmf = null;
        TrustManagerFactory tmf = null;

        try {
            keyStore = KeyStore.getInstance("JKS");
            try {
                try (InputStream tis = new FileInputStream(ksPath)) {
                    try {
                        keyStore.load(tis, sslPassword);
                        /*  this assignment may not work */
                        is = tis;
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.exit(1);
                    } catch (NoSuchAlgorithmException nsae) {
                        nsae.printStackTrace();
                        System.exit(1);
                    } catch (CertificateException ce) {
                        ce.printStackTrace();
                        System.exit(1);
                    }
                }
            } catch (IOException ise) {
                ise.printStackTrace();
                System.exit(1);
            }
        } catch (KeyStoreException kse) {
            kse.printStackTrace();
            System.exit(1);
        }

        try {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            try {
                kmf.init(keyStore, sslPassword);
            } catch (KeyStoreException kse) {
                kse.printStackTrace();
                System.exit(0);
            } catch (UnrecoverableKeyException uke) {
                uke.printStackTrace();
                System.exit(1);
            }
        } catch (NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
            System.exit(1);
        }

        try {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);
        } catch (KeyStoreException kse) {
                kse.printStackTrace();
                System.exit(0);
        } catch (NoSuchAlgorithmException nsae) {
            nsae.printStackTrace();
            System.exit(1);
        }

        SSLSocket sslsocket = null;
        SSLSocketFactory sslsocketfactory = null;
        try {
            try {
                String sslVersion = "TLSv1.2";
//                SSLContext sslctx = SSLContext.getInstance("SSLv3");
                SSLContext sslctx = SSLContext.getInstance(sslVersion);
                System.out.println(sslVersion + " provider is " + sslctx.getProvider().toString());
                KeyManager[] keyManagers = kmf.getKeyManagers();
                TrustManager[] trustManagers = tmf.getTrustManagers();
                SecureRandom secureRandom = new SecureRandom();
                try {
                    sslctx.init(keyManagers, trustManagers, secureRandom);
                    sslsocketfactory = sslctx.getSocketFactory();
                } catch (KeyManagementException kme) {
                    kme.printStackTrace();
                    System.exit(0);
                }
            } catch (NoSuchAlgorithmException nsae) {
                nsae.printStackTrace();
                System.exit(0);
            }

//            SSLSocketFactory sslsocketfactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
            System.out.println(" ");
            System.out.println(" ");
            System.out.println("***************************************************************************");
            System.out.println("Connecting with " + u2Host + " on port " + u2Port);
            sslsocket = (SSLSocket) sslsocketfactory.createSocket(u2Host, u2Port);
            sslsocket.startHandshake();
        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }
        return sslsocket;
    }

    private static void DecodeArgs(String[] args) {
        int nbrArgs = args.length;
        String tmp;
        String[] junk;
        for (int i = 0; i < nbrArgs; i++) {
            tmp = args[i];
            if (tmp.contains("=")) {
                junk = tmp.split("=");
                switch (junk[0]) {
                    case "u2host":
                        u2Host = junk[1];
                        break;
                    case "u2port":
                        u2Port = Integer.valueOf(junk[1]);
                        break;
                    case "command":
                        u2Cmd = junk[1];
                        break;
                }
            }
        }
//        u2Host = "localhost";
//        u2Port = 58555;
//        u2Cmd = "COUNT VOC SAMPLE 2";
    }

    private static String GetResponse(BufferedReader bufferedreader) {
        answer = "";
        String inStr;
        try {
            // read the response -------------------------------
            inStr = bufferedreader.readLine();
            while (inStr != null && !inStr.equals("")) {
                answer += inStr;
                inStr = bufferedreader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        inStr = null;
        return answer;
    }

    public static void main (String[] args) throws IOException {
        SSLSocket ssl = u2Connect(args);
        // ---------------------------------------------------------------------
//        InputStream inputstream = System.in;
        InputStream inputstream = ssl.getInputStream();
        InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
        BufferedReader bufferedreader = new BufferedReader(inputstreamreader);

        OutputStream outputstream = null;
        PrintWriter out = null;
        try {
            outputstream = ssl.getOutputStream();
            OutputStreamWriter sw = new OutputStreamWriter(outputstream);
            BufferedWriter bw = new BufferedWriter(sw);
            out = new PrintWriter(bw);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }

        // receive the welcome message ---------------------

        answer = GetResponse(bufferedreader);

        // send the command --------------------------------

//        byte[] bytMsg = u2Cmd.getBytes();
//        outputstream.write(bytMsg);

        while (!u2Cmd.equals("")) {
            out.write(u2Cmd);
            System.out.println("    Sent " + u2Cmd);
            answer = GetResponse(bufferedreader);
            System.out.println("Received " + answer);
            u2Cmd = "";
            System.out.print("Enter u2Cmd: ");
            BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
            try {
                u2Cmd = keyboard.readLine();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
    }

}
