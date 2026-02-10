package com.unilibre.webDS;

import javax.net.ssl.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.util.ArrayList;
import java.util.Properties;

public class commons {
    public static String logHeader = "";
    public static String BaseCamp = "cdr";
    public static boolean debugging=false;;
    public static long Expiry=10000;
    public static ArrayList<String> keystoreNames = new ArrayList<>();
    public static ArrayList<String> keystoreLocns = new ArrayList<>();
    private static String keystoreName = "server.keystore";
    private static String truststoreName = "server.truststore";
    public static SSLContext sslContext;

    public static String GetValue(String inValue, String def) {
        String value = System.getProperty(inValue, def);
        if (value == null || value.equals("")) value = def;
        return value;
    }

    public static Properties LoadProperties(String fname) throws IOException {
        Properties props = new Properties();
        InputStream is;
        try {
            is = new FileInputStream(fname);
        } catch (FileNotFoundException e) {
            logger.logthis(logHeader +  "ERROR: Cannot find config item: " + fname);
            throw e;
        }

        try {
            props.load(is);
        } catch (IOException e) {
            logger.logthis(logHeader +  "ERROR: Cannot load properties in file: " + fname);
            throw e;
        } catch (IllegalArgumentException iae) {
            logger.logthis(logHeader +  "FATAL:" + iae.getMessage());
            throw iae;
        }
        try {
            is.close();
        } catch (IOException e) {
            logger.logthis(logHeader +  "Cannot close '" + fname + "'  " + e.getMessage());
        }
        return props;
    }
    
    public static SSLContext CreateContext(KeyStore kstore, KeyStore tstore) throws Exception {

        if (debugging) logger.uSendMessage("Creating SSLContext using KeyStore " + keystoreName);
        KeyManager[] keyManagers;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try {
            keyManagerFactory.init(kstore, password(keystoreName));
        } catch (UnrecoverableKeyException e) {
            logger.uSendMessage(">>>");
            logger.uSendMessage(">>> SECURITY ABORT: SSLContext for KeyStore: invalid password ");
            logger.uSendMessage(">>>");
            System.exit(1);
        }
        keyManagers = keyManagerFactory.getKeyManagers();

        if (debugging) logger.uSendMessage("Creating SSLContext using TrustStore " + truststoreName);
        TrustManager[] trustManagers;
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(tstore);
        trustManagers = trustManagerFactory.getTrustManagers();

        sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, null);
        if (debugging) logger.uSendMessage("SSLContext created using TLS ");

        return sslContext;
    }
    
    public static char[] password(String name) throws FileNotFoundException {
        String pw = name + ".password";
        if (keystoreNames.indexOf(name) < 0) {
            logger.uSendMessage("     location for [" + name + ".password] has not been provided.");
            System.exit(1);
        } else {
            pw = keystoreLocns.get(keystoreNames.indexOf(name)) + ".password";
        }
        String pword = ReadDiskRecord(pw);
        if (pword.endsWith("\n")) pword = pword.replaceAll("\n","");  // Bloody Windows hack !!
        // decrypt pw HERE //
        return pword.toCharArray();
    }
    
    public static KeyStore loadKeyStore(String name) throws Exception {
        if (debugging) logger.uSendMessage("Creating KeyStore object for [" + name + "]");
        String storeLoc = System.getProperty(name);
        if (keystoreNames.indexOf(name) < 0) {
            keystoreNames.add(name);
            keystoreLocns.add(storeLoc);
        }
        final InputStream stream;
        if (storeLoc == null) {
            stream = cdrServer.class.getResourceAsStream(name);
        } else {
            stream = Files.newInputStream(Paths.get(storeLoc));
        }

        if (stream == null) {
            throw new RuntimeException("Could not load keystore [" + name + "]");
        }
        try (InputStream is = stream) {
            KeyStore loadedKeystore = KeyStore.getInstance("JKS");
            loadedKeystore.load(is, password(name));
            return loadedKeystore;
        }
    }

    public static String ReadDiskRecord(String infile) throws FileNotFoundException {
        System.out.println("ReadDiskRecord: " + infile);
        String rec = "";
        BufferedReader BRin = null;
        FileReader fr = null;
        try {
            fr = new FileReader(infile);
            BRin = new BufferedReader(fr);
            String line = null;
            try {
                line = BRin.readLine();
            } catch (IOException e) {
                logger.uSendMessage("read FAIL on " + infile);
                logger.uSendMessage(e.getMessage());
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    logger.uSendMessage("read FAIL on " + infile);
                    logger.uSendMessage(e.getMessage());
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                logger.uSendMessage("File Close FAIL on " + infile);
                logger.uSendMessage(e.getMessage());
            }
        } catch (IOException e) {
            logger.uSendMessage("-------------------------------------------------------------------");
            logger.uSendMessage("File Access FAIL :: " + infile);
            logger.uSendMessage(e.getMessage());
            logger.uSendMessage("-------------------------------------------------------------------");
            throw e;
        }
        return rec;
    }

    public static String ResponseHandler(String status, String descr, String response) {
        String replyMessage = response;
        if (response == null || !replyMessage.startsWith("{\"body\":")) {
            String responseStr = response == null ? "NULL" : response;
            replyMessage = "{\"body\": {\"status\": \"" + status + "\",\"message\": \"" + descr + "\",\"response\": \"" + responseStr + "\"}}";
        }
        return replyMessage;
    }

}
