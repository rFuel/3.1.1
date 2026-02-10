package com.unilibre.cdroem;

import com.northgateis.reality.rsc.RSCConnection;
import javax.net.ssl.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class commons {
    
    public static String pid = ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0];
    public static String slash = "/", BaseCamp = "./cdr";
    public static String Zmessage, url, usr, pwd, que, clientID;
    public static String pxHost, pxDbase, pxDbuser, pxDbacct, VTopic;
    public static String listeningOn = "PID: [" + pid + "] is lisenting for messages.";
    public static String Broker = "", logHeader = "";
    public static String msgIn = "Message  <><><><><><><><><><><><><><><><><><><><><><><><><><><>";
    public static String appjson = "application/json";
    public static String[] csvList = {};
    public static boolean ZERROR = false, DBconnected=false, debugging=false;;
    public static LocalDate Day0  = LocalDate.of(1967, Month.DECEMBER, 31);
    public static int VListeners=2, mqHeartBeat=120;
    public static long Expiry=10000;
    public static ArrayList<String> propKeys = new ArrayList<>();
    public static ArrayList<String> propVals = new ArrayList<>();
    public static ArrayList<String> SubsList = new ArrayList<>();
    public static ArrayList<String> DataList = new ArrayList<>();
    public static ArrayList<String> Templates = new ArrayList<>();
    public static ArrayList<String> TmplList = new ArrayList<>();
    public static ArrayList<String> keystoreNames = new ArrayList<>();
    public static ArrayList<String> keystoreLocns = new ArrayList<>();
    public static ArrayList<String> ReturnCodes = new ArrayList<>();
    private static String keystoreName = "server.keystore";
    private static String truststoreName = "server.truststore";
    public static SSLContext sslContext;
    public static RSCConnection rcon = null;
    
    public static void uSendMessage(String inMsg) {
        if (inMsg == null) return;
        logger.logthis(inMsg);
    }

    public static String GetValue(String inValue, String def) {
        int kPos = propKeys.indexOf(inValue.toUpperCase());
        String confV = "";
        if (kPos >= 0) confV = propVals.get(kPos);
        String value = System.getProperty(inValue, "");
        if (value.equals(null) || value.equals("")) value = confV;
        if (value.equals(null) || value.equals("")) value = def;
        return value;
    }

    
    public static void SetCommons(Properties confProps) {
        if (commons.ZERROR) return;
        // -------------------[ local PROXY DB credentials ]---------------------
        pxHost     = confProps.getProperty("pxIP", "ERROR");
        pxDbase    = confProps.getProperty("pxDB", "ERROR");
        pxDbuser   = confProps.getProperty("pxUP", "ERROR");
        pxDbacct   = confProps.getProperty("pxAC", "ERROR");
        propKeys.clear();
        propVals.clear();
        Set<Object> keys = confProps.keySet();
        String k, v;
        for (Object key: keys) {
            k = (String)key;
            if (k.startsWith("#")) continue;
            v = confProps.getProperty(k);
            propKeys.add(k.toUpperCase());
            propVals.add(v);
        }
        keys = null;
    }

    public static void SetupReturnCodes() {
        for (int rc = 0; rc < 1100; rc++) { ReturnCodes.add(""); }
        int nbrvals;
        String hCodes = ReadDiskRecord(BaseCamp + slash + "http-return-codes.csv");
        if (!hCodes.equals("")) {
            String[] tmp = hCodes.split("\\r?\\n");
            nbrvals = tmp.length;
            int idx;
            for (int rc = 0; rc < nbrvals; rc++) {
                String[] tmp1 = tmp[rc].split(",");
                try {
                    idx = Integer.valueOf(tmp1[0]);
                    ReturnCodes.set(idx, tmp1[1]);
                } catch (NumberFormatException nfe) {
                    // skip the line
                }
            }
        }
    }

    public static Properties LoadProperties(String fname) {
        Properties props = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(fname);
        } catch (FileNotFoundException e) {
            ZERROR = true;
            Zmessage = "ERROR: Cannot find config item: " + fname;
            logger.logthis(logHeader +  Zmessage);
            return null;
        }
        if (is != null) {
            try {
                props.load(is);
            } catch (IOException e) {
                logger.logthis(logHeader +  "ERROR: Cannot load properties in file: " + fname);
                ZERROR = true;
            } catch (IllegalArgumentException iae) {
                logger.logthis(logHeader +  "FATAL:" + iae.getMessage());
                System.exit(0);
            }
            try {
                is.close();
                is = null;
            } catch (IOException e) {
                logger.logthis(logHeader +  "Cannot close '" + fname + "'  " + e.getMessage());
            }
        } else {
            logger.logthis(logHeader +  "Please load '" + fname + "'");
        }
        return props;
    }
    
    public static SSLContext CreateContext(KeyStore kstore, KeyStore tstore) throws Exception {

        if (debugging) uSendMessage("Creating SSLContext using KeyStore " + keystoreName);
        KeyManager[] keyManagers;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try {
            String ks = GetValue(keystoreName, "");
            keyManagerFactory.init(kstore, password(ks));
        } catch (UnrecoverableKeyException e) {
            uSendMessage(">>>");
            uSendMessage(">>> SECURITY ABORT: SSLContext for KeyStore: invalid password ");
            uSendMessage(">>>");
            System.exit(1);
        }
        keyManagers = keyManagerFactory.getKeyManagers();

        if (debugging) uSendMessage("Creating SSLContext using TrustStore " + truststoreName);
        TrustManager[] trustManagers;
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(tstore);
        trustManagers = trustManagerFactory.getTrustManagers();

        sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, null);
        if (debugging) uSendMessage("SSLContext created using TLS ");

        return sslContext;
    }
    
    public static char[] password(String name) {
        String pw = name + ".password";
        if (keystoreNames.indexOf(name) < 0) {
            uSendMessage("     location for [" + name + ".password] has not been provided.");
            System.exit(1);
        } else {
            pw = keystoreLocns.get(keystoreNames.indexOf(name)) + ".password";
        }
        String pword = ReadDiskRecord(pw);
        if (pword.endsWith("\n")) pword = pword.replaceAll("\n","");  // Bloody Windows hack !!
        // decrypt pw HERE //
        if (ZERROR) System.exit(1);
        return pword.toCharArray();
    }
    
    public static KeyStore loadKeyStore(String storeLoc) throws Exception {
        if (debugging) uSendMessage("Creating KeyStore object for [" + storeLoc + "]");
        if (keystoreNames.indexOf(storeLoc) < 0) {
            keystoreNames.add(storeLoc);
            keystoreLocns.add(storeLoc);
        }
        final InputStream stream;
        if (storeLoc == null) {
            throw new RuntimeException("Could not load keystore - Not provided in config properties");
        } else {
            stream = Files.newInputStream(Paths.get(storeLoc));
        }

        if (stream == null) {
            throw new RuntimeException("Could not load keystore [" + storeLoc + "]");
        }
        try (InputStream is = stream) {
            KeyStore loadedKeystore = KeyStore.getInstance("JKS");
            loadedKeystore.load(is, password(storeLoc));
            return loadedKeystore;
        }
    }

    public static String ReadDiskRecord(String infile) {
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
                uSendMessage("read FAIL on " + infile);
                uSendMessage(e.getMessage());
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    uSendMessage("read FAIL on " + infile);
                    uSendMessage(e.getMessage());
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                uSendMessage("File Close FAIL on " + infile);
                uSendMessage(e.getMessage());
            }
        } catch (IOException e) {
            uSendMessage("-------------------------------------------------------------------");
            uSendMessage("File Access FAIL :: " + infile);
            uSendMessage(e.getMessage());
            uSendMessage("-------------------------------------------------------------------");
            ZERROR = true;
            Zmessage = e.getMessage();
        }
        return rec;
    }

    public static String ResponseHandler(String status, String descr, String response, String esbFMT) {
        String replyMessage = response;
        String stdDesc = commons.ReturnCodes.get(Integer.valueOf(status));
        if (!response.equals("")) {
            if (response.startsWith("{")) {
                if (replyMessage.startsWith("{\"body\":")) {
                    replyMessage = response;
                } else {
                    replyMessage = "{\"body\": {\"status\": \"" + status + "\",\"message\": \"" + stdDesc + "\",\"response\": \"" + response + "\"}}";
                }
            } else {
                replyMessage = "{\"body\": {\"status\": \"" + status + "\",\"message\": \"" + stdDesc + "\",\"response\": \"" + response + "\"}}";
            }
        } else {
            replyMessage = "{\"body\": {\"status\": \"" + status + "\",\"message\": \"" + stdDesc + "\",\"response\": \"" + response + "\"}}";
        }
        return replyMessage;
    }

    public static String ScrubText(String zmessage) {
        String text = "";
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<zmessage.length(); i++) {
            if (((int)zmessage.charAt(i))<128) sb.append(zmessage.charAt(i));
        }
        text = sb.toString();
        while (text.contains("  ")) { text = text.replace("  ", " "); }
        return  text;
    }

}
