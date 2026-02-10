package com.unilibre.restful;


import com.unilibre.cipher.uCipher;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import com.unilibre.core.uConnector;
import com.unilibre.restful.Http2Server;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.json.JSONObject;

import javax.net.ssl.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

public class methods {

    private static String appjson = "application/json";
    public static ArrayList<String> keystoreNames = new ArrayList<>();
    public static ArrayList<String> keystoreLocns = new ArrayList<>();
    private static String keystoreName = "server.keystore";
    private static String truststoreName = "server.truststore";
    public static SSLContext sslContext;
    public static Properties props = new Properties();
    private static String bindAddess = "";
    private static String protocol = "https";
    private static String URLhost;
    private static int URLport;
    private static String URLpath;

    public static void uSendMessage(String inMsg) {
        if (inMsg == null) return;
        String mTime, mDate, MSec;
        String iam = ""; //""{" + (Thread.currentThread().getName().replaceAll("\\ ", "-")) + "} ";
        int ThisMS;
        mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
        if (NamedCommon.ZERROR) {
            mDate = "**** ERROR";
        } else {
            mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
        }
        ThisMS = Calendar.getInstance().get(Calendar.MILLISECOND);
        MSec = "." + (ThisMS + "000").substring(0, 3);
        System.out.println(mDate + " " + mTime + MSec + " " + inMsg);
        mTime = "";
        mDate = "";
        MSec = "";
        ThisMS = 0;
        inMsg = "";
    }

    public static void GetDomain() throws IOException {
        if (System.getProperty("urlhost") != null) {
            NamedCommon.hostname = System.getProperty("urlhost", "localhost");
            URLhost = NamedCommon.hostname;
        } else {
            String s, e, domain = "", error = "";
            String cmd = "hostname --fqdn";
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader es = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while ((s = is.readLine()) != null) {
                domain += s;
            }
            while ((e = es.readLine()) != null) {
                error += e;
            }
        }
        if (NamedCommon.hostname.equals("UniLibre01") || NamedCommon.BaseCamp.toLowerCase().contains("/home/andy")) {
            uSendMessage(" ");
            uSendMessage("***************************************************************");
            uSendMessage("Reset BaseCamp to rfuel22");
            uSendMessage("***************************************************************");
            uSendMessage(" ");
            NamedCommon.hostname = "localhost";
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
        }
        if (System.getProperty("urlport") != null) {
            try {
                URLport = Integer.valueOf(System.getProperty("urlport"));
            } catch (NumberFormatException nfe) {
                uSendMessage("Invalid parameter urlport="+System.getProperty("urlport")+" - MUST be an integer.");
            }
        }
        if (System.getProperty("urlpath") != null) {
            URLpath = System.getProperty("urlpath");
        }
    }

    public static String getURLhost() {
        return URLhost;
    }

    public static String getURLpath() {
        return URLpath;
    }

    public static String getBindAddess() {
        bindAddess = protocol + "://" + URLhost + ":" + URLport;
        if (!URLpath.startsWith("/")) bindAddess += "/";
        bindAddess += URLpath;
        return bindAddess;
    }

    public static int getURLport() {
        return URLport;
    }

    public static void SecureProcess() throws Exception {
//        GetDomain();

        uSendMessage(" ");
        uSendMessage("---------------------------- [ SecureProcess() Initialising ] ---------------------------- ");

        int servePort;
        try {
            servePort = Integer.valueOf(GetValue("urlport", "8188"));
        } catch (NumberFormatException e) {
            uSendMessage("ERROR in urlport setting - not an integer. Default to 8188");
            servePort = 8188;
        }

        String host = GetValue("urlhost", NamedCommon.hostname);
        String path = GetValue("urlpath", "localhost");
        URLpath = path;

        KeyStore ks = methods.loadKeyStore("server.keystore");
        KeyStore ts = methods.loadKeyStore("server.truststore");
        SSLContext sslContext = methods.CreateContext(ks, ts);

        Properties props = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(props);

        boolean URLnotset = false;
        if (URLpath == null) URLnotset = true;
        if (URLpath == "") URLnotset = true;
        if (URLnotset) {
            //
            // default URL is in rFuel.properties
            //
            String checkURL = props.getProperty("serveurl", "");
            if (!checkURL.equals("")) URLpath = checkURL;

            //
            // URL can be over-riden by script property
            //
            checkURL = GetValue("urlpath", "");
            if (!checkURL.equals("")) URLpath = checkURL;
        }

        URLhost = NamedCommon.hostname;
        URLport = servePort;
        bindAddess = protocol + "://" + URLhost + ":" + URLport;
        if (!URLpath.startsWith("/")) bindAddess += "/";
        bindAddess += URLpath;

        uSendMessage("-------------------------------------------------------------------------------------------------------------");
        uSendMessage(" ");

        uSendMessage("Accepting requests on " + bindAddess);

    }

    public static void StandardProcess() throws Exception {
        GetDomain();
        protocol = "http";
        uSendMessage(" ");
        uSendMessage("---------------------------- [ StandardProcess() Initialising ] ---------------------------- ");

        int servePort;
        try {
            servePort = Integer.valueOf(GetValue("urlport", "8188"));
        } catch (NumberFormatException e) {
            uSendMessage("ERROR in urlport setting - not an integer. Default to 8188");
            servePort = 8188;
        }

        String host = GetValue("urlhost", NamedCommon.hostname);
        String path = GetValue("urlpath", "localhost");
        URLpath = path;

        Properties props = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(props);

        boolean URLnotset = false;
        if (URLpath == null) URLnotset = true;
        if (URLpath == "") URLnotset = true;
        if (URLnotset) {
            //
            // default URL is in rFuel.properties
            //
            String checkURL = props.getProperty("serveurl", "");
            if (!checkURL.equals("")) URLpath = checkURL;

            //
            // URL can be over-riden by script property
            //
            checkURL = GetValue("urlpath", "");
            if (!checkURL.equals("")) URLpath = checkURL;
        }

        URLhost = NamedCommon.hostname;
        URLport = servePort;
        bindAddess = protocol + "://" + URLhost + ":" + URLport;
        if (!URLpath.startsWith("/")) bindAddess += "/";
        bindAddess += URLpath;

        uSendMessage("-------------------------------------------------------------------------------------------------------------");
        uSendMessage(" ");

        uSendMessage("Accepting requests on " + bindAddess);

    }

    public static String BuildJsonReply(String status, String response, String laps) {
        String message = "";
        if (status == "200") {
            message = "OK";
        } else if (status == "401") {
            message = "Login required.";
        } else {
            message = "Bad request";
        }
        JSONObject jMsg = new JSONObject();
        jMsg.put("response", response);
        jMsg.put("message", message);
        jMsg.put("status", status);
        jMsg.put("query-time", laps);
        return jMsg.toString();
    }

    public static String GetValue(String inValue, String def) {
        String value = System.getProperty(inValue, def);
        if (value.equals(null) || value.equals("")) value = def;
        if (value.startsWith("ENC(")) {
            String tmp = value.substring(4, (value.length() - 1));
            value = uCipher.Decrypt(tmp);
        }
        return value;
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
        while (pword.endsWith("\n")) { pword = pword.substring(0,pword.length()-1); }
        if (NamedCommon.ZERROR) System.exit(1);
        if (pword.startsWith("ENC(")) {
            pword = pword.substring(4, pword.length());
            while (!pword.endsWith(")") && pword.length() > 0) {
                pword = pword.substring(0, pword.length() - 1);
            }
            if (pword.endsWith(")")) pword = pword.substring(0, pword.length() - 1);
            pword = uCipher.Decrypt(pword);
        }
        return pword.toCharArray();
    }

    public static SSLContext CreateContext(KeyStore kstore, KeyStore tstore) throws Exception {

        if (NamedCommon.debugging) uSendMessage("Creating SSLContext using KeyStore " + keystoreName);
        KeyManager[] keyManagers;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try {
            keyManagerFactory.init(kstore, password(keystoreName));
        } catch (UnrecoverableKeyException e) {
            uSendMessage(">>>");
            uSendMessage(">>> SECURITY ABORT: SSLContext for KeyStore: invalid password ");
            uSendMessage(">>>");
            System.exit(1);
        }
        keyManagers = keyManagerFactory.getKeyManagers();

        if (NamedCommon.debugging) uSendMessage("Creating SSLContext using TrustStore " + truststoreName);
        TrustManager[] trustManagers;
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(tstore);
        trustManagers = trustManagerFactory.getTrustManagers();

        sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, null);
        if (NamedCommon.debugging) uSendMessage("SSLContext created using TLS ");

        return sslContext;
    }

    public static KeyStore loadKeyStore(String name) throws Exception {
        if (NamedCommon.debugging) uSendMessage("Creating KeyStore object for [" + name + "]");
        String storeLoc = System.getProperty(name);
        if (keystoreNames.indexOf(name) < 0) {
            keystoreNames.add(name);
            keystoreLocns.add(storeLoc);
        }
        final InputStream stream;
        if (storeLoc == null) {
            stream = Http2Server.class.getResourceAsStream(name);
        } else {
            stream = Files.newInputStream(Paths.get(storeLoc));
        }

        if (stream == null) {
            throw new RuntimeException("Could not load keystore [" + name + "] - MUST be provided in shell script.");
        }
        try (InputStream is = stream) {
            KeyStore loadedKeystore = KeyStore.getInstance("JKS");
            loadedKeystore.load(is, password(name));
            return loadedKeystore;
        }
    }

    public static HttpServerExchange SetAndSend(HttpServerExchange exchange, String status, String reply) {
        if (reply != null) {
            exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), appjson);
            exchange.getResponseHeaders().put((Headers.STATUS), status);
            exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
            exchange.setStatusCode(Integer.valueOf(status));
            exchange.getResponseSender().send(reply);
        } else {
            uSendMessage("Got nothing to respond with");
        }
        uSendMessage("ReturnedResponse() ------------------------------------------------");
        System.out.println(" ");
        return exchange;
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
            if (!NamedCommon.isNRT) {
                uSendMessage("-------------------------------------------------------------------");
                uSendMessage("File Access FAIL :: " + infile);
                uSendMessage(e.getMessage());
                uSendMessage("-------------------------------------------------------------------");
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = e.getMessage();
            }
        }
        return rec;
    }

    public static String FieldOf(String str, String findme, int occ) {
        String ans, chkr;
        int iChk;
        String[] tmpStr;
        try {
            chkr = str;
        } catch (NullPointerException npe) {
            uSendMessage("ERROR: FieldOf() received a null string value");
            return "";
        }
        try {
            iChk = occ;
        } catch (NullPointerException npe) {
            uSendMessage("ERROR: FieldOf() received a null occurance value");
            return "";
        }
        tmpStr = str.split(findme);
        if (occ <= tmpStr.length) {
            ans = tmpStr[occ - 1];
        } else {
            ans = "";
        }
        return ans;
    }

    public static void SetMemory(String key, String val) {
        if (!key.equals("")) {
            int fnd = NamedCommon.ThisRunKey.indexOf(key);
            if (fnd < 0) {
                NamedCommon.ThisRunKey.add(key);
                NamedCommon.ThisRunVal.add(val);
            } else {
                NamedCommon.ThisRunVal.set(fnd, val);
            }
        }
    }

    public static String GetMemory(String key) {
        String val = "";
        if (!key.equals("")) {
            int fnd = NamedCommon.ThisRunKey.indexOf(key);
            if (fnd > -1) {
                val = NamedCommon.ThisRunVal.get(fnd);
            }
        }
        return val;
    }

    public static void SetupReturnCodes() {
//        uSendMessage("Loading: ./conf/http-return-codes.csv");
        for (int rc = 0; rc < 1100; rc++) { NamedCommon.ReturnCodes.add(""); }
        int nbrvals;
        String hCodes = ReadDiskRecord(NamedCommon.BaseCamp + "/conf/http-return-codes.csv");
        if (!hCodes.equals("")) {
            String[] tmp = hCodes.split("\\r?\\n");
            nbrvals = tmp.length;
            int idx;
            for (int rc = 0; rc < nbrvals; rc++) {
                String[] tmp1 = tmp[rc].split(",");
                try {
                    idx = Integer.valueOf(tmp1[0]);
                    NamedCommon.ReturnCodes.set(idx, tmp1[1]);
                } catch (NumberFormatException nfe) {
                    // skip the line
                }
            }
        }
    }

    public static String ResponseHandler(String status, String descr, String response, String esbFMT) {
        if (response == null) return "";
        if (response.equals("")) return response;
        String replyMessage = response;

        if (response.substring(0, 1).equals("{")) {
            replyMessage = response;
        } else {
            if (replyMessage.startsWith(NamedCommon.xmlProlog)) {
                replyMessage = replyMessage.substring(NamedCommon.xmlProlog.length(), replyMessage.length());
                response = replyMessage;
            }
            replyMessage = NamedCommon.xmlProlog + "<body><status>" + status +
                    "</status>" + "<message>" + descr + "</message><response>" +
                    response + "</response></body>";
        }
        replyMessage = uConnector.Format(replyMessage, esbFMT);
        return replyMessage;
    }

}
