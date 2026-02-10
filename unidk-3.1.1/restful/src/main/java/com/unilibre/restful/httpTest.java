package com.unilibre.restful;


import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.json.JSONException;
import org.json.JSONObject;
import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.util.ArrayList;
import java.util.Iterator;

public class httpTest {

    public ArrayList<String> keystoreNames = new ArrayList<>();
    public ArrayList<String> keystoreLocns = new ArrayList<>();
    private String keystoreName = "server.keystore";
    private String truststoreName = "server.truststore";
    private String jHeader = "request";

    public static void main(String[] args) throws Exception {
        httpTest test = new httpTest();
        test.uRestHandler();
    }

    private void uRestHandler() {
        KeyStore ks;
        KeyStore ts;
        SSLContext sslContext=null;
        {
            try {
                ks = loadKeyStore("server.keystore");
                ts = loadKeyStore("server.truststore");
                sslContext = CreateContext(ks, ts);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }
        }

        Undertow server = Undertow.builder()
                .setIoThreads(15)
                .setWorkerThreads(10)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpsListener(8188, "localhost", sslContext)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {
                        String iam = Thread.currentThread().getName().replaceAll("\\ ", "-");
                        System.out.println("{" + iam + "} ReceivedRequest() *************************************************");
                        exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                                    String req = new String(data);
                                    String reply = ProcessRequest(req);
                                    exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), "application/json");
                                    exchange.getResponseHeaders().put((Headers.STATUS), 200);
                                    exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
                                    exchange.setStatusCode(Integer.valueOf("200"));
                                    exchange.getResponseSender().send(reply);
                                    req = null;
                                    reply = null;
                                },

                                (body, exception) -> {
                                    String reply = ErrorHandler(exception.toString());
                                    exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), "application/json");
                                    exchange.getResponseHeaders().put((Headers.STATUS), 501);
                                    exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
                                    exchange.setStatusCode(Integer.valueOf("501"));
                                    exchange.getResponseSender().send(reply);
                                    reply = null;
                                });
                    }
                })
                .build();
        server.start();
    }

    private String ProcessRequest(String request) {
        String answer = "";
        JSONObject obj = new JSONObject(request);
        ArrayList<String> reqKeys = new ArrayList<>();
        ArrayList<String> reqVals = new ArrayList<>();
        Iterator<String> jKeys = null;
        jKeys = obj.getJSONObject(jHeader).keys();
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = obj.getJSONObject(jHeader).get(zkey).toString();
            reqKeys.add(zkey.toLowerCase());
            reqVals.add(zval);
        }
        String task = GetRequestValue(reqKeys, reqVals, "task");
        switch (task) {
            case "050":
                answer = Reader(reqKeys, reqVals);
                break;
            case "055":
                answer = Writer(reqKeys, reqVals);
                break;
            default:
                answer = "ERROR: Wrong task";
        }
        return answer;
    }

    private String Reader(ArrayList<String> reqKeys, ArrayList<String> reqVals) {
        return "okay";
    }

    private String Writer(ArrayList<String> reqKeys, ArrayList<String> reqVals) {
        return "okay";
    }

    private String ErrorHandler(String error) {
        return "failed";
    }

    public String GetRequestValue(ArrayList<String> reqKeys, ArrayList<String> reqVals, String key) throws JSONException {
        String ans = "";
        int fnd = reqKeys.indexOf(key.toLowerCase());
        if (fnd >= 0) {
            ans = reqVals.get(fnd);
            reqKeys.remove(fnd);
            reqVals.remove(fnd);
        }
        return ans;
    }

    public KeyStore loadKeyStore(String name) throws Exception {
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

    public char[] password(String name) {
        String pw = name + ".password";
        if (keystoreNames.indexOf(name) < 0) {
            System.out.println("     location for [" + name + ".password] has not been provided.");
            System.exit(1);
        } else {
            pw = keystoreLocns.get(keystoreNames.indexOf(name)) + ".password";
        }
        String pword = ReadDiskRecord(pw);
        while (pword.endsWith("\n")) {
            pword = pword.substring(0, pword.length() - 1);
        }
        // encryupt password here
        return pword.toCharArray();
    }

    public String ReadDiskRecord(String infile) {
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
                System.out.println("read FAIL on " + infile);
                System.out.println(e.getMessage());
                return rec;
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    System.out.println("read FAIL on " + infile);
                    System.out.println(e.getMessage());
                    return rec;
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                System.out.println("File Close FAIL on " + infile);
                System.out.println(e.getMessage());
                return rec;
            }
        } catch (IOException e) {
            System.out.println("-------------------------------------------------------------------");
            System.out.println("File Access FAIL :: " + infile);
            System.out.println(e.getMessage());
            System.out.println("-------------------------------------------------------------------");
        }
        return rec;
    }

    public SSLContext CreateContext(KeyStore kstore, KeyStore tstore) throws Exception {
        KeyManager[] keyManagers;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try {
            keyManagerFactory.init(kstore, password(keystoreName));
        } catch (UnrecoverableKeyException e) {
            System.out.println(">>>");
            System.out.println(">>> SECURITY ABORT: SSLContext for KeyStore: invalid password ");
            System.out.println(">>>");
            System.exit(1);
        }
        keyManagers = keyManagerFactory.getKeyManagers();

        TrustManager[] trustManagers;
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(tstore);
        trustManagers = trustManagerFactory.getTrustManagers();

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(keyManagers, trustManagers, null);

        return ctx;
    }

}
