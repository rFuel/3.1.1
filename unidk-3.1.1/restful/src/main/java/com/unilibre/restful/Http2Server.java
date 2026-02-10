package com.unilibre.restful;
/* ******** Copyright UniLibre on 2015. ALL RIGHTS RESERVED ********  */

/* This web service mixes HttpInOut with CDR - there is no jms in this */

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.core.DataConverter;
import com.unilibre.core.MessageProtocol;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import javax.net.ssl.SSLContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;


public class Http2Server {

    private static final char[] TEST_PASSWORD = "passw0rd".toCharArray();
    private static Properties siteConfig;
    private static boolean stopNow = false;
    private static boolean goDirect = false;
    private static boolean monitor = true;
    private static boolean secure  = false;
    private static boolean isDev   = false;
    private static boolean Externalping = false;
    private static boolean errSW = false;
    private static boolean multihost = true;
    private static String prefix = "";
    private static String status;
    private static String siteSecret = "";
    private static String SecretHold = "x-rfuel-api";
    private static String reply;
    private static String httpreq;
    private static String httpMethod = "";
    private static String httpQuery = "";
    private static String inHost;
    private static String inPort;
    private static String inPath;
    private static String thisHDR;
    private static String debugStr;
    private static String usage = "";
    private static String pingReq = "";
    private static String protocol = "https";
    private static String cdrMessage = "{\"request\":{\"version\":\"v2\",\"bsb\":\"000000\",\"customer\":\"$\",\"action\":\"#\",\"correlationid\":\"!\",\"purge-cache\":\"true\",\"page-size\":\"100\",\"page\":\"1\"}}";
    private static String sepChar = "~";
    private static int IOthreads = 1, workerThreads = 10;
    private static long startM, finishM, lastActive, rightNow;
    private static double laps, div = 1000000000.00;

    public static void main(final String[] args) {

        //  urlhost=localhost                                                       //
        //  urlport=8188                                                            //
        //  urlpath=cdr/banking/                                                    //
        //  secure=true                                                             //
        //  server.keystore=/upl/ssl/rfuel_unilibre_com_au.jks                      //
        //  server.truststore=/upl/ssl/rfuel_unilibre_com_au.jks                    //
        //                                                                          //
        //  put the keystore and truststore password in;                            //
        //  /upl/ssl/rfuel_unilibre_com_au.jks.password                             //
        //  it will automatically get picked up in the password method              //

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.upl = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            NamedCommon.gmods = NamedCommon.BaseCamp + "lib/";
            License.domain = "rfuel22";
            NamedCommon.hostname = License.domain;
            prefix = "local.";
            isDev = true;
        }

        if (!isLicenced()) { System.out.println("licence failure"); System.exit(1); }

        secure = System.getProperty("secure", "").toLowerCase().equalsIgnoreCase("true");
        NamedCommon.isRest = true;
        if (secure) {
            SecureProcess();            // set up HTTPS listener                    //
        } else {
            StandardProcess();          // set up HTTP  listener                    //
        }
        WaitForRequests();          // loop infinitely to watch for idle time       //
    }

    private static boolean isLicenced() {
        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);

        if (NamedCommon.upl.equals("")) {
            NamedCommon.upl = System.getProperty("user.dir");
            NamedCommon.BaseCamp = NamedCommon.upl;
        }
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        NamedCommon.isDocker = true;

        Properties rfProps;
        rfProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) return false;
        uCommons.SetCommons(rfProps);
        rfProps = null;

        return License.IsValid();
    }

    private static void WaitForRequests() {

        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("[ Serving Requests ] --------------------------------------------");
        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("Accepting requests on " + cdrCommons.getAddress());
        uCommons.uSendMessage(" ");
        uCommons.uSendMessage(" ");

        if (!monitor) return;

        lastActive = System.nanoTime();
        int idleSeconds;
        String pingResponse;
        DocumentBuilderFactory domFactory =DocumentBuilderFactory.newInstance(); 
        DocumentBuilder domBuilder;
        Document xmlDoc;
//        OutputFormat format;
        Writer out;
//        XMLSerializer serializer;

        // ----------------------------------------------------------
        // When ping = "show", MessageProtocol shows a lot of details
        // When ping =  "now", MessageProtocol does a CheckSource()
        // ----------------------------------------------------------

        while (!stopNow) {
            rightNow = System.nanoTime();
            laps = (rightNow - lastActive) / div;
            idleSeconds = ((int) laps);
            if (idleSeconds > NamedCommon.mqHeartBeat) {
                if (!pingReq.equals("")) {
                    pingResponse = ProcessRequest(pingReq);
                    if (NamedCommon.ConnectionError) {
                        uCommons.uSendMessage("Experiencing RPC issues while waiting for requests.");
                        uCommons.uSendMessage("Trying to re-establish a connection with your Source DB");
                        SourceDB.ReConnect();
                        if (NamedCommon.ZERROR) System.exit(0);
                    } else {
                        if (NamedCommon.debugging) {
                            if (pingResponse.startsWith("<")) {
                                try {
                                    out = new StringWriter();
                                    domBuilder = domFactory.newDocumentBuilder();
                                    xmlDoc = domBuilder.parse(new InputSource(new StringReader(pingResponse)));
//                                    format = new OutputFormat();
//                                    format.setLineWidth(65);
//                                    format.setIndenting(true);
//                                    format.setOmitXMLDeclaration(false);
//                                    format.setIndent(3);
//                                    serializer = new XMLSerializer(out, format);
//                                    serializer.serialize(xmlDoc);
                                    uCommons.uSendMessage("**");
                                    System.out.println(out.toString());
                                } catch (IOException e) {
                                    uCommons.uSendMessage(e.getMessage());
                                } catch (SAXException e) {
                                    uCommons.uSendMessage(e.getMessage());
                                } catch (ParserConfigurationException e) {
                                    e.printStackTrace();
                                }
                                out = null;
                                domBuilder = null;
                                xmlDoc = null;
//                                format = null;
//                                serializer = null;
                            } else {
                                JSONObject jObj = new JSONObject(pingResponse);
                                System.out.println(jObj.toString(2));
                                jObj = null;
                            }
                        }
                    }
                }
                SourceDB.ReConnect();
                Properties rfProps;
                rfProps = uCommons.LoadProperties("rFuel.properties");
                if (NamedCommon.ZERROR) System.exit(0);
                rfProps = null;
                if (!Externalping) lastActive = System.nanoTime();
                System.out.println();
                uCommons.uSendMessage("*** waiting for requests on " + cdrCommons.getAddress());
                System.out.println();
                Externalping = false;
            } else {
//                if (String.valueOf(idleSeconds).endsWith("0")) System.out.println(NamedCommon.mqHeartBeat - idleSeconds);
                uCommons.Sleep(1);
            }
        }
    }

    private static void StandardProcess() {
        protocol = "http";
        cdrCommons.setSecure(false);
        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("[ Initialising ] ------------------------------------------------");

        Initialise();

        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("Initialisation ERROR: cannot proceed");
            uCommons.uSendMessage(NamedCommon.Zmessage);
            System.exit(1);
        }
        String chkPath = cdrCommons.getPath();

        uCommons.uSendMessage("     BaseCamp: " + NamedCommon.BaseCamp);
        uCommons.uSendMessage("    IOthreads: " + IOthreads);
        uCommons.uSendMessage("workerThreads: " + workerThreads);
        uCommons.uSendMessage("  Message TTL: " + (NamedCommon.Expiry / 1000) + " seconds.");
        uCommons.uSendMessage("         Host: " + cdrCommons.getHost());
        uCommons.uSendMessage("         Port: " + cdrCommons.getPort());
        uCommons.uSendMessage("         Path: " + cdrCommons.getPath());
        uCommons.uSendMessage("-----------------------------------------------------------------");

        Undertow server = Undertow.builder()
                .setIoThreads(IOthreads)
                .setWorkerThreads(workerThreads)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpListener(cdrCommons.getPort(), cdrCommons.getHost())
                .setHandler(exchange -> {

                    String iam = Thread.currentThread().getName().replaceAll("\\ ", "-");
                    cdrCommons.SetIAM(iam);
                    uCommons.uSendMessage("{" + iam + "} ReceivedRequest() *************************************************");

                    RequestResetter(exchange);

                    if (inPath.equals(chkPath) && httpMethod.equals("POST")) {

                        if (!exchange.getResponseHeaders().contains(thisHDR))
                            exchange.getResponseHeaders().add(Headers.LOCATION, thisHDR);
                        // -----------------------------------------------------------------------------

                        httpreq = "";
                        errSW = false;
                        while (httpreq.equals("")) {
                            exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                                        uCommons.uSendMessage("Request-Handler()");
                                        httpreq = new String(data);
                                    },

                                    (body, exception) -> {
                                        uCommons.uSendMessage("Error-Handler()");
                                        httpreq = exception.toString();
                                        errSW   = true;
                                    });
                        }
                        if (!errSW) {
                            reply = ProcessRequest(httpreq);
                        } else {
                            uCommons.uSendMessage("*");
                            uCommons.uSendMessage("-------------------------------------------------------------------------------");
                            uCommons.uSendMessage("An HTTPS exception has occurred: [" + httpreq + "]");
                            uCommons.uSendMessage("-------------------------------------------------------------------------------");
                            uCommons.uSendMessage("*");
                            reply = ErrorHandler(httpreq);
                        }

                    } else {
                        if (status.equals("200")) {
                            if (httpMethod.equals("GET")) {
                                reply = ProcessGetRequest(httpQuery);
                            } else {
                                uCommons.uSendMessage("-------------------------------------------------------------");
                                uCommons.uSendMessage("Exchange issue:");
                                uCommons.uSendMessage("inPath: " + inPath + "   chkPath : " + chkPath + "   Method " + httpMethod);
                                uCommons.uSendMessage("reply : " + reply);
                                uCommons.uSendMessage("-------------------------------------------------------------");
                            }
                        }
                    }

                    if (reply.contains("WaitForReply():")) stopNow = true;

                    ReplyManager();

                    exchange = methods.SetAndSend(exchange, status, reply);

                    if (!Externalping) lastActive = System.nanoTime();
                    NamedCommon.ZERROR = false;
                    NamedCommon.Zmessage = "";

                    if (stopNow) System.exit(0);
                    if (NamedCommon.ConnectionError) {
                        uCommons.uSendMessage("Experiencing RPC issues!");
                        uCommons.uSendMessage("Trying to re-establish a connection with your Source DB");
                        SourceDB.ReConnect();
                        if (NamedCommon.ZERROR) System.exit(0);
                    }
                    uCommons.Sleep(0);
                    exchange = null;
                })
                .build();
        server.start();
    }

    private static void SecureProcess() {

        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("[ Initialising ] ------------------------------------------------");

        Initialise();

        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("Initialisation ERROR: cannot proceed");
            uCommons.uSendMessage(NamedCommon.Zmessage);
            System.exit(1);
        }

        uCommons.uSendMessage("[ Set Security Features ON ] ------------------------------------");
        KeyStore ks = null;
        SSLContext sslContext = null;
        try {
            ks = methods.loadKeyStore("server.keystore");
            KeyStore ts = methods.loadKeyStore("server.truststore");
            sslContext = methods.CreateContext(ks, ts);
        } catch (Exception e) {
            uCommons.uSendMessage(e.getMessage());
            return;
        }

        String chkPath = "/" + cdrCommons.getPath();

        uCommons.uSendMessage("     BaseCamp: " + NamedCommon.BaseCamp);
        uCommons.uSendMessage("    IOthreads: " + IOthreads);
        uCommons.uSendMessage("workerThreads: " + workerThreads);
        uCommons.uSendMessage("  Message TTL: " + (NamedCommon.Expiry / 1000) + " seconds.");
        uCommons.uSendMessage("-----------------------------------------------------------------");

        Undertow server = Undertow.builder()
                .setIoThreads(IOthreads)
                .setWorkerThreads(workerThreads)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpsListener(cdrCommons.getPort(), cdrCommons.getHost(), sslContext)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {

                        String iam = Thread.currentThread().getName().replaceAll("\\ ", "-");
                        cdrCommons.SetIAM(iam);
                        uCommons.uSendMessage("{" + iam + "} ReceivedRequest() *************************************************");

                        RequestResetter(exchange);

                        if (inPath.equals(chkPath) && httpMethod.equals("POST")) {

                            if (!exchange.getResponseHeaders().contains(thisHDR))
                                exchange.getResponseHeaders().add(Headers.LOCATION, thisHDR);
                            // -----------------------------------------------------------------------------

                            exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                                        uCommons.uSendMessage("Request-Handler()");
                                        String req = new String(data);
                                        httpreq = req;
                                        debugStr = req;
                                        reply = ProcessRequest(req);
                                    },

                                    (body, exception) -> {
                                        uCommons.uSendMessage("Error-Handler()");
                                        debugStr = "Exception: " + exception.toString();
                                        httpreq = debugStr;
                                        uCommons.uSendMessage("*");
                                        uCommons.uSendMessage("-------------------------------------------------------------------------------");
                                        uCommons.uSendMessage("An HTTPS exception has occurred: [" + exception.toString() + "]");
                                        uCommons.uSendMessage("-------------------------------------------------------------------------------");
                                        uCommons.uSendMessage("*");
                                        reply = ErrorHandler(exception.toString());
                                    });

                        } else {
                            if (status.equals("200")) {
                                if (httpMethod.equals("GET")) {
                                    reply = ProcessGetRequest(httpQuery);
                                } else {
                                    uCommons.uSendMessage("-------------------------------------------------------------");
                                    uCommons.uSendMessage("Exchange issue:");
                                    uCommons.uSendMessage("inPath: " + inPath + "   chkPath : " + chkPath + "   Method " + httpMethod);
                                    uCommons.uSendMessage("reply : " + reply);
                                    uCommons.uSendMessage("-------------------------------------------------------------");
                                }
                            }
                        }

                        if (reply.contains("WaitForReply():")) stopNow = true;

                        ReplyManager();

                        exchange = methods.SetAndSend(exchange, status, reply);

                        if (!Externalping) lastActive = System.nanoTime();
                        NamedCommon.ZERROR = false;
                        NamedCommon.Zmessage = "";

                        if (stopNow) System.exit(0);
                        uCommons.Sleep(0);
                        exchange = null;
                    }

                })
                .build();
        server.start();
    }

    private static void Initialise() {

        String conf = System.getProperty("conf", "");
        if (conf.equals("")) conf = "http.properties";

        if (siteConfig == null) {
            uCommons.uSendMessage("Config: " + conf);
            String fname = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash + conf;
            siteConfig = uCommons.LoadProperties(fname);
            if (NamedCommon.ZERROR) System.exit(1);
        }

        cdrMessage  = GetValue("request","");
        sepChar     = GetValue("sepchar", sepChar);
        siteSecret  = GetValue("secret", "");
        NamedCommon.Broker = GetValue("broker", "");

        int servePort = 8188;
        try {
            NamedCommon.Expiry  = Long.valueOf(GetValue("responseTTL", String.valueOf(10000)));
            IOthreads           = Integer.valueOf(GetValue("iothreads", String.valueOf(IOthreads)));
            workerThreads       = Integer.valueOf(GetValue("workerthreads", String.valueOf(workerThreads)));
            servePort           = Integer.valueOf(GetValue("urlport", "8188"));
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("Error in " + conf + ": " + nfe.getMessage());
            System.exit(0);
        }

        pingReq     = GetValue("ping", "");
        monitor     = GetValue("monitor", "false").toLowerCase().equals("true");
        goDirect    = GetValue("direct", "").toLowerCase().equals("true");
        usage       = GetValue("usage", "").toLowerCase();
        lastActive = System.nanoTime();

        try {
            cdrCommons.Initialise();
        } catch (IOException e) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = e.getMessage();
            uCommons.uSendMessage(NamedCommon.Zmessage);
            return;
        }

        cdrCommons.setMode(goDirect);
        cdrCommons.setSecure(secure);
        String host = GetValue("urlhost", "localhost");
        String path = GetValue("urlpath", "");
        if (isDev) host = "localhost";
        cdrCommons.setURLhost(host);
        cdrCommons.setURLpath(path);
        cdrCommons.setURLport(servePort);
        if (NamedCommon.Expiry < 100) NamedCommon.Expiry = 10000;

        if (NamedCommon.ReturnCodes == null) SetupReturnCodes();

        if (!siteSecret.equals("")) {
            uCommons.uSendMessage("***********************************");
            uCommons.uSendMessage("Must know: " + siteSecret);
            uCommons.uSendMessage("***********************************");
        }

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.debug(String.valueOf(Level.OFF));
        root.info(String.valueOf(Level.OFF));
        root.warn(String.valueOf(Level.OFF));
    }

    private static void SetupReturnCodes() {
        NamedCommon.ReturnCodes = new ArrayList<>();
        for (int rc = 0; rc < 1100; rc++) { NamedCommon.ReturnCodes.add(""); }
        int nbrvals;
        String hCodes = uCommons.ReadDiskRecord(NamedCommon.BaseCamp + "/conf/http-return-codes.csv");
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

    private static void ReplyManager() {
        if (reply == null) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Internal Server Error.";
        }

        if (reply.equals("")) {
            uCommons.uSendMessage("rFuel stop may be ON - check now!");
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Cannot serve your request. httpReq: [" + httpreq + "]";
        }

        if (reply.startsWith("<<")) {
            reply = reply.replaceAll("\\<\\<", "");
            reply = reply.replaceAll("\\>\\>", "");
        }

        if (reply.startsWith("FAIL")) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = reply;
            reply = "";
        }

        if (NamedCommon.ZERROR) {
            NamedCommon.ZERROR = NamedCommon.ZERROR;
            NamedCommon.Zmessage = NamedCommon.Zmessage;
        }

        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("ERROR: " + NamedCommon.Zmessage);
            status = "400";
            reply = DataConverter.ResponseHandler(status, "Bad Request", NamedCommon.Zmessage, "JSON");
        }

        if (!reply.startsWith("{") && !reply.startsWith("<")) {
            reply  = "{\"message\": \"" + reply + "\"}";
        }
    }

    private static void RequestResetter(HttpServerExchange exchange) {
        status = "200";
        reply = "";
        httpreq = "";
        inHost = exchange.getHostName();
        inPort = String.valueOf(exchange.getHostPort());
        inPath = exchange.getRelativePath();
        httpMethod = exchange.getRequestMethod().toString();
        httpQuery = exchange.getQueryString();
        thisHDR = protocol + "://" + inHost + ":" + inPort + inPath;

        // Authenticate the request --------------------

        if (!siteSecret.equals("") && !inPath.toLowerCase().endsWith("/ping")) {
            String secret = "", hTemp;
            boolean isThere = false;
            // Get the site secret out of the header x-rfuel-api
            // unsure of how undertow handles case sensitivity so doing it long-handed.
            HeaderMap headerMap = exchange.getRequestHeaders();
            String headers = headerMap.getHeaderNames().toString();
            headers = headers.replace("[", "");
            headers = headers.replace("]", "");
            String[] arrHeader = headers.split(",");
            int hLoop = arrHeader.length;
            for (int h=0; h < hLoop ; h++) {
                hTemp = arrHeader[h];
                while(hTemp.startsWith(" ")) {hTemp = hTemp.substring(1,hTemp.length());}
                if (hTemp.toLowerCase().equals(SecretHold)) {
                    isThere = true;
                    secret = headerMap.get(hTemp).toString();
                    secret = secret.replace("[", "");
                    secret = secret.replace("]", "");
                    uCommons.uSendMessage("SiteSecret() provided : [" + secret + "]");
                    break;
                }
            }
            boolean isErr = true;
            if (isThere) {
                isErr = false;
                if (secret == null) secret = "";
                if (!secret.equals(siteSecret)) isErr = true;
                if (isErr) uCommons.uSendMessage("SiteSecret() is in ERROR : [" + secret + "]");
            }
            headerMap = null;
            secret = "";
            if (isErr) {
                // SiteSecret is WRONG ... but do not tell them !!
                uCommons.uSendMessage("rFuel error 090346-");
                status = "403";
                reply = "SiteSecret() ERROR !"; //DataConverter.ResponseHandler(status, "Forbidden", secret, "JSON");
            }
        }

        debugStr = "empty-request";
    }

    private static String ProcessGetRequest(String query) {
        String request = cdrMessage, answer = "", tmp;
        if (query.equals("")) {
            Externalping = true;
            uCommons.uSendMessage(" Health_checker(ping)");
            return "success";
        }
        String[] qTmp = query.split("=");
        // Customer ------------------------------------------------------------
        if (qTmp[0].toLowerCase().equals("loginid")) {
            while (qTmp[1].contains(sepChar)) { qTmp[1] = qTmp[1].replace(sepChar, ":"); }
            request = request.replace("$", qTmp[1]);
        } else {
            while (qTmp[1].contains(sepChar)) { qTmp[0] = qTmp[0].replace(sepChar, ":"); }
            request = request.replace("$", qTmp[0]);
        }
        // Action --------------------------------------------------------------
        String[] tmparr = ("/a/b/c/" + inPath).split("/");
        tmp = tmparr[tmparr.length-1];
        request = request.replace("#", tmp);
        // Correlation ID ------------------------------------------------------
        tmp = UUID.randomUUID().toString();
        while (tmp.contains(" ")) { tmp = tmp.replace(" ", "_"); }
        request = request.replace("!", tmp);
        answer = ProcessRequest(request);
        return answer;
    }

    private static String ProcessRequest(String request) {
        uCommons.uSendMessage("ProcessRequest()");
        APImsg.instantiate();
        uCommons.SetCommons(uCommons.LoadProperties("rFuel.properties"));
        if (NamedCommon.ZERROR) return NamedCommon.Zmessage;
        cdrCommons.setMode(goDirect);
        String answer = "";
        startM = System.nanoTime();
        //
        switch (usage) {
            case "cdr":
                answer = cdrCommons.HandleResponse(request);
                break;
            default:
                answer = HttpInOut("", request);
                break;
        }
        finishM = System.nanoTime();
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";
        return answer;
    }

    private static String HttpInOut(String getQ, String request) {
        String answer = "";
        NamedCommon.Reset();
        APImsg.instantiate();

        uCommons.MessageToAPI(request);
        if (NamedCommon.ZERROR) {
            status = "500";
            return NamedCommon.Zmessage;
        }

        NamedCommon.Zmessage = "";
        NamedCommon.MessageID = APImsg.APIget("correlationid") + "_" + NamedCommon.pid;
        answer = MessageProtocol.handleProtocolMessage(APImsg.APIget("task"), "1", request);
        if (APImsg.APIget("task").equals("055")) {
            if (answer.equals("")) {
                uCommons.Sleep(0);
                answer = u2Commons.ReadAnItem("uRESPONSES", NamedCommon.zID, "1", "", "");
            }
            answer = DataConverter.ResponseHandler(String.valueOf(status), "DONOTALTER", answer, APImsg.APIget("FORMAT").toUpperCase());
        }
        return answer;
    }

    public static String GetValue(String inValue, String def) {
        String value = System.getProperty(inValue, "");
        if (value.equals(null) || value.equals("")) value = siteConfig.getProperty(inValue, "");
        if (value.equals(null) || value.equals("")) value = def;
        if (value.startsWith("ENC(")) {
            String tmp = value.substring(4, (value.length() - 1));
            value = uCipher.Decrypt(tmp);
        }
        return value;
    }

    private static String ErrorHandler(String error) {
        uCommons.uSendMessage("ERROR: " + error);
        String answer = "";
        status = "400";
        answer = DataConverter.ResponseHandler(status, "Bad Request", error, "JSON");
        return answer;
    }

}