package com.unilibre.dataserver;


/* ******** Copyright UniLibre on 2015. ALL RIGHTS RESERVED ********  */

/* This web service mixes HttpInOut with JmsInOut */

/* For JMS, make sure there is a broker with a virtual topic */
/* The broker is set in the shell script -Dbroker=           */

//  testing with isWebs=true for HttpInOut

//import com.sun.org.apache.xml.internal.serialize.OutputFormat;
//import com.sun.org.apache.xml.internal.serialize.XMLSerializer;
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
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.jms.JMSException;
import javax.net.ssl.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.text.SimpleDateFormat;
import java.util.*;

public class http2jms {

    private static ArrayList<String> keystoreNames = new ArrayList<>();
    private static ArrayList<String> keystoreLocns = new ArrayList<>();
    private static ArrayList<String> whitelist = new ArrayList<>();
    private static ArrayList<String> blacklist = new ArrayList<>();
    private static ArrayList<String> ReturnCodes;
    private static String BaseCamp = System.getProperty("user.dir");
    private static final String keystoreName = "server.keystore";
    private static final String truststoreName = "server.truststore";
    private static final String jHeader = "request", is="<is>", tm="<tm>";
    private static final String tester = "testQueue";
    private static String getTemplate = "{\"request\":{\"version\":\"v2\",\"bsb\":\"000000\",\"customer\":\"$\",\"action\":\"#\",\"correlationid\":\"!\",\"purge-cache\":\"true\",\"page-size\":\"100\",\"page\":\"1\"}}";
    private static String getQ = "059_CDR_Responses", dbfHost = "rfuel14";
    private static String requestorIP="", siteSecret = "", bindAddess = "";
    private static String httpHost, httpPath, httpMethod, httpQuery, inPath;
    private static String pingReq = "", cfile;
    private static int httpPort;
    private static int iothreads = 1;
    private static int msgCounter = 1;
    private static int workers = 10;
    private static int heartbeat = 300;
    private static boolean https = true;
    private static boolean jms2jms = true;
    private static boolean isDev = false;
    private static boolean getPing = false;
    private static Properties siteProps;
    private static Properties runProps;
    private static Properties bkrProps;
    private final long Expiry = 30000;
    private ArrayList<String> reqKeys;
    private ArrayList<String> reqVals;
    private ArrayList<String> sysUsers;
    private ArrayList<String> sysSecrets;
    private String reply, iam, mqHost, mqQue, mqPath, mqPort, mqURL, correlationID, inRequest, dbgMessage;
    private int status = 0, procID=0, intJunk=0;
    private JMSConsumer jmsc = null;
    private JMSProducer jmsp = null;
    private boolean undertowOkay = false;
    private String strJunk = "";
    private static boolean monitor = false;
    private static boolean stopNow = false;
    private static long  lastActive, rightNow;
    private static double laps, div = 1000000000.00;

    // setter methods for Mule Connector *************

    public void initVars() {
        mqQue = "";
        getQ  = "";
        jms2jms = false;
    }

    private http2jms() throws Exception {
        WebServer();
        WaitForRequests();          // loop infinitely to watch for idle time       //
    }

    private void WebServer() throws Exception {

        String svrKeyStore   = System.getProperty("server.keystore", "");
        String svcTrustStore = System.getProperty("server.truststore", "");
        String cmdPort       = System.getProperty("port", "");

        cfile = System.getProperty("conf", "webDS.properties");
        runProps = LoadProperties(cfile);
        System.out.println("configuring according to " + cfile);

        if (!svrKeyStore.equals(""))   runProps.setProperty("server.keystore",svrKeyStore);
        if (!svcTrustStore.equals("")) runProps.setProperty("server.truststore",svcTrustStore);

        monitor     = runProps.getProperty("monitor", "false").toLowerCase().equals("true");
        pingReq     = runProps.getProperty("ping", "");
        https       = runProps.getProperty("secure", "false").toLowerCase().equals("true");
        httpHost    = runProps.getProperty("httphost", "");
        if (isDev) httpHost = "192.168.48.1";
        httpPath    = runProps.getProperty("httppath", "/api/webDS/");
        getTemplate = runProps.getProperty("mtemplate", getTemplate);

        try {
            String hb   = runProps.getProperty("heartbeat", "30");
            heartbeat = Integer.valueOf(hb);
        } catch (NumberFormatException nfe) {
            System.out.println("Heartbeat not provided - defauilting to 300");
            heartbeat = 300;
        }

        try {
            String tmp = runProps.getProperty("httpport", "");
            if (!cmdPort.equals("")) tmp = cmdPort;
            httpPort = Integer.valueOf(tmp);
        } catch (NumberFormatException nfe) {
            System.out.println("Port number not provided - defauilting to 8188");
            httpPort = 8188;
        }

        bkrProps = null;
        String bkr = System.getProperty("broker", "");
        if (bkr.equals("")) bkr = runProps.getProperty("broker", "");
        if (!bkr.equals("")) bkrProps = LoadProperties(bkr);

        // ------------------------- Please Read --------------------------------------------
        //  jms2jms: false: use the AMQ RESTful interface - needs a lot of work and testing *
        //            true: send message to the Virtual topic the usual in-out pattern      *
        //                  the message in-out pattern sends from jms topic to jms queue    *
        //                  hence : jms2jms                                                 *
        // ----------------------------------------------------------------------------------

        if (bkrProps == null || bkrProps.size() == 0) {
            jms2jms = false;
            mqQue  = ""; // ""Consumer.4.VirtualTopic.rFuel";
            mqHost = ""; // runProps.getProperty("mqhost", "");
            mqPort = ""; // runProps.getProperty("mqPort", "8161");
            mqPath = ""; // runProps.getProperty("mqpath", "/api/message");
        } else {
            String vTopic = bkrProps.getProperty("topic");
            if (vTopic == null) {
                System.out.println("");
                System.out.println("This process is set to use jms but [" + bkr+"] does not have a virtual topic");
                System.out.println("");
                System.out.println("");
                System.exit(1);
            }
            if (!vTopic.equals("")) {
                mqQue = "VirtualTopic." + bkrProps.getProperty("topic");
                jms2jms = true;
            } else {
                mqQue = System.getProperty("queue", "");
                if (mqQue.equals("")) {
                    System.out.println("No MQ queue has been provided in config file " +
                            System.getProperty("conf", "webDS.properties"));
                    return;
                }
            }
        }

        iam    = "main";
        reply  = "";
        procID = 0;

        if (jms2jms) {
            String bkrUser = bkrProps.getProperty("bkruser", "");
            String bkrPass = bkrProps.getProperty("bkrpword", "");
            mqURL     = bkrProps.getProperty("url", "");
            if (mqURL.equals("")) {
                System.out.println("No MQ URL has been provided in config file " +
                        System.getProperty("conf", "webDS.properties"));
                return;
            }
            System.out.println("---------------- Create JMS Consumner ----------------");
            jmsc = new JMSConsumer();
            jmsc.clID = NamedCommon.pid + ":http2jms:cons";
            jmsc.Prepare(mqURL, jmsc.clID);
            System.out.println("---------------- Create JMS Producer  ----------------");
            jmsp = new JMSProducer();
            jmsp.clID = NamedCommon.pid + ":http2jms:prod";
            NamedCommon.bkr_user = bkrUser;
            NamedCommon.bkr_pword= bkrPass;
            NamedCommon.Expiry   = Expiry;
            jmsp.PrepareConnector(mqURL, mqQue);
            System.out.println(" ");
            System.out.println("------------------------------------------------------");
            System.out.println("Ensure that URestResponder is running ----------------");
            System.out.println("------------------------------------------------------");
            System.out.println(" ");
        } else {
            System.out.println(" ");
            System.out.println("------------------------------------------------------");
            System.out.println("Ensure that uRestResponder is NOT running ------------");
            System.out.println("------------------------------------------------------");
            System.out.println(" ");
            mqHost = dbfHost;
            mqPath = "/api/message";
        }

        System.out.println("---------------- Create Undertow Server----------------");

        if (https) {
            bindAddess = "https" + "://" + httpHost + ":" + httpPort + httpPath;
            System.out.println("Requesting SECURE service at " + bindAddess);
            System.out.println("-------------------------------------------------------");
            SecureServer();
        } else {
            bindAddess = "http" + "://" + httpHost + ":" + httpPort + httpPath;
            System.out.println("Requesting service at " + bindAddess);
            System.out.println("-----------------------------------------------------------------");
            HttpServer(httpPort, httpHost, bindAddess);
        }

        LoadValidUsers();

        System.out.println(" ");
        System.out.println("[ Serving Requests ] --------------------------------------------");
        System.out.println(" ");
        System.out.println("Accepting requests on " + bindAddess);
        System.out.println(" ");
        System.out.println(" ");
        System.out.println("-----------------------------------------------------------------");
    }

    private void LoadValidUsers() {
        sysUsers       = null;
        String users   = ReadDiskRecord(NamedCommon.BaseCamp + "/conf/users");
        sysUsers       = new ArrayList<String>(Arrays.asList(users.split("\\r?\\n")));
//        sysSecrets     = null;
//        String secrets = ReadDiskRecord(NamedCommon.BaseCamp + "/conf/secrets");
//        sysSecrets     = new ArrayList<String>(Arrays.asList(secrets.split("\\r?\\n")));
    }

    private void SecureServer() throws Exception {

        // -------------- Reset to User Parameters !! ------------------------
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        siteProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(siteProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        siteProps.clear();
        // -------------------------------------------------------------------
        siteSecret  = runProps.getProperty("secret", "");
        String broker = runProps.getProperty("broker", "");
        if (!broker.equals("") && !(broker == null)) {
            Properties bProps = LoadProperties(BaseCamp + "/conf/" + broker);
        }

        try {
            iothreads = Integer.valueOf(runProps.getProperty("iothreads", String.valueOf(iothreads)));
            workers   = Integer.valueOf(runProps.getProperty("workerthreads", String.valueOf(workers)));
        } catch (NumberFormatException nfe) {
            System.out.println("Error in properties file: " + nfe.getMessage());
            System.exit(0);
        }

        KeyStore ks = loadKeyStore(keystoreName);
        KeyStore ts = loadKeyStore(truststoreName);
        SSLContext sslContext = CreateContext(ks, ts);

        Undertow server = Undertow.builder()
                .setIoThreads(iothreads)
                .setWorkerThreads(workers)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpsListener(httpPort, httpHost, sslContext)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {
                        ServeHttpRequests(exchange, bindAddess);
                        exchange.endExchange();
                        exchange = null;
                    }
                })
                .build();
        server.start();
    }

    private void HttpServer(int servePort, String host, String bindAddess) {

        // -------------- Reset to site Parameters !! ------------------------
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        siteProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(siteProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        siteProps.clear();
        // -------------------------------------------------------------------
        siteSecret  = runProps.getProperty("secret", "");
        String broker = runProps.getProperty("broker", "");
        if (!broker.equals("") && !(broker == null)) {
            Properties bProps = LoadProperties(BaseCamp + "/conf/" + broker);
        }

        try {
            iothreads = Integer.valueOf(runProps.getProperty("iothreads", String.valueOf(iothreads)));
            workers   = Integer.valueOf(runProps.getProperty("workerthreads", String.valueOf(workers)));
        } catch (NumberFormatException nfe) {
            System.out.println("Error in properties file: " + nfe.getMessage());
            System.exit(0);
        }

        Undertow server = Undertow.builder()
//                .setIoThreads(iothreads)
//                .setWorkerThreads(workers)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpListener(servePort, host)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws JMSException {
                        ServeHttpRequests(exchange, bindAddess);
                        exchange.endExchange();
                        exchange = null;
                    };
                })
                .build();
        server.start();
    }

    private void CheckReply() {
        if (reply.startsWith("<<")) {
            reply = reply.replaceAll("\\<\\<", "");
            reply = reply.replaceAll("\\>\\>", "");
        }
        if (reply.startsWith("FAIL")) { 
            LogEvent(reply);
            status = 400; 
        }
    }

    private void ServeHttpRequests(HttpServerExchange exchange, String bindAddess) throws JMSException {
        if (isSecretError(exchange)) {
            CheckReply();
            exchange = SetAndSend(exchange, status, reply);
            return;
        }

        procID++;
        if (procID > iothreads) procID = 1;

        this.httpMethod = exchange.getRequestMethod().toString();
        this.httpQuery = exchange.getQueryString();
        this.inPath = exchange.getRelativePath();
        requestorIP = "";

        this.status = 200;
        this.reply = "";
        iam = Thread.currentThread().getName();
        System.out.println("");
        LogEvent("ReceivedRequest(" + iam + ") *************************************************");

        undertowOkay = false;
        inRequest = "";
        dbgMessage = "";
        if (httpMethod.equals("POST")) {
            getPing = false;
            dbgMessage += "http POST\n";
            while (inRequest.equals("")) {
                exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                            String req = new String(data);
                            inRequest = req;
                            if (req.equals("")) {
                                inRequest = "disallowed";
                                LogEvent("empty POST request - " + inRequest);
                                status = 400;
                                reply  = "\"Your request did not have a body.\"";
                            }
                            requestorIP = body.getSourceAddress().toString();
                            requestorIP = (requestorIP+": ").split(":")[0].replaceAll("/","");
                            // can now build a whitelist & blacklist of requestors
                            if (!req.equals("")) {
                                LogEvent("ProcessRequest() from " + requestorIP);
                                if (blacklist.indexOf(requestorIP) < 0) {
                                    try {
                                        this.reply = ProcessRequest(req);
                                        undertowOkay = true;
                                    } catch (JMSException e) {
                                        System.out.println(e.getMessage());
                                    }
                                } else {
                                    reply = "\"You are blocked.\"";
                                    inRequest = "disallowed";
                                    LogEvent(reply + " " +inRequest);
                                    status = 400;
                                }
                            }
                        },
                        (body, exception) -> {
                            inRequest = exception.toString();
                            undertowOkay = true;
                            dbgMessage += "http exception \n";
                            LogEvent(" ");
                            LogEvent("An exception has occurred: " + inRequest);
                            LogEvent(" ");
                            this.reply = ErrorHandler(exception.toString());
                        }
                );
                if (inRequest.equals("")) {
//                    System.out.println("                        **** Missed the request, trying again *****");
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                       //
                    }
                }
            }
            undertowOkay = true;
        } else {
            getPing = true;
            undertowOkay = true;
            if (httpMethod.equals("GET") || httpMethod.equals("PUT")) {
                reply = ProcessGetRequest(httpQuery);
                LoadValidUsers();
            } else {
                undertowOkay = false;
                dbgMessage = "Unhandled method: " + httpMethod;
            }
        }

        if (!undertowOkay) {
            uSendMessage("-------------------------------------------------------------");
            uSendMessage("Method  : " + httpMethod);
            uSendMessage("request : " + inRequest);
            uSendMessage("Issue   : " + dbgMessage);
            uSendMessage("-------------------------------------------------------------");
        }

        CheckReply();
        if (!reply.startsWith("[")) {
            reply = ResponseFormatter(String.valueOf(this.status), "", reply);
        } else {
            reply = "{ \"body\": " + reply + "}";
        }
        LogEvent("Response: (" + Thread.currentThread().getName() + ") returned (" + reply.length() + ") bytes.");
        exchange = SetAndSend(exchange, status, reply);
        if (reply.contains("The RPC failed")) {
            System.out.println(" ");
            System.out.println("****************************************************");
            System.out.println("RPC Issues with source database. Shutting down now.");
            System.out.println("****************************************************");
            System.out.println(" ");
            // will be restarted by supervisorD
            exchange.endExchange();
            System.exit(1);
        }
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage="";
    }

    private boolean isSecretError(HttpServerExchange exchange) {
        boolean isErr = false;
        if (!siteSecret.equals("")) {
            HeaderMap headerMap = exchange.getRequestHeaders();
            boolean isThere = headerMap.contains("x-rfuel-api");
            String secret = "";
            if (isThere) {
                isErr = false;
                secret = headerMap.get("x-rfuel-api").toString();
                if (secret == null) secret = "";
                secret = secret.replace("[", "");
                secret = secret.replace("]", "");
                if (NamedCommon.cSeed.equals("")) {
                    if (!secret.equals(siteSecret)) isErr = true;
                    if (isErr) System.out.println(("SiteSecret() is in ERROR : [" + secret + "]"));
                } else {
                    /// set siteSecret to "Encrypted-client-base";
                    String str = uCipher.Decrypt(secret + "~" + NamedCommon.cSeed + "~");
                    String[] parts = (str + " * *").split("\\*");
                    String cust = parts[0].trim();
                    String expr = parts[1].trim();
                    if (sysUsers.indexOf(cust) >=0) {
                        // check licence expiry
                        if (expr.equals("")) expr = "0";
                        if (!License.CheckExpiryDate(expr)) {
                            isErr = true;
                            reply = "[" + cust + "] licence has expired.";
                        }
                    } else {
                        isErr = true;
                        reply = "[" + cust+ "] is NOT a valid user";
                    }
                }
            }
            headerMap = null;
            secret = "";
            if (isErr) {
                status = 400;
                if (reply. equals("")) reply = "No assistance provided, refer to logs.";
                LogEvent("SiteSecret() ERROR " + reply);
                reply = ResponseFormatter(String.valueOf(status), "", reply);
            }
        }
        return isErr;
    }

    private String ProcessGetRequest(String query) throws JMSException {
        status = 200;
        if (query.equals("") || query.equals("ping")) {
            uSendMessage(" Health_checker(ping)");
            return "success";
        }
        boolean isAdminTask = false;
        String[] params = (query + "&").split("&");
        for (int p=0 ; p < params.length ; p++) {
            String[] qTmp = (params[p] + "=").split("=");
            switch (qTmp[0].toLowerCase()) {
                case "whitelist":
                    if (qTmp.length == 2) {
                        whitelist.add(qTmp[1]);
                        if (blacklist.indexOf(qTmp[1]) >= 0) blacklist.remove(blacklist.indexOf(qTmp[1]));
                    }
                    isAdminTask = true;
                    break;
                case "blacklist":
                    if (qTmp.length == 2) {
                        if (blacklist.indexOf(qTmp[1]) < 0) blacklist.add(qTmp[1]);
                    }
                    isAdminTask = true;
                    break;
            }
        }
        if (isAdminTask)  return "Done.";

        query = query.replaceAll("\\&", "<tm>");
        query = query.replaceAll("\\=", "<is>");
        query = msgCommons.jsonifyMessage(query);
        try {
            query = URLDecoder.decode(query, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            LogEvent(e.getMessage());
            return e.getMessage();
        }
        this.reply = ProcessRequest(query);
        return this.reply;
    }

    private HttpServerExchange SetAndSend(HttpServerExchange exchange, int s, String reply) {
        exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), "application/json");
        exchange.getResponseHeaders().put((Headers.STATUS), s);
        exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
        exchange.getRequestHeaders().put((Headers.LOCATION), bindAddess);
        exchange.setStatusCode(s);
        exchange.getResponseSender().send(reply);
        if (!getPing) lastActive = System.nanoTime();
        return exchange;
    }

    private String ProcessRequest(String request) throws JMSException {
        String answer = "";

        if (!DecodeRequest(request)) {
            status=400;
            String errString = "Message decoding failed. Message is not in json or standard rfuel format";
            LogEvent(errString);
            return errString;
        }

        if (!isValidRequest(request)) {
            this.status = 400;
            String errString = "Failed internal validation";
            LogEvent(errString);
            return errString;
        }

        if (mqQue.equals(tester)) getQ = mqQue;       // for (Arq) Simon's tests

        if (jms2jms) {
            NamedCommon.isWebs = false;
            answer = JmsInOut(getQ, request);
        } else {
            NamedCommon.isWebs = true;
            answer = HttpInOut(getQ, request);
        }

        Runtime.getRuntime().gc();
        return answer;
    }

    private boolean isValidRequest(String req) {
        if (GetDetail("task").equals("999")) {
            LogEvent("No task assigned");
            return true;
        }
        if (GetDetail("replyto").equals("") && jms2jms) {
            LogEvent("No replyto in message");
            return false;
        }
        if (GetDetail("correlationid").equals("")) {
            LogEvent(" No correlationId");
            return false;
        }

        if (req.contains("action")) {
            if (!req.contains("customer")) return false;
        } else {
            this.strJunk = GetDetail("task");
            if (this.strJunk.equals("")) {
                LogEvent("no task or action");
                return false;
            }
            switch (this.strJunk) {
                case "050":
                    if (GetDetail("grp").equals("")) {
                        if (GetDetail("map").equals("")) {
                            LogEvent("no map");
                            return false;
                        }
                        if (GetDetail("item").equals("")) {
                            LogEvent("no item");
                            return false;
                        }
                    }
                    break;
                case "055":
                    if (GetDetail("mscat").equals("")) {
                        LogEvent("no mscat");
                        return false;
                    }
                    // ----------------------------------------------------------
                    // NB:  Not every 055 message has a payload. For example;
                    //      when they want to run a program of subroutine, they
                    //      do not need to send a payload.
                    // ----------------------------------------------------------
//                    if (reqKeys.indexOf("payload") < 0) return false;
//                    if (GetDetail("payload").equals("")) return false;
                    break;
                default:
            }
        }
        return true;
    }

    private String JmsInOut(String getQ, String request) throws JMSException {
        String answer = "";

        if (reqVals.size() == 0) {
            this.status = 400;
            String errString = "Failed key:value pair validation";
            LogEvent(errString);
            return errString;
        }

        this.status = 200;

        // -----------------------------------------------------------------
        String replyto= GetDetail("replyto");
        if (replyto.equals("")) replyto = getQ;
        correlationID = GetDetail("correlationid");
        NamedCommon.CorrelationID = correlationID;
        request = StringifyRequest();
        jmsp.sendMsg(request);
        String filter = "JMSCorrelationID = '" + correlationID + "'";
        jmsc.SetFilter(filter);
        LogEvent("Produce request  for  CorrelationID " + correlationID);
        LogEvent("Consume response for  CorrelationID " + correlationID + " at Queue " + replyto);
        answer = jmsc.consume(replyto);
        // -----------------------------------------------------------------

        return answer;
    }

    private String StringifyRequest() {
        StringBuilder answer = new StringBuilder();
        boolean hasReplyTo = false;
        String zkey, zval;
        for (int i=0 ; i < reqKeys.size(); i++) {
            zkey = reqKeys.get(i);
            zval = reqVals.get(i);
            answer.append(zkey.toUpperCase() + is + zval + tm);
            if (zkey.toLowerCase().equals("replyto")) hasReplyTo = true;
        }
        if (!hasReplyTo) {
            answer.append("replyto" + is + getQ);
        }
        return answer.toString();
    }

    private boolean DecodeRequest(String request) {

        if (request.equals("")) return false;

        // ----------------------------------------------------------------------
        // Normally, we ask for the request to be in a json "request" object
        //      however, with OpenAPI and Mulesoft, this is really hard to define
        //      in yaml / raml
        // So, IF the request does not have the "request" object, get key-values
        //      without the jHeader.
        // ----------------------------------------------------------------------

        reqKeys = null;
        reqVals = null;
        reqKeys = new ArrayList<>();
        reqVals = new ArrayList<>();
        reqKeys.clear();
        reqVals.clear();
        String tempVer = "";

        try {
            JSONObject obj = null;
            obj = new JSONObject(request);
            Iterator<String> jKeys = null;
            if (request.contains(jHeader)) {
                jKeys = obj.getJSONObject(jHeader).keys();
            } else {
                jKeys = obj.keys();
            }
            String zkey, zval;
            while (jKeys.hasNext()) {
                zkey = jKeys.next();
                if (request.contains(jHeader)) {
                    zval = obj.getJSONObject(jHeader).get(zkey).toString();
                } else {
                    zval = obj.get(zkey).toString();
                }
                if (zkey.toLowerCase().equals("correlationid")) {
                    zval = zval.replaceAll("\\.", "_");
                }
                reqKeys.add(zkey.toLowerCase());
                reqVals.add(zval);
                int posx;
                switch (zkey) {
                    case "action":
                        // swap action for map
                        posx = reqKeys.indexOf(zkey.toLowerCase());
                        zkey = "map";
                        reqKeys.set(posx,zkey);
                        zkey = "task";
                        zval = "050";
                        reqKeys.add(zkey.toLowerCase());
                        reqVals.add(zval);
                        break;
                    case "customer":
                        // swap customer for item
                        posx = reqKeys.indexOf(zkey.toLowerCase());
                        zkey = "item";
                        reqKeys.set(posx,zkey);
                        break;
                    case "version":
                        // stash it away until all properties are obtained
                        tempVer = zval;
                        break;
                }
            }
            if (!tempVer.equals("")) {
                zkey = "map";
                int posx = reqKeys.indexOf(zkey.toLowerCase());
                zval = reqVals.get(posx);
                zval = tempVer + "/" + zval;
                reqVals.set(posx, zval);
            }
            obj = null;
        } catch (JSONException je) {
            if (request.startsWith("{")) {
                LogEvent("Not a json payload");
                return false;
            }
            LogEvent("Not JSON, so assuming this is an rFuel message!");
            request = request.replaceAll("\\r?\\n", "");
            request = request.replace("<IS>", "<is>");
            request = request.replace("<Is>", "<is>");
            request = request.replace("<iS>", "<is>");
            request = request.replace("<TM>", "<tm>");
            request = request.replace("<Tm>", "<tm>");
            request = request.replace("<tM>", "<tm>");

            String[] mParts = request.split("<tm>");
            int eop = mParts.length;
            if (eop == 1) {
                LogEvent("rFuel message is empty");
                return false;
            }
            String line, mKey, mVal;
            for (int p=0; p < eop ; p++) {
                line = mParts[p];
                if (line.replace(" ", "").equals("")) continue;;
                String[] lParts = line.split("<is>");
                mKey = lParts[0];
                mVal = "";
                if (lParts.length > 0 && !line.endsWith("<is>")) mVal = lParts[1];
                reqKeys.add(mKey.toLowerCase());
                reqVals.add(mVal);
            }

            int chk = reqKeys.indexOf("format");
            if (chk < 0) {
                reqKeys.add("format");
                reqVals.add(NamedCommon.esbfmt);
            } else {
                String schk = reqVals.get(chk);
                switch (schk.toUpperCase()) {
                    case "XML":
                        break;
                    case "JSON":
                        break;
                    default:
                        reqVals.set(chk, NamedCommon.esbfmt);
                }
            }
        }
        return true;
    }

    private String GetDetail(String ppty) {
        int posx = reqKeys.indexOf(ppty.toLowerCase());
        if (posx < 0) return "";
        return reqVals.get(posx);
    }

    private String HttpInOut(String getQ, String request) {
        String answer = "", response = "";
        NamedCommon.Reset();
        NamedCommon.BaseCamp = BaseCamp;
        APImsg.instantiate();

//        request = StringifyRequest();
        uCommons.MessageToAPI(request);
        if (NamedCommon.ZERROR) {
            this.status = 400;
            LogEvent(NamedCommon.Zmessage);
            return NamedCommon.Zmessage;
        }

        NamedCommon.Zmessage = "";
        NamedCommon.ReturnCodes = ReturnCodes;
        NamedCommon.MessageID = NamedCommon.pid + "_" + msgCounter;
        msgCounter++;
        boolean isJson = APImsg.APIget("format").toLowerCase().equals("json");
        answer = MessageProtocol.handleProtocolMessage(GetDetail("task"), "1", request);
        if (NamedCommon.ZERROR) {
            if (isJson) {
                strJunk = "\"status\"";
            } else {
                strJunk = "<status>";
            }
            intJunk = answer.indexOf(strJunk);
            if (intJunk >= 0) {
                intJunk = intJunk + strJunk.length();
                if (isJson) intJunk = intJunk + 1;   // cater for the ":" in json
                strJunk = answer.substring(intJunk, intJunk + 3);
                try {
                    this.status = Integer.valueOf(strJunk);
                } catch (NumberFormatException nfe) {
                    LogEvent(nfe.getMessage());
                    this.status = 400;
                }
            }
            NamedCommon.uStatus = String.valueOf(status);
        }
        if (GetDetail("task").equals("055") && !NamedCommon.isWebs) {
            uCommons.Sleep(0);
            response = u2Commons.ReadAnItem("uRESPONSES", NamedCommon.zID, "1", "", "");
            strJunk = "<status>";
            intJunk = response.indexOf(strJunk);
            if (intJunk >= 0) {
                intJunk = intJunk + strJunk.length();
                strJunk = response.substring(intJunk, intJunk + 3);
                try {
                    this.status = Integer.valueOf(strJunk);
                } catch (NumberFormatException nfe) {
                    LogEvent(nfe.getMessage());
                    this.status = 400;
                }
                u2Commons.DeleteAnItem("uRESPONSES", NamedCommon.zID);
                answer = DataConverter.ResponseHandler(String.valueOf(this.status), "DONOTALTER", response, GetDetail("FORMAT").toUpperCase());
            } else {
                answer = DataConverter.ResponseHandler(String.valueOf(this.status), "DONOTALTER", answer, GetDetail("FORMAT").toUpperCase());
            }
        }
        response = "";
        request  = "";
        this.status = Integer.valueOf(NamedCommon.uStatus);
        return answer;
    }

    private void LogEvent(String s) { uSendMessage(s); }

    private void uSendMessage(String inMsg) {
        if (inMsg == null) return;
        String mTime, mDate, MSec;
        int ThisMS;
        mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
        if (NamedCommon.ZERROR) {
            mDate = "**** ERROR";
        } else {
//            mDate = iam;
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

    private String ErrorHandler(String error) {
        LogEvent("ERROR (1): " + error);
        String answer = "";
        status = 400;
        answer = ResponseFormatter(String.valueOf(status), "", error);
        return answer;
    }

    private String ResponseFormatter(String status, String descr, String response) {
        if (response.startsWith("{")) return response;
        if (response.toLowerCase().startsWith("<?xml")) return response;
        String replyMessage = "{\"response\": " + response + "}";
        return replyMessage;
    }

    private static String ReadDiskRecord(String infile) {

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
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    System.out.println("read FAIL on " + infile);
                    System.out.println(e.getMessage());
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
            }
        } catch (IOException e) {
            System.out.println("-------------------------------------------------------------------");
            System.out.println("File Access FAIL :: " + infile);
            System.out.println(e.getMessage());
            System.out.println("-------------------------------------------------------------------");
        }
        return rec;
    }

    private char[] password(String name) {
        String pw = name + ".password";
        if (keystoreNames.indexOf(name) < 0) {
            System.out.println("     location for [" + name + ".password] has not been provided.");
            System.exit(1);
        } else {
            pw = keystoreLocns.get(keystoreNames.indexOf(name)) + ".password";
        }
        String pword = ReadDiskRecord(pw);
        while (pword.endsWith("\n")) { pword = pword.substring(0,pword.length()-1); }
//        if (pword.startsWith("ENC(")) {
//            pword = pword.substring(4, pword.length());
//            while (!pword.endsWith(")") && pword.length() > 0) {
//                pword = pword.substring(0, pword.length() - 1);
//            }
//            if (pword.endsWith(")")) pword = pword.substring(0, pword.length() - 1);
//            pword = uCipher.Decrypt(pword);
//        }
        return pword.toCharArray();
    }

    private SSLContext CreateContext(KeyStore kstore, KeyStore tstore) throws Exception {
        KeyManager[] keyManagers;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try {
            keyManagerFactory.init(kstore, password(keystoreName));
        } catch (UnrecoverableKeyException e) {
            System.out.println(">>> SECURITY ABORT: SSLContext for KeyStore: invalid password ");
            System.exit(1);
        }
        keyManagers = keyManagerFactory.getKeyManagers();

        TrustManager[] trustManagers;
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(tstore);
        trustManagers = trustManagerFactory.getTrustManagers();

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, null);

        return sslContext;
    }

    private KeyStore loadKeyStore(String name) throws Exception {
        String storeLoc = runProps.getProperty(name, "");
        // ---------------------------------------------------------------------
        if (storeLoc.equals("")) {
            System.out.println("[      ] No TLS cert found. HTTPS listener unavailable. Will guide you through generation now.");
            File directory = new File(String.valueOf("/upl/ssl/"));
            if (!directory.exists()) {
                directory.mkdir();
                if (!uCommons.WriteDiskRecord(NamedCommon.BaseCamp+"/ssl/test.txt", "delete me")) {
                    System.out.println("[      ] Cannot create directory: "+NamedCommon.BaseCamp+"/ssl/");
                    System.exit(1);
                }
            }
            System.out.println("[      ] To enable secure communication, please follow the steps below.");
            Scanner reader = new Scanner(System.in);
            System.out.println("[      ] Shall we generate a Certificate Signing Request (CSR) to send to your CA for signing?\n" +
                    "[Y] Yes\n" +
                    "[N] No");
            String yn = reader.nextLine();
            reader.close();
            if (yn.toUpperCase().equals("N")) {
                System.out.println("[CREATE] Okay - stopping now.");
                System.exit(1);
            }
            String CN = "rfuel."+NamedCommon.uplSite.toLowerCase()+".internal";
            String cmd ="openssl req -new -newkey rsa:2048 -nodes \\\n" +
                    "  -keyout rfuel.key \\\n" +
                    "  -out rfuel.csr \\\n" +
                    "  -subj \"/CN="+CN+"/O="+NamedCommon.uplSite+"/OU=Data Services\"";
//            uCommons.nixExecute(cmd, false);
            cmd = "mv ./rfuel.csr "+NamedCommon.BaseCamp+"/ssl/rfuel.csr";
//            uCommons.nixExecute(cmd, false);
            System.out.println("[CREATE] ");
            System.out.println("[CREATE] fuel.csr is found here: "+NamedCommon.BaseCamp+"/ssl/rfuel.csr ");
            System.out.println("[CREATE] ");
            System.out.println("[CREATE] Use an ftp client to obtain it and send it for signing, internally.");
            System.out.println("[CREATE] ");
            System.out.println("Dear Security Team,\n" +
                    "Attached is the CSR for rFuel which was just created.\n" +
                    "Please review and sign using your internal CA so we can enable secure HTTPS communications.\n" +
                    "CSR File   : rfuel.csr\n" +
                    "Expected CN: "+CN+"\n");
            System.out.println(" ");
            System.out.println("When the csr is signed and returned:");
            System.out.println("1. Copy the certificate to "+NamedCommon.BaseCamp+"/ssl/rfuel.cer ");
            System.out.println("2. Modify webDS.properties:");
            System.out.println("  a. server.keystore="+NamedCommon.BaseCamp+"/ssl/rfuel.cer ");
            System.out.println("  b. server.truststore="+NamedCommon.BaseCamp+"/ssl/rfuel.cer ");
            System.out.println(" ");
            Scanner rdr = new Scanner(System.in);
            System.out.println("Please press enter so I can stop.\n");
            yn = rdr.nextLine();
            rdr.close();
            System.exit(1);
        }
        // ---------------------------------------------------------------------
        if (keystoreNames.indexOf(name) < 0) {
            keystoreNames.add(name);
            keystoreLocns.add(storeLoc);
        }
        final InputStream stream;
        if (storeLoc == null) {
            stream = http2jms.class.getResourceAsStream(name);
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

    private static Properties LoadProperties(String fname) {
        Properties lProps = new Properties();
        if (!fname.contains("/")) { fname = BaseCamp + "/conf/" + fname; }
        InputStream is;
        try {
            is = new FileInputStream(fname);
        } catch (FileNotFoundException e) {
            is = null;
        }
        if (is != null) {
            try {
                lProps.load(is);
            } catch (IOException e) {
                System.out.println("Using properties file: " + fname + "  ::  "  + "Cannot find " + e.getMessage());
            } catch (IllegalArgumentException iae) {
            }
            try {
                is.close();
                is = null;
            } catch (IOException e) {
                System.out.println("Cannot close '" + fname + "'  " + e.getMessage());
            }
        } else {
            System.out.println("Please load '" + fname + "'");
        }
        return lProps;
    }

    private static void SetupReturnCodes() {
        ReturnCodes = new ArrayList<>();
        for (int rc = 0; rc < 1100; rc++) { ReturnCodes.add(""); }
        int nbrvals;
        String hCodes = ReadDiskRecord(BaseCamp + "/conf/http-return-codes.csv");
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

    private static boolean iamLicenced() {
        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);

        if (NamedCommon.upl.equals("")) {
            NamedCommon.upl = System.getProperty("user.dir");
            NamedCommon.BaseCamp = NamedCommon.upl;
        }
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            uCommons.SetMemory("domain", dbfHost);
            NamedCommon.BaseCamp = "\\\\"+dbfHost+"\\all\\upl";
            NamedCommon.upl = NamedCommon.BaseCamp;
            NamedCommon.slash = "/";
            isDev = true;
        }
        BaseCamp = NamedCommon.BaseCamp;

        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        siteProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(siteProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        siteProps.clear();
        return License.IsValid();
    }

    private void WaitForRequests() {
        if (!monitor) return;

        System.out.println();
        System.out.println("*** waiting on requests with heartbeats every " + heartbeat + " seconds ***");
        System.out.println();

        lastActive = System.nanoTime();
        int idleSeconds;
        String pingResponse, hb;
        Properties chkProps = null;
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
            if (idleSeconds > heartbeat) {
                if (!pingReq.equals("")) {
                    pingResponse = "";
                    try {
                        pingResponse = ProcessRequest(pingReq);
                    } catch (JMSException e) {
                        //
                    }
                    if (NamedCommon.ConnectionError) {
                        System.out.println("Experiencing RPC issues while waiting for requests.");
                        System.out.println("Trying to re-establish a connection with your Source DB");
                        SourceDB.ReConnect();
                        if (NamedCommon.ZERROR) System.exit(0);
                    } else {
                        DecodeRequest(pingReq);
                        if (GetDetail("ping").equals("show")) {
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
                                    System.out.println("**");
                                    System.out.println(out.toString());
                                    System.out.println("**");
                                } catch (IOException e) {
                                    System.out.println(e.getMessage());
                                } catch (SAXException e) {
                                    System.out.println(e.getMessage());
                                } catch (ParserConfigurationException e) {
                                    System.out.println(e.getMessage());
                                }
                                out = null;
                                domBuilder = null;
                                xmlDoc = null;
//                                format = null;
//                                serializer = null;
                            } else {
                                JSONObject jObj = new JSONObject(pingResponse);
                                System.out.println(jObj.toString(4));
                                jObj = null;
                            }
                        }
                    }
                }
                SourceDB.ReConnect();
                chkProps  = LoadProperties(cfile);
                monitor   = chkProps.getProperty("monitor", "false").toLowerCase().equals("true");
                pingReq   = chkProps.getProperty("ping", "");
                hb        = chkProps.getProperty("heartbeat", "30");
                try {
                    heartbeat = Integer.valueOf(hb);
                } catch (NumberFormatException nfe) {
                    System.out.println("A valid heartbeat has not been provided - defauilting 300");
                    heartbeat= 300;
                }
                chkProps = null;
                hb = "";
                lastActive = System.nanoTime();
                if (!monitor) return;
                System.out.println();
                System.out.println("Accepting requests on " + bindAddess);
                System.out.println("*** with heartbeats every " + heartbeat + " seconds ***");
                System.out.println();
            } else {
                uCommons.Sleep(0);
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        //   Receive HTTPS (POST) requests.
        //   Sends the body to JMS queue !!
        //   Wait for the response at a predefined queue
        //   Return the response to the requestor.

        if (!iamLicenced()) { System.out.println("licence failure"); System.exit(1); }
        NamedCommon.ShowDateTime = true;

        SetupReturnCodes();
        new http2jms();
    }

}
