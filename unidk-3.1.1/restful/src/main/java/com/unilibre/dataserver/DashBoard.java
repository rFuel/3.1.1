package com.unilibre.dataserver;

/*  Simple HTTP web service for rFuel related artefacts */
/*  IF https is required, see http2jms for SecureServer */
/*  This handles RESTful requests from web pages in /upl/dashboard */
/*  To serve the web pages, run "python3 -m http.server 8080" in /upl/dashboard */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

public class DashBoard {

    private static final String spcs = "                                                   ";
    private static Properties siteProps;
    private static Properties runProps;
    private static int httpPort;
    private static int iothreads = 1;
    private static int workers = 10;
    private static int heartbeat = 300;
    private static boolean isDev = false;
    private static boolean isSSE = false;  // socket streaming events
    private static boolean getPing = false;
    private static boolean monitor = false;
    private static boolean stopNow = false;
//    private String iam;
    private static String bindAddess = "", searchPath="", cors_Origin="*";
    private static String reply, httpHost, httpPath, cfile, event;
    private static String JOLOKIA_URL, USERNAME, PASSWORD, ORIGIN;
    private int status = 0;
    private static long  lastActive, rightNow;
    private static double laps, div = 1000000000.00;


    private DashBoard() throws Exception {
        WebServer();
        WaitForRequests();          // loop infinitely to watch for idle time       //
    }

    private void WebServer() throws Exception {

        cfile = System.getProperty("conf", "webDS.properties");
        runProps = uCommons.LoadProperties(cfile);
        uCommons.uSendMessage("configuring according to " + cfile);

        monitor     = runProps.getProperty("monitor", "false").toLowerCase().equals("true");
        httpHost    = runProps.getProperty("httphost", "");
        if (isDev) httpHost = "192.168.48.1";
        httpPath    = runProps.getProperty("httppath", "/api/webDS/");
        String webH = runProps.getProperty("webhost", "");
        String webP = runProps.getProperty("webport", "");
        if (!webH.equals("") && !webP.equals("")) cors_Origin = "http://"+webH+":"+webP;

        try {
            String hb   = runProps.getProperty("heartbeat", "30");
            heartbeat = Integer.valueOf(hb);
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("Heartbeat not been provided - defauilting to 300");
            heartbeat = 300;
        }

        try {
            String tmp = runProps.getProperty("httpport", "");
            httpPort = Integer.valueOf(tmp);
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("Port number not been provided - defauilting to 8188");
            httpPort = 8188;
        }

//        iam    = "main";
        reply  = "";

        uCommons.uSendMessage("*");
        uCommons.uSendMessage("-------------------------------------------------------");
        uCommons.uSendMessage("-------Ensure that uRestResponder is NOT running-------");
        uCommons.uSendMessage("---------------- Create Undertow Server----------------");

        bindAddess = "http" + "://" + httpHost + ":" + httpPort + httpPath;
        uCommons.uSendMessage("------ Binding HTTP service at " + bindAddess+" -------");
        HttpServer(httpPort, httpHost, bindAddess);

        uCommons.uSendMessage("Set CORS Access-Control-Allow-Origin: "+cors_Origin);
        uCommons.uSendMessage("    ... where the web pages are served.");
        uCommons.uSendMessage("[ Serving Requests ] --------------------------------------------");
        uCommons.uSendMessage("Accepting requests on " + bindAddess);
        uCommons.uSendMessage("-----------------------------------------------------------------");
        uCommons.uSendMessage("*");
        uCommons.uSendMessage("*");
    }

    private void WaitForRequests() {
        if (!monitor) return;

        uCommons.uSendMessage("*");
        uCommons.uSendMessage("*** waiting on requests with heartbeats every " + heartbeat + " seconds ***");
        uCommons.uSendMessage("*");

        lastActive = System.nanoTime();
        int idleSeconds;
        String hb;
        Properties chkProps = null;

        while (!stopNow) {
            rightNow = System.nanoTime();
            laps = (rightNow - lastActive) / div;
            idleSeconds = ((int) laps);
            if (idleSeconds > heartbeat) {
                chkProps  = uCommons.LoadProperties(cfile);
                monitor   = chkProps.getProperty("monitor", "false").toLowerCase().equals("true");
                hb        = chkProps.getProperty("heartbeat", "30");
                try {
                    heartbeat = Integer.valueOf(hb);
                } catch (NumberFormatException nfe) {
                    uCommons.uSendMessage("A valid heartbeat has not been provided - defauilting 300");
                    heartbeat= 300;
                }
                chkProps = null;
                hb = "";
                lastActive = System.nanoTime();
                if (!monitor) return;
                uCommons.uSendMessage("*");
                uCommons.uSendMessage("Accepting requests on " + bindAddess);
                uCommons.uSendMessage("*** with heartbeats every " + heartbeat + " seconds ***");
                uCommons.uSendMessage("*");
            } else {
                uCommons.Sleep(0);
            }
        }
    }

    private void HttpServer(int servePort, String host, String bindAddess) {

        // -------------- Reset to site Parameters !! ------------------------
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        siteProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(siteProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        // -------------------------------------------------------------------
        siteProps.clear();

        try {
            iothreads = Integer.valueOf(runProps.getProperty("iothreads", String.valueOf(iothreads)));
            workers   = Integer.valueOf(runProps.getProperty("workerthreads", String.valueOf(workers)));
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("Error in properties file: " + nfe.getMessage());
            System.exit(0);
        }

        isSSE = false;
        Undertow server = Undertow.builder()
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpListener(servePort, host)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {
                        ServeHttpRequests(exchange);
                        if (!isSSE) {
                            exchange.endExchange();
                            exchange = null;
                        } else {
                            uCommons.uSendMessage("Long running job - exchange is NOT ended or closed.");
                        }
                    };
                })
                .build();
        server.start();
    }

    private void ServeHttpRequests(HttpServerExchange exchange) throws Exception {

        lastActive = System.currentTimeMillis();
        this.status = 200;
        this.reply = "";
        this.event = "";
        isSSE = false;
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage="";

        event = ("\""+exchange.getRequestMethod()+"\""+spcs).substring(0,10) + exchange.getRequestPath();

        // -----------------------------------------------------------------------------------------
        //       Serve the end-points
        // -----------------------------------------------------------------------------------------

        exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Origin"), cors_Origin);
        exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Methods"), "GET, POST, OPTIONS");
        exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Headers"), "Content-Type");

        if (exchange.getRequestMethod().equalToString("OPTIONS")) {
            exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Origin"), cors_Origin);
            exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Methods"), "GET, POST, OPTIONS");
            exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Headers"), "Content-Type");
            exchange.setStatusCode(204);
            exchange.endExchange();
            LogEventHandler();
            return;
        }

        if (exchange.getRequestPath().equals("/api/rfuel/status")) {
            String cmd = "'/upl/jstat.sh"+"'";
            reply = runScript(cmd);
            Map<String, List<Object>> structured = JstatOutputParser.parse(reply);
            String json="";
            try {
                json = new ObjectMapper().writeValueAsString(structured);
            } catch (Exception e) {
                System.out.println("ObjectMapper ERROR: "+e.getMessage());
            }
            reply = json;
            exchange = SetAndSend(exchange, status, reply);
            LogEventHandler();
            return;
        }

        if (exchange.getRequestPath().equals("/api/rfuel/logs")) {
            JSONObject jObj = jolokiaRequest(searchPath);
            reply = jObj.toString();

            String qname;
            int qcount,qsize;

            JSONObject obj = new JSONObject(reply);
            JSONArray jArr = new JSONArray();

            JSONObject ans = new JSONObject();
            ans.put("Name", "901-SQL-inserts.log");
            ans.put("Consumers", 0);
            ans.put("Messages", 0);
            jArr.put(ans);
            ans = null;

            JSONArray mbeans = obj.getJSONArray("value");
            for (int i = 0; i < mbeans.length(); i++) {
                String mbean = mbeans.getString(i);
                qname = extractQueueName(mbean);
                String readPath = "/read/" + URLEncoder.encode(mbean, "UTF-8");
                JSONObject queueData = jolokiaRequest(readPath);
                JSONObject value = queueData.getJSONObject("value");
                qcount = value.getInt("ConsumerCount");
                qsize = value.getInt("QueueSize");

                if (qcount > 0) {
                    ans = new JSONObject();
                    ans.put("Name", qname);
                    ans.put("Consumers", qcount);
                    ans.put("Messages", qsize);
                    jArr.put(i, ans);
                }

                queueData = null;
                value = null;
            }

            ans = new JSONObject();
            ans.put("queues", jArr);
            reply = ans.toString();
            ans = null;
            jArr= null;
            jObj= null;
            obj = null;
            mbeans = null;

            exchange = SetAndSend(exchange, status, reply);
            LogEventHandler();
            return;
        }

        if (exchange.getRequestPath().startsWith("/api/rfuel/logs/stream")) {
            // e.g. /api/rfuel/logs/stream?name=012_Fetch_001
            String[] parts = exchange.getRequestPath().split("/");
            String logName = parts[parts.length - 1];
            LogStreamer ls = new LogStreamer();
            ls.setLogName(logName);
            ls.setBaseCamp(NamedCommon.BaseCamp);
            // Dispatch safely from here before doing anything else
            HttpServerExchange finalExchange = exchange;
            exchange.dispatch(() -> {
                ls.setExchange(finalExchange);  // now safe to pass the exchange
                // Set headers inside the same thread that handles the response
                finalExchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Origin"), "*");
                finalExchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Methods"), "GET, OPTIONS");
                finalExchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Headers"), "Content-Type");
                finalExchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/event-stream");
                ls.StartStreamer();             // this runs on worker thread
            });
            isSSE = true;
            LogEventHandler();
            return;
        }

    }

    private String runScript(String script) throws IOException {
        if (isDev) {
            ProcessBuilder builder = new ProcessBuilder("ssh",  "unilibre@rfuel14", "bash", "-c", script);
            Process process = builder.start();
            InputStream inputStream = process.getInputStream();
            reply = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } else {
            try {
                ProcessBuilder builder = new ProcessBuilder("bash", "-c", script);
                builder.redirectErrorStream(true);
                Process process = builder.start();
                reply = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                reply = "Script error: " + e.getMessage();
            }
        }
        return reply;
    }

    private static JSONObject jolokiaRequest(String path) throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(JOLOKIA_URL + path))
                .header("Authorization", "Basic " + Base64.getEncoder()
                        .encodeToString((USERNAME + ":" + PASSWORD).getBytes()))
                .header("Origin", ORIGIN)
                .header("Accept", "application/json")
                .GET()
                .build();

        HttpResponse<InputStream> response = null;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (ConnectException e) {
            uCommons.uSendMessage("QueueScanner ERROR - check jolokia.url and brokerName !!");
            throw new RuntimeException("HTTP " + e.getLocalizedMessage());
        }

        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP " + response.statusCode() + ": " + new String(response.body().readAllBytes()));
        }

        client = null;
        request= null;

        String json = new String(response.body().readAllBytes(), StandardCharsets.UTF_8);
        return new JSONObject(json);
    }

    private static String extractQueueName(String mbean) {
        for (String token : mbean.split(",")) {
            if (token.startsWith("destinationName=")) {
                return token.split("=")[1];
            }
        }
        return null;
    }

    private HttpServerExchange SetAndSend(HttpServerExchange exchange, int s, String reply) {
        exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), "application/json");
        exchange.getResponseHeaders().put((Headers.STATUS), s);
        exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
        exchange.getRequestHeaders().put((Headers.LOCATION), bindAddess);
        exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Origin"), "*");
        exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Methods"), "GET, POST, OPTIONS");
        exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Headers"), "Content-Type");
        exchange.setStatusCode(s);
        exchange.getResponseSender().send(reply);
        if (!getPing) lastActive = System.nanoTime();
        return exchange;
    }

    private void LogEvent(String s) { uCommons.uSendMessage(s); }

    private void LogEventHandler() {
        rightNow = System.nanoTime();
        laps = (rightNow - lastActive) / div;
        event = (event+spcs).substring(0,50)+" Response:  (" + reply.length() + ") bytes.";
        event+= "  (" + ((int) laps) + ") seconds";
        LogEvent(event);
        event = "";
    }

    public static void main(final String[] args) throws Exception {
        for (int i=0 ; i<3; i++) {System.out.println(" "); }
        uCommons.uSendMessage("-----------------------------------------------------------------");
        String opsys = System.getProperty("os.name");
        if (opsys.toLowerCase().contains("windows")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
            String slash = "/";
            NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
            isDev = true;
        }
        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);

        Properties sProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/this.server");
        String broker = sProps.getProperty("brokers", "");
        Properties bkrProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/"+broker);
        uCommons.BkrCommons(bkrProps);
        searchPath = "/search/org.apache.activemq:brokerName="+NamedCommon.bkrName+",destinationType=Queue,destinationName=*,type=Broker";
        JOLOKIA_URL = NamedCommon.jolokiaURL;
        USERNAME    = NamedCommon.bkr_user;
        PASSWORD    = NamedCommon.bkr_pword;
        ORIGIN      = NamedCommon.jolokiaORIGIN;

        //   Receive HTTPS (POST) requests.
        //   Sends the body to JMS queue !!
        //   Wait for the response at a predefined queue
        //   Return the response to the requestor.

        new DashBoard();
    }

}
