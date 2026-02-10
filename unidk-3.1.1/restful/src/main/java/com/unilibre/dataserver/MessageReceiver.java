package com.unilibre.dataserver;

import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.commons;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import com.unilibre.restful.KeystoreManager;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.jms.JMSException;
import javax.net.ssl.SSLContext;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageReceiver {

    private static String JOLOKIA_URL;
    private static String USERNAME;
    private static String PASSWORD;
    private static String ORIGIN;
    private static String WATCHq;
    private static String searchPath;
    private static List<String> mqTasks;
    private static List<String> mqNames;
    private static String bindAddess="";
    private static String httpHost="localhost";
    private static int    httpPort=8080, status=200;
    private static String httpPath="";
    private static String reply="";
    private static boolean secure=false;
    private static SSLContext sslContext=null;

    private static void WebServer() throws Exception {
        if (secure) {
            bindAddess = "https" + "://" + httpHost + ":" + httpPort + httpPath;
            HttpsServer();
        } else {
            bindAddess = "http" + "://" + httpHost + ":" + httpPort + httpPath;
            HttpServer();
        }

        System.out.println("Accepting requests on " + bindAddess);
        System.out.println("-----------------------------------------------------------------");
    }

    private static void HttpsServer() {
        System.out.println("--------------------- Create Undertow Server---------------------");
        System.out.println("Requesting service at " + bindAddess);

        Undertow server = Undertow.builder()
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpsListener(httpPort, httpHost, sslContext)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws JMSException {
                        ServeHttpRequests(exchange);
                        exchange.endExchange();
                        exchange = null;
                    };
                })
                .build();
        server.start();
    }

    private static void HttpServer() {
        System.out.println("--------------------- Create Undertow Server---------------------");
        System.out.println("Requesting service at " + bindAddess);

        Undertow server = Undertow.builder()
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpListener(httpPort, httpHost)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws JMSException {
                        ServeHttpRequests(exchange);
                        exchange.endExchange();
                        exchange = null;
                    };
                })
                .build();
        server.start();
    }

    private static void ServeHttpRequests(HttpServerExchange exchange) throws JMSException {

        System.out.println("");
        System.out.println("ReceivedRequest *************************************************");
        AtomicBoolean okay= new AtomicBoolean(false);
        while (!okay.get()) {
            reply = "";
            if (exchange.getRequestPath().contains("/jobstats")) {
                okay.set(ProcessRequest("#jobstats"));
            } else {
                exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                            String req = new String(data);
                            reply = "";
                            if (req.equals("")) {
                                status = 400;
                                reply = "\"Your request did not have a body.\"";
                            }
                            // can now build a whitelist & blacklist of requestors
                            if (!req.equals("")) {
                                System.out.println("ProcessRequest()");
                                status = 200;
                                okay.set(ProcessRequest(req));
                            }
                        },
                        (body, exception) -> {
                            reply = exception.toString();
                        }
                );
            }
        }
        System.out.println("Response: (" + Thread.currentThread().getName() + ") returned (" + reply.length() + ") bytes.");
        exchange = SetAndSend(exchange, status, reply);
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage="";
    }

    private static boolean ProcessRequest(String request){
        String answer = "";
        System.out.println("Request: " + request);
        boolean okay = false;
        if (request.contains("#jobstats")) {
            try {
                reply = GetJobStats();
            } catch (Exception e) {
                reply = e.getMessage();
                status = 501;
            }
        } else {
            okay = activeMQ.produce(NamedCommon.bkr_url, NamedCommon.bkr_user, NamedCommon.bkr_pword, "Message Fwd", "010_Start_001", request);
            if (!okay) {
                System.out.println(commons.ERRmsg);
                reply = commons.ERRmsg;
                status = 501;
            }
        }
        Runtime.getRuntime().gc();
        return true;
    }

    private static HttpServerExchange SetAndSend(HttpServerExchange exchange, int s, String reply) {
        exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), "application/json");
        exchange.getResponseHeaders().put((Headers.STATUS), s);
        exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
        exchange.getRequestHeaders().put((Headers.LOCATION), bindAddess);
        exchange.setStatusCode(s);
        exchange.getResponseSender().send(reply);
        return exchange;
    }

    private static String GetJobStats() throws Exception {
        List<String> Qnames = new ArrayList<>();
        List<Integer> Qsize = new ArrayList<>();
        String answer = "";

        // Step 1: Search all queue MBeans
        JSONObject searchResponse = jolokiaRequest(searchPath);
        JSONArray mbeans = searchResponse.getJSONArray("value");

        int idx=0;          // used for indexOf
        int qsize=0;        // how many messages
        String qname;       // name of the queue
        String qTask;       // Only report on Fetch, Burst, Flip combined stats

        for (int i = 0; i < mbeans.length(); i++) {
            String mbean = mbeans.getString(i);
            qname = extractQueueName(mbean);
            qsize=0;
            if (qname != null && qname.startsWith(WATCHq)) {
                // Step 2: Query this queue's stats
                String readPath = "/read/" + URLEncoder.encode(mbean, "UTF-8");
                JSONObject queueData = jolokiaRequest(readPath);
                JSONObject value = queueData.getJSONObject("value");
                qsize = value.getInt("QueueSize");
                qTask = qname.split("_")[0];
                if (Qnames == null) {
                    idx = -1;
                } else {
                    idx = Qnames.indexOf(qTask);
                }
                if (idx >= 0) {
                    qsize += Qsize.get(idx);
                    Qsize.set(idx, qsize);
                } else {
                    Qnames.add(qTask);
                    Qsize.add(qsize);
                }
            }
        }

        JSONObject obj = new JSONObject();
        int tot=0;
        String QN, QV;
        for (int i=0 ; i< Qnames.size() ; i++) {
            idx = mqTasks.indexOf(Qnames.get(i));
            if (idx < 0) {
                QN = Qnames.get(i);
            } else {
                QN = mqNames.get(idx);
            }
            tot+= Qsize.get(i);
            obj.put(QN, Qsize.get(i));
        }
        obj.put("Total", tot);
        Qnames.clear();
        Qsize.clear();
        answer = obj.toString();
        obj = null;
        return answer;
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

    public static void main(String[] args) throws Exception {
        String opsys = System.getProperty("os.name");
        if (opsys.toLowerCase().contains("windows")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp to " + knw);
            String slash = "/";
            NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
        }
        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);

        Properties sProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/this.server");
        String broker = sProps.getProperty("brokers", "");
        Properties bkrProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/"+broker);
        uCommons.BkrCommons(bkrProps);
        NamedCommon.bkr_url = uCommons.FieldOf(NamedCommon.messageBrokerUrl, "\\?", 1);

        mqTasks = new ArrayList<>(Arrays.asList(bkrProps.getProperty("tasks").split("\\,")));
        mqNames = new ArrayList<>(Arrays.asList(bkrProps.getProperty("qname").split("\\,")));

        activeMQ.SetTransacted(true);

        JOLOKIA_URL = NamedCommon.jolokiaURL;
        USERNAME    = NamedCommon.bkr_user;
        PASSWORD    = NamedCommon.bkr_pword;
        ORIGIN      = NamedCommon.jolokiaORIGIN;
        WATCHq      = "01";
        searchPath = "/search/org.apache.activemq:brokerName="+NamedCommon.bkrName+",destinationType=Queue,destinationName=*,type=Broker";

        sProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/receiver.properties");
        // receiver.properties
        // host=        {mandatory}
        // port=        {mandatory}
        httpHost = sProps.getProperty("host", httpHost);
        secure = sProps.getProperty("secure", "false").toLowerCase().equals("true");
        try {
            httpPort = Integer.valueOf(sProps.getProperty("port"));
        } catch (NumberFormatException nfe) {
            System.out.println("port setting MUST be an integer! Using "+httpPort);
        }

        // The ONLY way to set "secure" is to have a tls cert imported
        // into the "UniLibre" keystore -> KeyStoreManager !!!!
        String keyName = "rfuel-msg-receiver";
        uCommons.uSendMessage("[INFO] Load Keystore ("+keyName+")");
        KeystoreManager km = new KeystoreManager(keyName);
        uCommons.uSendMessage("[INFO] Check for Certificate");
        secure = km.KeyStoreIsReady();
        if (secure) {
            uCommons.uSendMessage("[INFO] Get SSL Context");
            sslContext = km.GetSSLcontext();
        }
        uCommons.uSendMessage("[INFO] Keystore ("+keyName+") completed");
        WebServer();
    }
}
