package com.unilibre.dataworks;


import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import com.unilibre.core.coreCommons;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

public class QueueScanner {

    private static String JOLOKIA_URL;
    private static String USERNAME;
    private static String PASSWORD;
    private static String ORIGIN;
    private static String WATCHq;
    private static boolean debugMode=false;
    private static final String heartbeat = "<<heartbeat>> ... QueueScanner()";

    public static void main(String[] args) throws Exception {
        for (int i=0 ; i<3; i++) { System.out.println(" "); }
        String opsys = System.getProperty("os.name");
        if (opsys.toLowerCase().contains("windows")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
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

        JOLOKIA_URL = NamedCommon.jolokiaURL;
        USERNAME    = NamedCommon.bkr_user;
        PASSWORD    = NamedCommon.bkr_pword;
        ORIGIN      = NamedCommon.jolokiaORIGIN;
        WATCHq      = NamedCommon.queueWATCH;

        String searchPath = "/search/org.apache.activemq:brokerName="+NamedCommon.bkrName+",destinationType=Queue,destinationName=*,type=Broker";

        List<String> Qnames;
        List<Integer> Qconsumers;
        List<Integer> Qsize;

        for (int i=0 ; i<3; i++) { System.out.println(" "); }
        uCommons.uSendMessage("Starting Scanner on queues like "+WATCHq+"... ");
        uCommons.uSendMessage("Broker  "+NamedCommon.bkrName);
        uCommons.uSendMessage("URL     "+JOLOKIA_URL);
        uCommons.uSendMessage("ORIGIN  "+ORIGIN);
        uCommons.uSendMessage("---------------------------------------------------------------------------");
        System.out.println(" ");
        uCommons.uSendMessage("Pausing 2 minutes for Dispatcher to start");
        uCommons.Sleep(120);
        uCommons.uSendMessage("Starting ...");
        System.out.println(" ");

        int hbCnt=0;        // heartbeat counter
        while (!coreCommons.StopNow()) {
            Qnames = new ArrayList<>();
            Qconsumers = new ArrayList<>();
            Qsize = new ArrayList<>();
            // Step 1: Search all queue MBeans
            JSONObject searchResponse = jolokiaRequest(searchPath);
            JSONArray mbeans = searchResponse.getJSONArray("value");

            int qcount=0;       // how many consumers
            int qsize=0;        // how many messages
            String qname;       // name of the queue
            String tableName;

            for (int i = 0; i < mbeans.length(); i++) {
                String mbean = mbeans.getString(i);
                qname = extractQueueName(mbean);
                qcount=0;
                qsize=0;
                if (qname != null && qname.startsWith(WATCHq)) {
                    // Step 2: Query this queue's stats
                    String readPath = "/read/" + URLEncoder.encode(mbean, "UTF-8");
                    JSONObject queueData = jolokiaRequest(readPath);
                    JSONObject value = queueData.getJSONObject("value");
                    qcount = value.getInt("ConsumerCount");
                    qsize = value.getInt("QueueSize");
                    Qnames.add(qname);
                    Qconsumers.add(qcount);
                    Qsize.add(qsize);
                }
            }

            int lx = Qnames.size(), ratio;
            boolean action;
            for (int x=0 ; x < lx ; x++) {
                action=false;
                qname = Qnames.get(x);
                qcount= Qconsumers.get(x);
                qsize = Qsize.get(x);
                if (debugMode) {
                    uCommons.uSendMessage("Queue: " + qname);
                    uCommons.uSendMessage("     : # messages  " + qsize);
                    uCommons.uSendMessage("     : # consumers " + qcount);
                }
                if (qsize > 0) {
                    if (qcount == 0) {
                        if (debugMode) uCommons.uSendMessage("     : # action  - restarting the master");
                        tableName = qname.substring(qname.indexOf("["), qname.length());
                        InsertDispatcher.SetRole("master");
                        InsertDispatcher.startWorkerForTable(tableName);
                        if (debugMode) uCommons.uSendMessage("     : # action  - done.");
                        action=true;
                    }
                    if (qcount == 0) {
                        ratio = qsize;
                    } else {
                        if (qsize == 0) {
                            ratio = qsize;
                        } else {
                            ratio = qsize / qcount;     // # messages / # consumers
                            if (qcount >= NamedCommon.maxSmartCons) ratio=1;   // Set a maximum of 10 workers
                        }
                    }
                    if (ratio > 20) {
                        if (debugMode) uCommons.uSendMessage("     : # action  - adding a slave process");
                        tableName = qname.substring(qname.indexOf("["), qname.length());
                        InsertDispatcher.SetRole("worker");
                        InsertDispatcher.startWorkerForTable(tableName);
                        if (debugMode) uCommons.uSendMessage("     : # action  - done.");
                        action=true;
                    }
                }
                if (action) {
                    hbCnt = 0;
                    continue;
                }
                if (debugMode) uCommons.uSendMessage("     : # action  - no action required.");
            }
            Qnames.clear();
            Qconsumers.clear();

            // check and re-check every 10 seconds
            uCommons.Sleep(10);
            rProps = uCommons.LoadProperties("rFuel.properties");
            if (NamedCommon.ZERROR) System.exit(0);
            uCommons.SetCommons(rProps);
            hbCnt++;
            if (hbCnt > 120) {
                // 1 hour of no activity
                uCommons.uSendMessage(heartbeat);
                hbCnt=0;
            }
        }
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

}
