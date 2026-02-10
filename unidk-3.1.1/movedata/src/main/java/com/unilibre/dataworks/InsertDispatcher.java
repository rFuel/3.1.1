package com.unilibre.dataworks;

// This process dispatches messages from Dispatch queue to table queues.
// It is run from supervisord

import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.commons;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import com.unilibre.core.coreCommons;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class InsertDispatcher {

    private static final ArrayList<String> activeWorkers = new ArrayList<>();
    private static final ScheduledExecutorService monitorPool = Executors.newScheduledThreadPool(1);
    private static final String incorrid = "SQL-Worker-";
    private static final String toQue = "901.SQL.inserts.";
    private static final String fromQue = "900.SQL.Dispatcher";
    private static String role = "master";
    private static String insertWorker, classpath, curl;
    private static int idleCnt=0;

    public static void SetRole(String inval) {
        role = inval;
    }

    public static void Dispatch() throws Exception {
        boolean shown=false;
        String message, tableName;
        while (!coreCommons.StopNow()) {
            message = activeMQ.consume(NamedCommon.bkr_url, NamedCommon.bkr_user, NamedCommon.bkr_pword, "InsertDispatcher", fromQue);
            //
            // Defensive programming - ActiveMQ at Kiwibank on RHEL hosts is weird !!
            //
            while (commons.ZERROR) {
                uCommons.uSendMessage(commons.ERRmsg);
                uCommons.Sleep(2);
                message = activeMQ.consume(NamedCommon.bkr_url, NamedCommon.bkr_user, NamedCommon.bkr_pword, "InsertDispatcher", fromQue);
            }
            if (message.equals("")) {
                idleCnt++;
                if (idleCnt > 60) CheckQueues();
                idleCnt=0;
                continue;
            }
            JSONObject jMessage = new JSONObject(message);
            tableName = jMessage.getString("table");
            if (activeWorkers.indexOf(tableName) < 0) startWorkerForTable(tableName);
            jMessage = null;
            //
            // Redirect to the table queue for processing
            //
            activeMQ.produce(NamedCommon.bkr_url, NamedCommon.bkr_user, NamedCommon.bkr_pword, incorrid+tableName, toQue+tableName, message);
            activeMQ.Acknowledge();
            idleCnt=0;
        }
    }

    public static void startWorkerForTable(String tableName) throws IOException {
        //
        // This is fantastic at starting jobs and recording them in activeWorkers
        // BUT ... how do I remove them from activeWorkers when the master stops??
        //
        if (classpath == null) Initialise();
        System.out.println("Starting ["+role+"] for table: " + tableName);
        Path logFile = Paths.get(NamedCommon.BaseCamp +"/logs/901-SQL-inserts.log");
        ProcessBuilder pb = new ProcessBuilder("java", "-cp", classpath, insertWorker, tableName, role);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile.toFile()));
        pb.redirectErrorStream(true);               // Combines stdout + stderr
        pb.start();                                 // Fire and forget

        // yes, this has been checked above however, this is method is called from other classes
        if (activeWorkers.indexOf(tableName) < 0) activeWorkers.add(tableName);
    }

    private static void LegacyQueues() {

    }

    private static void CheckQueues() {
        String thisQ, cmd;
        int nbrQues = activeWorkers.size();
        for (int q=0 ; q < nbrQues ; q++) {
            thisQ = activeWorkers.get(q);
            uCommons.uSendMessage("Checking "+toQue+thisQ);
            thisQ = thisQ.replace("[", "\\[");
            thisQ = thisQ.replace("]", "\\]");
            cmd = curl + toQue + thisQ;
            String response = uCommons.nixExecCmd(cmd, 999);
            JSONObject root = new JSONObject(response);
            JSONObject value = root.getJSONObject("value");
            int queueSize = value.getInt("QueueSize");
            int consumerCount = value.getInt("ConsumerCount");

            if (queueSize == 0) {
                if (consumerCount == 0) {
                    activeWorkers.set(q,""); // force restart on next table message
                }
            }

            uCommons.uSendMessage("Messages waiting: " + queueSize);
            uCommons.uSendMessage("Consumer count  : " + consumerCount);
            System.out.println(" ");
        }
        int q=0;
        while (q< activeWorkers.size()) {
            if (activeWorkers.get(q).equals("")) {
                activeWorkers.remove(q);
                q--;
            }
            q++;
        }
        idleCnt=0;
    }

    private static void Initialise() {
        classpath = System.getProperty("java.class.path");
        insertWorker = "com.unilibre.dataworks.InsertWorker";

        String userpass = NamedCommon.bkr_user+":"+NamedCommon.bkr_pword;
        String baseUrl= NamedCommon.bkr_url;
        baseUrl = baseUrl.replace("tcp", "http");
        baseUrl = baseUrl.replace("ssl", "http");
        baseUrl = baseUrl.replace("61616", "8161");
        String brokerUrl= baseUrl + "/api/jolokia/read/";
        curl = "curl -u " + userpass + " -H \"Origin: http://localhost\" ";
        curl+= brokerUrl + "org.apache.activemq:type=Broker,brokerName=" + NamedCommon.bkrName;
        curl+= ",destinationType=Queue,destinationName=";
    }

    public static void main(String[] args) throws Exception {
        String opsys = System.getProperty("os.name");
        if (opsys.toLowerCase().contains("windows")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
            String slash = "/";
            NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
        }
        if (NamedCommon.BaseCamp.contains("/home/andy")) NamedCommon.BaseCamp = NamedCommon.DevCentre;

        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);

        Properties sProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/this.server");
        String broker = sProps.getProperty("brokers", "");
        Properties bkrProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/"+broker);
        uCommons.BkrCommons(bkrProps);
        NamedCommon.bkr_url = uCommons.FieldOf(NamedCommon.messageBrokerUrl, "\\?", 1);
        activeMQ.SetTransacted(true);
        Initialise();
        uCommons.uSendMessage("Dispatcher started ...");
        activeMQ.SetACK(false);

        Dispatch();
    }

}
