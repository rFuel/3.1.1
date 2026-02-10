package com.unilibre.cdroem;

import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class cdrReceiver {

    private ArrayList<String> reqKeys = new ArrayList<>();
    private ArrayList<String> reqVals = new ArrayList<>();
    private String appjson = "application/json";
    public static String jHeader = "request";
    private nsCommonData comData;
    private static long startM = 0, finishM = 0;
    private static double laps, div = 1000000000.00;

    public cdrReceiver(int iOthreads, int workerThreads, SSLContext sslContext, String siteSecret, String chkPath) {
        if (comData != null) comData = null;
        comData = new nsCommonData();
        if (comData.ZERROR) {
            commons.ZERROR = true;
            return;
        }

        Undertow server = Undertow.builder()
                .setIoThreads(iOthreads)
                .setWorkerThreads(workerThreads)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpsListener(cdrCommons.getPort(), cdrCommons.getHost(), sslContext)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {

                        startM = System.nanoTime();
                        comData.uSendMessage("ReceivedRequest() *************************************************");

                        String status = "200";
                        final String[] reply = {""};
                        String inHost = exchange.getHostName();
                        String inPort = String.valueOf(exchange.getHostPort());
                        String inPath = exchange.getRelativePath();
                        String thisHDR= "https://" + inHost + ":" + inPort + inPath;

                        // Authenticate the request --------------------
                        if (!siteSecret.equals("")) {
                            HeaderMap headerMap = exchange.getRequestHeaders();
                            String secret="";
                            boolean isThere = headerMap.contains("X-rfuel-api");
                            boolean isErr = true;
                            if (isThere) {
                                isErr = false;
                                secret = headerMap.get("X-rfuel-api").toString();
                                if (secret == null) secret = "";
                                secret = secret.replace("[", "");
                                secret = secret.replace("]", "");
                                if (!secret.equals(siteSecret)) isErr = true;
                            }
                            headerMap = null;
                            secret = "";
                            if (isErr) {
                                status = "403";
                                secret = "Access Denied";  // Do not provide details or reasons.
                                reply[0] = commons.ResponseHandler(status, "Forbidden", secret, "JSON");
                            }
                        }

                        if (inPath.equals(chkPath) && reply[0].equals("")) {

                            if (!exchange.getResponseHeaders().contains(thisHDR)) exchange.getResponseHeaders().add(Headers.LOCATION, thisHDR);
                            // -----------------------------------------------------------------------------
                            try {
                                exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                                            String req = new String(data);
                                            comData.uSendMessage("ProcessRequest()");
                                            try {
                                                reply[0] = ProcessRequest(req);
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        },

                                        (body, exception) -> {
                                            comData.uSendMessage(" ");
                                            comData.uSendMessage("An exception has occurred: " + exception.toString());
                                            comData.uSendMessage(" ");
                                            reply[0] = ErrorHandler(exception.toString());
                                        });
                            } catch (Exception e) {
                                comData.uSendMessage("ERROR: " + e.getMessage());
                            }
                            // -----------------------------------------------------------------------------
                        }
                        finishM = System.nanoTime();
                        laps = (finishM - startM) / div;
                        if (reply[0] == null) {
                            commons.ZERROR = true;
                            if (commons.Zmessage.equals("") || commons.Zmessage == null) commons.Zmessage = "Internal Server Error.";
                            commons.uSendMessage(commons.Zmessage);
                        }

                        if (reply[0].equals("")) {
                            commons.ZERROR = true;
                            if (commons.Zmessage.equals("") || commons.Zmessage == null) commons.Zmessage = "Cannot obtain the body";
                            commons.uSendMessage(commons.Zmessage);
                        }

                        if (!inPath.equals(chkPath)) {
                            commons.ZERROR = true;
                            commons.Zmessage = "Request sent to wrong URL path:";
                            commons.uSendMessage(commons.Zmessage);
                            commons.uSendMessage("Properties are set to check [" + chkPath + "] you sent it to [" + inPath + "]");
                        }

                        if (commons.ZERROR || comData.ZERROR) {
                            if (status.equals("200")) status = "500";
                            if (comData.ZERROR) commons.Zmessage = comData.Zmessage;
                            reply[0] = commons.ResponseHandler(status, "", commons.ScrubText(commons.Zmessage), "JSON");
                        }

                        commons.uSendMessage("Response: "+ reply[0].length() +" bytes returned response");
                        exchange = SetAndSend(exchange, status, reply[0]);
                    }
                })
                .build();
        server.start();
    }

    private HttpServerExchange SetAndSend(HttpServerExchange exchange, String status, String reply) {
        if (reply == null) comData.uSendMessage("Response is empty.");
        exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), appjson);
        exchange.getResponseHeaders().put((Headers.STATUS), status);
        exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
        exchange.getResponseHeaders().put((Headers.LOCATION), cdrCommons.getAddress());
        exchange.setStatusCode(Integer.valueOf(status));
        exchange.getResponseSender().send(reply);
        comData.uSendMessage("ReturnedResponse() ------------------------------------------------");
        comData.uSendMessage("Processed in " + laps + " seconds");
        System.out.println(" ");
        return exchange;
    }

    private String ProcessRequest(String request) throws IOException {
        commons.ZERROR = false;
        commons.Zmessage="";
        comData = null;
        comData = new nsCommonData();
        comData.nsReqLoader(request);

        if (!comData.nsMsgGet("bsb").equals("")) {
            String bsbConf = comData.BaseCamp + comData.slash  + comData.nsMsgGet("bsb");
            Properties msgProps = commons.LoadProperties(bsbConf);
            if (commons.ZERROR) return "";
            commons.pxHost = msgProps.getProperty("pxIP", commons.pxHost);
            commons.pxDbase = msgProps.getProperty("pxDB", commons.pxDbase);
            commons.pxDbuser = msgProps.getProperty("pxUP", commons.pxDbuser);
            commons.pxDbacct = msgProps.getProperty("pxAC", commons.pxDbacct);
        }
        comData.host = commons.pxHost;
        comData.dbase = commons.pxDbase;
        comData.dbuser = commons.pxDbuser;
        comData.dbacct = commons.pxDbacct;
        if (comData.ZERROR) return "";

        String usage = comData.usage;
        String hold01=usage;
        String mUse = comData.nsMsgGet("usage").toLowerCase();

        if (!mUse.equals("")) usage = mUse;

        String answer = "";

        if (!comData.DBconnected) comData.ConnectProxy();

        switch (usage) {
            case "cdr":
                answer = cdrCommons.HandleResponse(comData, request);
                break;
            case "loadbp":
                answer = cdrCommons.LoadBP(comData, request);
                break;
            default: ;
                answer  = commons.ResponseHandler("400", "Bad Request", "Unknown Http2 usage property", "JSON");
                System.out.println("Unknown Http2 usage property");
                break;
        }
        usage = hold01;
        if (comData.DBconnected) comData.DisconnectProxy();
        return answer;
    }

    private String ErrorHandler (String error) {
        commons.uSendMessage("ERROR: " + error);
        String answer = "";
        answer = commons.ResponseHandler("400", "Bad Request", error, "JSON");
        return answer;
    }

}
