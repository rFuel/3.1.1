package com.unilibre.gui;


import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.logging.Level;

public class GuiReceiver {

    public ArrayList<String> httpCodes;
    private static ArrayList<String> ReturnCodes;
    private final char[] TEST_PASSWORD = "passw0rd".toCharArray();
    private boolean stopNow=false;
    private String status;
    private String inHost;
    private String inPort;
    private String inPath;
    private String thisHDR;
    private String reply;
    private int IOthreads = 1, workerThreads = 10;
    private long startM, finishM, lastActive, rightNow;
    private double laps, div = 1000000000.00;

    private guiMethods gm;
    private cdrCommons com;

    public static void main(final String[] args) throws Exception {
        BuildCodes();
        new GuiReceiver();
    }

    private static void BuildCodes() {
        guiMethods tempGM = new guiMethods();
        ReturnCodes = new ArrayList<>();
        for (int rc = 0; rc < 1100; rc++) { ReturnCodes.add(""); }
        int nbrvals;
        String pwd = System.getProperty("user.dir");
        String hCodes = tempGM.ReadDiskRecord(pwd+"/rfuel/http-return-codes.csv");
        if (hCodes.equals("<<ERROR>>")) {
            System.out.println("Please load "+pwd+"/rfuel/http-return-codes.csv ");
            System.exit(0);
        }
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
                // ## BUGFIX ##
                tmp1 = null;
            }
            // ## BUGFIX ##
            tmp = null;
        }
    }

    private GuiReceiver() throws Exception {

        this.gm = new guiMethods();
        this.com= new cdrCommons();
        this.httpCodes = ReturnCodes;

        SecureProcess();            // set up an https listener then come back      //
        WaitForRequests();          // loop infinitely to watch for idle time       //
    }

    private void Sleep(int sec) {
        sec = sec * 1000;
        if (sec ==0) sec = 500;
        try {
            Thread.sleep(sec);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    private void WaitForRequests() {

        gm.uSendMessage(" ");
        gm.uSendMessage("[ Serving Requests ] --------------------------------------------");
        gm.uSendMessage(" ");
        gm.uSendMessage("Accepting requests on " + com.getAddress());
        gm.uSendMessage(" ");
        gm.uSendMessage(" ");

        this.lastActive = System.nanoTime();
        int idleSeconds;

        while (!stopNow) {
            this.rightNow = System.nanoTime();
            this.laps = (this.rightNow - this.lastActive) / this.div;
            idleSeconds = ((int) this.laps);
//            if (idleSeconds > NamedCommon.mqHeartBeat) {
//                uSendMessage("WaitForRequests() *********************************************************");
//                lastActive = System.nanoTime();
//            } else {
                Sleep(0);
//            }
        }
    }

    private void SecureProcess() throws Exception {

        gm.uSendMessage(" ");
        gm.uSendMessage("[ Initialising ] ------------------------------------------------");

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.debug(String.valueOf(Level.OFF));
        root.info(String.valueOf(Level.OFF));
        root.warn(String.valueOf(Level.OFF));
        this.lastActive = System.nanoTime();

        int servePort;
        try {
            servePort = Integer.valueOf(gm.GetValue("urlport", "8188"));
        } catch (NumberFormatException e) {
            gm.uSendMessage("ERROR in urlport setting - not an integer. Default to 8188");
            servePort = 8188;
        }

        com.setSecure(gm.GetValue("secure", "").toLowerCase().equals("true"));
        com.setURLhost(gm.GetValue("urlhost", "localhost"));
        com.setURLpath(gm.GetValue("urlpath", "/rfuel"));
        com.setURLport(servePort);

        com.Initialise();

        gm.uSendMessage(" ");
        gm.uSendMessage("[ Set Security Features ON ] ------------------------------------");
        KeyStore ks = gm.loadKeyStore("server.keystore");
        KeyStore ts = gm.loadKeyStore("server.truststore");
        SSLContext sslContext = gm.CreateContext(ks, ts);

        String chkPath = com.getPath();

        Undertow server = Undertow.builder()
                .setIoThreads(IOthreads)
                .setWorkerThreads(workerThreads)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpsListener(com.getPort(), com.getHost(), sslContext)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {

                        gm.uSendMessage("ReceivedRequest() *************************************************");

                        status = "200";
                        reply = "";
                        inHost = exchange.getHostName();
                        inPort = String.valueOf(exchange.getHostPort());
                        inPath = exchange.getRelativePath();
                        thisHDR= "https://" + inHost + ":" + inPort + inPath;

//                        HeaderMap headerMap = exchange.getRequestHeaders();

                        if (inPath.equals(chkPath) && reply.equals("")) {
                            if (!exchange.getResponseHeaders().contains(thisHDR)) exchange.getResponseHeaders().add(Headers.LOCATION, thisHDR);
                            // -----------------------------------------------------------------------------
                            try {
                                HttpServerExchange finalExchange = exchange;
                                exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                                            String req = new String(data);
                                            gm.uSendMessage("ProcessRequest()");
                                            reply = ProcessRequest(finalExchange, req);
                                        },

                                        (body, exception) -> {
                                            gm.uSendMessage(" ");
                                            gm.uSendMessage("An exception has occurred: " + exception.toString());
                                            gm.uSendMessage(" ");
                                            reply = ErrorHandler(finalExchange, exception.toString());
                                        });
                            } catch (Exception e) {
                                gm.uSendMessage("ERROR: " + e.getMessage());
                            }
                            // -----------------------------------------------------------------------------
                        }

                        if (com.isZERROR()) {
                            gm.uSendMessage("ERROR: " + com.GetZmessage());
                            reply  = ResponseHandler("400", "Bad Request", com.GetZmessage(), "JSON");
                        }

                        if (reply.contains("WaitForReply():")) stopNow=true;

                        if (reply == null) {
                            com.SetError(true, "Internal Server Error.");
                        }

                        if (reply.equals("")) {
                            gm.uSendMessage("rFuel stop may be ON - check now!");
                            com.SetError(true, "Cannot obtain the body.");
                        }

                        if (com.isZERROR()) {
                            gm.uSendMessage("ERROR: " + com.GetZmessage());
                            reply  = ResponseHandler("400", "Bad Request", com.GetZmessage(), "JSON");
                        }

                        laps = (finishM - startM) / div;
                        gm.uSendMessage("Response: "+ reply.length() +" bytes returned in "+laps+" seconds");
                        exchange = SetAndSend(exchange, status, reply);
                        lastActive = System.nanoTime();
                        com.SetError(false, "");

                        if (stopNow) System.exit(0);

                    }
                })
                .build();
        server.start();
    }

    private String ProcessRequest(HttpServerExchange exchange, String request) {

        String answer = "";

        gm.uSendMessage("Got " + request);
        answer =  gm.BuildJsonReply("200", "OK", "");
        if (!answer.equals("")) return answer;

        gm.getHeaders(com, exchange.getRequestHeaders());
        String method = exchange.getRequestMethod().toString();

        if (method.equals("GET")) {
            answer = gm.HandleRequest(gm, com, "GET", exchange, request);
        } else if (method.equals("POST")) {
            answer = gm.HandleRequest(gm, com, "POST", exchange, request);
        } else if (method.equals("OPTIONS")) {
            answer = gm.BuildJsonReply("400", method + " requests are dis-allowed.", "");
        } else if (method.equals("PUT")) {
            answer = gm.BuildJsonReply("400", method + " requests are dis-allowed.", "");
        } else if (method.equals("DELETE")) {
            answer = gm.BuildJsonReply("400", method + " requests are dis-allowed.", "");
        } else {
            answer = gm.ErrorMsg("Unknown method call received");
            gm.uSendMessage("Unknown method call received");
        }

        return answer;
    }

    private String ErrorHandler(HttpServerExchange finalExchange, String error) {
        gm.uSendMessage("ERROR: " + error);
        String answer = "";
        answer = ResponseHandler("400", "Bad Request", error, "JSON");
        return answer;
    }

    public HttpServerExchange SetAndSend(HttpServerExchange exchange, String status, String reply) {
        if (reply != null) {
            exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), "application/json");
            exchange.getResponseHeaders().put((Headers.STATUS), status);
            exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
            exchange.getResponseHeaders().put((Headers.LOCATION), com.getAddress());
            exchange.setStatusCode(Integer.valueOf(status));
            exchange.getResponseSender().send(reply);

        } else {
            gm.uSendMessage("Got nothing to respond with");
        }
        gm.uSendMessage("ReturnedResponse() ------------------------------------------------");
        System.out.println(" ");
        return exchange;
    }

    public String ResponseHandler(String status, String descr, String response, String esbFMT) {
        String replyMessage = response;
        String stdDesc = this.httpCodes.get(Integer.valueOf(status));
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

}
