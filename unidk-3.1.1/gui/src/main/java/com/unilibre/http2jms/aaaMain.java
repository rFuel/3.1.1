package com.unilibre.http2jms;



import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class aaaMain {
    private String bkrUrl = "tcp://192.168.48.107:61616";
    private String consClientID = "consAndy";
    private String prodClientID = "prodAndy";
    private String tm="<tm>", is="<is>";
    private String sQue = "Consumer.4.VirtualTopic.rFuel";
    private String cQue = "059_H2J_Responses";
    private JMSConsumer jmsc = null;
    private JMSProducer jmsp = null;

    private aaaMain() {
        String host = "localhost";
        int servePort = 8188;
        int IOthreads = 10;
        int workerThreads = IOthreads * 10;
        Undertow myServer = Undertow.builder()
                .setIoThreads(IOthreads)
                .setWorkerThreads(workerThreads)
                .addHttpListener(servePort, host)
                .setHandler(new HttpHandler() {
                    private String reply = "";
                    private String status = "200";
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {
                        // Authenticate the request --------------------
                        // Validate the request ------------------------
                        // Process the request
                        try {
                            exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                                        String req = new String(data);
                                        reply = RequestHandler(req);
                                        req = null;
                                    },

                                    (body, exception) -> {
                                        reply = ErrorHandler(exception.toString());
                                    });
                        } catch (Exception e) {
//                            System.out.println("mERROR: " + e.getMessage());
                        }

                        String errMessage = null;
                        if (reply == null) {
                            errMessage = "Internal Server Error.";
                        } else if (reply.equals("")) {
                            System.out.println("rFuel stop may be ON - check now!");
                            errMessage = "Cannot obtain the body";
                        }
                        if (errMessage != null) {
                            if (status.equals("200")) status = "500";
                            reply = errMessage;
                        }

                        if (!reply.startsWith("{")) {
                            String descr = "OK";
                            reply = ResponseHandler(status, descr, reply);
                        }

                        exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), "application/json");
                        exchange.getResponseHeaders().put((Headers.STATUS), status);
                        exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
                        exchange.getResponseHeaders().put((Headers.LOCATION), "http://"+host+":"+servePort);
                        exchange.setStatusCode(Integer.parseInt(status));
                        exchange.getResponseSender().send(reply);
                        System.out.println(Thread.currentThread().getName()+ "  ==>  " + reply);
                        errMessage = null;
                    }
                })
                .build();
        myServer.start();
        System.out.println("Accepting requests on http://localhost:8188");
    }

    private String RequestHandler(String request) {
        String tName = Thread.currentThread().getName();
        System.out.println(tName + " Received a request");
        String answer = "";

        Map<String, String> reqMap = ReqLoader(request);
        String rVal, message;
        StringBuilder sb = new StringBuilder();
        for (String rKey: reqMap.keySet()) {
            rVal = reqMap.get(rKey);
            sb.append(rKey + is + rVal + tm);
            rKey = null;
        }
        message = sb.toString();
        sb = null;

        String filter  = reqMap.getOrDefault("correlationid".toUpperCase(), "No correlationID");

        answer = "jms2jsmOK";
        if (!answer.equals("")) return answer;

        this.jmsc = new JMSConsumer();
        this.jmsp = new JMSProducer();

        // -----------------------------------------------------

        jmsc.Prepare(filter, bkrUrl, consClientID, "admin", "admin", tName);
        jmsp.PrepareConnector(prodClientID, bkrUrl, sQue, "admin", "admin", tName);
        jmsp.send(message, filter);

//        answer = this.jmsc.consume(cQue);
        answer = "jms2jsmOK";
        // -----------------------------------------------------
        jmsc.shutdown();
        jmsp.shutdown();
        // -----------------------------------------------------
        jmsc = null;
        jmsp = null;
        return answer;
    }

    public Map ReqLoader(String request) {
        Map<String, String> msgMap = new HashMap<>();
        JSONObject obj = null;
        obj = new JSONObject(request);
        Iterator<String> jKeys;
        jKeys = obj.getJSONObject("request").keys();
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = obj.getJSONObject("request").get(zkey).toString();
            if (zkey.toUpperCase().equals("REPLYTO")) zval = cQue;
            if (zkey.toUpperCase().equals("CORRELATIONID")) { zval += "_" + Thread.currentThread().getName(); }
            msgMap.put(zkey.toUpperCase(),zval);
        }
        return msgMap;
    }

    private String ErrorHandler(String request) {
        return "Error World";
    }

    public static String ResponseHandler(String status, String descr, String response) {
        String replyMessage = response;
        if (response == null || !replyMessage.startsWith("{\"body\":")) {
            String responseStr = response == null ? "NULL" : response;
            replyMessage = "{\"body\": {\"status\": \"" + status + "\",\"message\": \"" + descr + "\",\"response\": \"" + responseStr + "\"}}";
        }
        return replyMessage;
    }

    public static void main(String[] args) throws Exception {
        new aaaMain();
    }

}

