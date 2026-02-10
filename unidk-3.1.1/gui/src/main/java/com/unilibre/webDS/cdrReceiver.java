package com.unilibre.webDS;

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;

import javax.net.ssl.SSLContext;
import java.io.IOException;

public class cdrReceiver {

    private String appjson = "application/json";
    public static String jHeader = "request";

    public cdrReceiver(int iOthreads, int workerThreads, SSLContext sslContext, String siteSecret, String chkPath) {

        Undertow server = Undertow.builder()
                .setIoThreads(iOthreads)
                .setWorkerThreads(workerThreads)
//                .addHttpsListener(cdrCommons.getPort(), cdrCommons.getHost(), sslContext)
                .addHttpListener(cdrCommons.getPort(), cdrCommons.getHost())
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {
                        nsCommonData comData = new nsCommonData();

                        logger.uSendMessage("ReceivedRequest() *************************************************");

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
                                reply[0] = commons.ResponseHandler(status, "Forbidden", secret);
                            }
                        }

                        if (inPath.equals(chkPath) && reply[0].equals("")) {

                            if (!exchange.getResponseHeaders().contains(thisHDR)) exchange.getResponseHeaders().add(Headers.LOCATION, thisHDR);
                            // -----------------------------------------------------------------------------
                            try {
                                exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                                            String req = new String(data);
                                            logger.uSendMessage("ProcessRequest()");
                                            try {
                                                reply[0] = ProcessRequest(comData, req);
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        },

                                        (body, exception) -> {
                                            logger.uSendMessage(" ");
                                            logger.uSendMessage("An exception has occurred: " + exception.toString());
                                            logger.uSendMessage(" ");
                                            reply[0] = ErrorHandler(exception.toString());
                                        });
                            } catch (Exception e) {
                                logger.uSendMessage("ERROR: " + e.getMessage());
                                e.printStackTrace();
                            }
                            // -----------------------------------------------------------------------------
                        }

                        String errMessage = null;
                        if (reply[0] == null) {
                            errMessage = "Internal Server Error.";
                        } else if (reply[0].equals("")) {
                            logger.uSendMessage("rFuel stop may be ON - check now!");
                            errMessage = "Cannot obtain the body";
                        }

                        if (errMessage != null) {
                            if (status.equals("200")) status = "500";
                            reply[0] = commons.ResponseHandler(status, "Internal Server Error",errMessage);
                        }

                        logger.uSendMessage("Response: "+ reply[0].length() +" bytes returned response");
                        exchange = SetAndSend(exchange, comData, status, reply[0]);

                    }
                })
                .build();
        server.start();
    }

    private HttpServerExchange SetAndSend(HttpServerExchange exchange, nsCommonData comData, String status, String reply) {
        if (reply == null) logger.uSendMessage("Response is empty.");
        exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), appjson);
        exchange.getResponseHeaders().put((Headers.STATUS), status);
        exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
        exchange.getResponseHeaders().put((Headers.LOCATION), cdrCommons.getAddress());
        exchange.setStatusCode(Integer.parseInt(status));
        exchange.getResponseSender().send(reply);
        logger.uSendMessage("ReturnedResponse() ------------------------------------------------");
        System.out.println(" ");
        return exchange;
    }

    private String ProcessRequest(nsCommonData comData, String request) throws IOException {
        comData.nsReqLoader(request);

        String usage = comData.usage;
        String hold01=usage;
        String mUse = comData.nsMsgGet("usage").toLowerCase();
        if (!mUse.equals("")) usage = mUse;

        String answer = "";

        try {
            switch (usage) {
                case "cdr":
                    answer = cdrCommons.HandleResponse(comData, request);
                    break;
                default:
                    ;
                    answer = commons.ResponseHandler("400", "Bad Request", "Unknown Http2 usage property");
                    System.out.println("Unknown Http2 usage property");
                    break;
            }
            usage = hold01;
        } finally {
            try {
                comData.DisconnectProxy();
            } catch (Exception e) {
                System.err.println("Error closing Proxy connection after call. Ignoring this error");
                e.printStackTrace();
            }
        }

        return answer;
    }

    private String ErrorHandler (String error) {
        logger.uSendMessage("ERROR: " + error);
        String answer = "";
        answer = commons.ResponseHandler("400", "Bad Request", error);
        return answer;
    }

}
