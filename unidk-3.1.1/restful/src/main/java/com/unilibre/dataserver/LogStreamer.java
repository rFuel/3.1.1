package com.unilibre.dataserver;


import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogStreamer {

    private String logName, BaseCamp;
    private HttpServerExchange exchange;
    public boolean disconnected=false;

    public void setLogName(String name) {this.logName = name;}
    public void setExchange(HttpServerExchange inval) {exchange = inval;}
    public void setBaseCamp(String inval) {BaseCamp = inval;}
    public void setDisconnected(boolean inval) {disconnected = inval;}

    public boolean getDisconnected() {return disconnected;}

    public void StartStreamer() {

        // only the worker thread gets into this code.
        exchange.startBlocking();
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/event-stream");
        exchange.getResponseHeaders().put(new HttpString("Access-Control-Allow-Origin"), "*");
        exchange.setPersistent(true);

        Path logFile = Paths.get(BaseCamp + "/logs/", logName);

        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(logFile.toFile(), "r");
        } catch (FileNotFoundException e) {
            System.out.println("Cannot access logFile "+logFile.toAbsolutePath());
            System.out.println(e.getMessage());
            return;
        }
        try (
                OutputStream out = exchange.getOutputStream()
        ) {
            file.seek(Math.max(0, file.length() - 500));  // in case the log is <= 250 bytes
            AtomicBoolean clientDisconnected = new AtomicBoolean(false);
            try {
                exchange.addExchangeCompleteListener((ex, next) -> {
                    clientDisconnected.set(true);
                    next.proceed();
                });
            } catch (Exception e) {
                System.out.println("addExchangeCompleteListener ERROR "+e.getMessage());
                return;
            }
            String sse;
            long now, prev=file.getFilePointer();
            while (!clientDisconnected.get()) {
                String line = file.readLine();
                now = file.getFilePointer();
                if (line != null) {
                    sse = "data: " + line + "\n\n";
                    OutputLine(sse, out);
                } else {
                    file.close();
                    Thread.sleep(2500); // poll pause
                    logFile = Paths.get(BaseCamp + "/logs/", logName);
                    file = new RandomAccessFile(logFile.toFile(), "r");
                    file.seek(now);
                    if (now-prev > 50) {
                        sse = "data: refresh the log handle. Picking up from "+now+"\n\n";
                        OutputLine(sse, out);
                        prev = now;
                    }
                }
            }
            setDisconnected(true);
            exchange.endExchange();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void OutputLine(String sse, OutputStream out) {
        try {
            out.write(sse.getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (IOException e) {
            System.out.println("Write to 'out' ERROR: "+e.getMessage());
        }
    }
}