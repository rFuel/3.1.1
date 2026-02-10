package com.unilibre.restful;

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SqlCommands;
import com.unilibre.commons.uCommons;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.*;
import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class cdcReceiver {

    private static final String SER_STRINGS = StringSerializer.class.getName();        // Serdes.String().getClass().getName();
    private static final String DES_STRINGS = StringDeserializer.class.getName();      // Serdes.String().getClass().getName();
    private static final String SER_B_ARRAY = BytesSerializer.class.getName();         // Serdes.ByteArray().getClass().getName();
    private static final String DES_B_ARRAY = BytesDeserializer.class.getName();       // Serdes.ByteArray().getClass().getName();
    private static final String RFUEL_API = "x-rfuel-api";
    private static final String RFUEL_KEY = "x-rfuel-key";
    private static final String SQB_OPEN = "[", SQB_CLOSE= "]", TILDE = "~";
    private static final int LINGER = 200, KBYTE=1024;
    private static final String TMARK="<tm>", IMark = "<im>", spc = " ", STAR="*";
    private static final String BASELOG = "{\"passport\":\"$passport\",\"sourceinstance\":" +
            "\"$source\",\"sourceaccount\":\"$account\",\"date\":\"$date\",\"time\":" +
            "\"$time\",\"item\":\"$item\",\"file\":\"$file\",\"record\":\"$record\"}";
    private static final String PASSPORT="$passport", SOURCE="$source", ACCOUNT="$account",
            DATE="$date", TIME="$time", ITEM="$item", FILE="$file", RECORD="$record",
            ULTRACS="ultracs", ERA="era", DCONV="D4-", TCONV="MTS", HYPHEN="-", TRUE="true",
            COLON=":";

    private static int IOthreads, workerThreads, inPort;
    private static String inHost, reply, status, inRequest, contentType, thisHDR, spacer="<im> ", coreApp="ultracs";
    private static String keystoreName = "server.keystore", sqlInsert, passport, issuer;
    private static String GROUP_ID_CONFIG, CLIENT_ID, runID, inPath;
    private static String brokers, valSerdes, keySerdes, keyType, valType, compression, reqKey, reqRec;
    private static String acks, saslAzure, saslAWS, retries, saslKey, saslUser, saslPass, topic_name;
    private static Properties configProperties;
    private static int kafkaBatchSize, maxblockms, epoch, runningTot=0, thisBatch=0, lx;
    private static boolean sasl, batching;
    private static SSLContext sslContext;
    private static ArrayList<String> keystoreNames, keystoreLocns, msgKeys, messages;
    private static Producer producer;

    private static void SecureProcess() {
        // ------------------- handle https traffic ------------------------
        SendMessage(STAR);
        SendMessage("[ Create HTTPS Endpoint ] ---------------------------------------");
        KeyStore ks;
        SSLContext sslContext;
        try {
            ks = loadKeyStore("server.keystore");
            KeyStore ts = loadKeyStore("server.truststore");
            sslContext = CreateContext(ks, ts);
        } catch (Exception e) {
            SendMessage(e.getMessage());
            return;
        }

        Undertow server = Undertow.builder()
                .setIoThreads(IOthreads)
                .setWorkerThreads(workerThreads)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
                .addHttpsListener(Integer.valueOf(inPort), inHost, sslContext)
                .setHandler(new HttpHandler() {
                    @Override
                    public void handleRequest(HttpServerExchange exchange) throws Exception {
                        if (!status.equals("200")) {
                            reply = "Setup error.";
                        } else {
                            inRequest = "";
                            if (exchange.getRequestMethod().toString().equals("POST")) {
                                while(inRequest.equals("")) {
                                    if (!exchange.getResponseHeaders().contains(thisHDR))
                                        exchange.getResponseHeaders().add(Headers.LOCATION, thisHDR);
                                    HeaderMap headers = exchange.getRequestHeaders();
                                    // -----------------------------------------------------------------------------
                                    exchange.getRequestReceiver().receiveFullBytes((body, data) -> {
                                                String req = new String(data);
                                                inRequest = req;
                                                reply = ProcessRequest(headers, req);
                                            },
                                            (body, exception) -> {
                                                SendMessage("Error-Handler()");
                                                inRequest = exception.toString();
                                                SendMessage("*");
                                                SendMessage("-------------------------------------------------------------------------------");
                                                SendMessage("An HTTPS exception has occurred: [" + exception.toString() + "]");
                                                SendMessage("-------------------------------------------------------------------------------");
                                                SendMessage("*");
                                                reply = ErrorHandler(inRequest);
                                            });
                                }
                            } else {
                                SendMessage("-------------------------------------------------------------");
                                SendMessage("Exchange issue:");
                                SendMessage("Request did not match end-point or POST method" );
                                SendMessage("-------------------------------------------------------------");
                                reply = "---";
                            }
                            Thread.sleep(500);
                        }
                        SetAndSend(exchange, status, reply);
                        exchange = null;
                    }
                })
                .build();
        server.start();
        SendMessage("Accepting requests at : " + thisHDR);
        SendMessage("-----------------------------------------------------------------");
        SendMessage(STAR);
    }

    private static String ProcessRequest(HeaderMap headers, String req) {
        issuer   = headers.get(RFUEL_API).toString();      // licenced subscribers - future dev
        passport = headers.get(RFUEL_KEY).toString();
        if (issuer.startsWith(SQB_OPEN)) issuer = issuer.substring(1, issuer.length()-1);
        if (passport.startsWith(SQB_OPEN)) passport = passport.substring(1, passport.length()-1);

        req = uCipher.Decrypt(req+TILDE+passport+TILDE);

        String answer = BuildJson(req);

        reqKey = FieldOf(answer, TMARK, 1);
        reqRec = FieldOf(answer, TMARK, 2);
        if (msgKeys.indexOf(reqKey) < 0) {
            lx = reqRec.length();
            msgKeys.add(reqKey);
            messages.add(reqRec);
            thisBatch = thisBatch + lx;
            if ((thisBatch) > kafkaBatchSize) {
                if (!SendToKafka()) {
                    if (!SendToSQL()) {
                        // ********* write to disk *********
                        SendMessage("Oh Shit !!");
                    }
                }
                msgKeys.clear();
                messages.clear();
                thisBatch = 0;
            }
        }
        return "got it!";
    }

    private static String BuildJson(String event) {
        String sRec, dt, tm, acct, file, item, recd, json, host=coreApp, logKey;
        sRec  = FieldOf(event, TMARK, 2);
        event = FieldOf(event, TMARK, 1);

        dt = FieldOf(event, "\\.", 1);
        dt = oconvD(dt, DCONV);
        tm = FieldOf(event, "\\.", 2);
        tm = oconvM(tm, TCONV);

        acct = FieldOf(sRec, IMark, 2);
        file = FieldOf(sRec, IMark, 3);
        item = FieldOf(sRec, IMark, 4);
        recd = FieldOf(sRec, IMark, 5);

        json = BASELOG;
        json = json.replace(PASSPORT, passport + TILDE + issuer);
        json = json.replace(SOURCE, host);
        json = json.replace(ACCOUNT, acct);
        json = json.replace(DATE, dt);
        json = json.replace(TIME, tm);
        json = json.replace(FILE, file);
        json = json.replace(ITEM, item);
        json = json.replace(RECORD, recd);

        switch (coreApp) {
            case ULTRACS:
                logKey = file + TILDE + item;
                break;
            case ERA:
                logKey = host + TILDE + acct + TILDE + file + TILDE + item;
                break;
            default:
                logKey = acct + TILDE + file + TILDE + item;
        }
        return logKey+TMARK+json;
    }

    public static String FieldOf(String str, String findme, int occ) {
        String ans, chkr;
        int iChk;
        String[] tmpStr;
        try {
            chkr = str;
        } catch (NullPointerException npe) {
            SendMessage("ERROR: FieldOf() received a null string value");
            return "";
        }
        try {
            iChk = occ;
        } catch (NullPointerException npe) {
            SendMessage("ERROR: FieldOf() received a null occurance value");
            return "";
        }
        tmpStr = str.split(findme);
        if (occ <= tmpStr.length) {
            ans = tmpStr[occ - 1];
        } else {
            ans = "";
        }
        return ans;
    }

    public static String oconvM(String base, String cc) {
        base.trim();
        if (!isNumeric(base)) return base;
        String theAns = base, chr = "";
        String sign = "";
        if (base.contains("-")) {
            sign = "-";
            base = base.replaceAll("\\-", "");
        }
        if (base.contains("+")) {
            sign = "+";
            base = base.replaceAll("\\+", "");
        }
        if (cc.substring(1, 2).equals("D")) {
            int dpl;
            try {
                dpl = Integer.valueOf(cc.substring(2, 3)); //MD2,
            } catch (NumberFormatException nfe) {
                SendMessage("Bad conversion code: " + cc + " Setting to 0 decimal places.");
                dpl = 0;
            }
            String number = base;
            if (dpl == 0) {
                number += "";
            } else {
                int ix1 = (base.length() - dpl);
                while (ix1 < dpl) {
                    base = "0" + base;
                    ix1 = (base.length() - dpl);
                }
                String p1 = base.substring(0, ix1);
                String p2 = base.substring(ix1, base.length());
                number = p1 + "." + p2;
            }
            String cm = "", dlr = "", pct = "";
            if (cc.indexOf(",") > 0) cm = ",";
            if (cc.indexOf("$") > 0) dlr = "$";
            if (cc.indexOf("%") > 0) pct = "%";
            String fmt, zeros;
            zeros = "." + "0000000000".substring(0, dpl);
            if (zeros.equals(".")) zeros = "";
            if (cm.length() > 0) {
                fmt = "#,##0" + zeros;
            } else {
                fmt = "##0" + zeros;
            }
            double amount;
            try {
                amount = Double.parseDouble(number);
                DecimalFormat formatter = new DecimalFormat(fmt);
                theAns = dlr + formatter.format(amount) + pct;
            } catch (NumberFormatException e) {
                theAns = number;
            }
        }
        if (cc.substring(1, 2).equals("T")) {
            int seconds = Integer.valueOf(base);
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            cal.add(Calendar.SECOND, seconds);
            Date timeOfDay = cal.getTime();
            String fmt = "kk:mm", tCode = "";
            if (cc.length() > 2) {
                if (seconds < 43200) fmt = "kk:mm a";
                cc = cc.toUpperCase();
                tCode = cc.substring(2, cc.length());
                switch (tCode) {
                    case "H":
                        fmt = "hh:mm a";
                        break;
                    case "S":
                        fmt = "HH:mm:ss";
                        break;
                    case "HS":
                        fmt = "hh:mm:ss a";
                        break;
                    case "SH":
                        fmt = "hh:mm:ss a";
                        break;
                    case ".":
                        fmt = "kk.mm";
                        break;
                }
            }
            SimpleDateFormat sdf = new SimpleDateFormat(fmt);
            theAns = sdf.format(timeOfDay);
            theAns = theAns.toLowerCase().replaceAll("\\ ", "");
        }
        theAns = sign + theAns;
        return theAns;
    }

    public static String oconvD(String base, String cc) {
        base.trim();
        if (!isNumeric(base)) return base;
        Calendar cal = Calendar.getInstance();
        int addDays = 0;
        String sep = " ";
        if (cc.length() > 2) sep = cc.substring(2, 3);
        SimpleDateFormat D4 = new SimpleDateFormat("dd" + sep + "MM" + sep + "yyyy");
        SimpleDateFormat D2 = new SimpleDateFormat("dd" + sep + "MM" + sep + "yy");
        String Day0 = "31" + sep + "12" + sep + "1967";
        String theAns = "", fmt = "dmy";

        try {
            addDays = Integer.valueOf(base);
            Date dd = null;
            try {
                dd = D4.parse(Day0);
                cal.setTime(dd);
                cal.add(Calendar.DATE, addDays);
            } catch (ParseException e) {
                SendMessage("oconvD Parse error: " + e.getMessage());
                System.exit(0);
                theAns = base;
                return theAns;
            }
            if (!sep.equals(" ")) {
                if (cc.length() > 3) fmt = cc.substring(3, cc.length()).toLowerCase();
                cc = cc.substring(0, 2);
            }
            switch (sep) {
                case " ":
                    switch (fmt) {
                        case "dmy":
                            fmt = "dd MMM yyyy";
                            break;
                        case "mdy":
                            fmt = "MMM dd yyyy";
                            break;
                        case "ymd":
                            fmt = "yyyy MMM dd";
                            break;
                        case "ydm":
                            fmt = "yyyy dd MMM";
                            break;
                    }
                    break;
                default:
                    switch (fmt) {
                        case "dmy":
                            fmt = "dd" + sep + "MM" + sep + "yyyy";
                            break;
                        case "mdy":
                            fmt = "MM" + sep + "dd" + sep + "yyyy";
                            break;
                        case "ymd":
                            fmt = "yyyy" + sep + "MM" + sep + "dd";
                            break;
                        case "ydm":
                            fmt = "yyyy" + sep + "dd" + sep + "MM";
                            break;
                    }
            }
            switch (cc) {
                case "D4":
                    D4 = new SimpleDateFormat(fmt);
                    theAns = D4.format(cal.getTime());
                    if (sep.equals(" ")) theAns = theAns.toUpperCase();
                    break;
                case "D2":
                    if (sep.equals(" ")) {
                        fmt = "dd MMM yy";
                    } else {
                        fmt = "dd" + sep + "MM" + sep + "yy";
                    }
                    D2 = new SimpleDateFormat(fmt);
                    theAns = D2.format(cal.getTime());
                    if (sep.equals(" ")) theAns = theAns.toUpperCase();
                    break;
                default:
                    theAns = D4.format(cal.getTime());
            }
        } catch (NumberFormatException nfe) {
            theAns = base;
        }
        return theAns;
    }

    public static boolean isNumeric(String str) {
        boolean ans = true;
        try {
            double d = Double.parseDouble(str);
        } catch (NumberFormatException nfe) {
            ans = false;
        }
        return ans;
    }

    private static boolean SendToKafka() {
        // get a new Producer ----------------------
        if (producer != null) CloseProducer();
        producer = GetProducer();
        if (producer == null) return false;
        //
        int nbrLoops = messages.size();
        String combo = keyType + HYPHEN + valType;
        boolean success = true;
        String skey, sval;

        // send batch inside a transaction boundary
        try {
            producer.beginTransaction();
            for (int i = 0; i < nbrLoops; i++) {
                skey = msgKeys.get(i);
                sval = messages.get(i);
                switch (combo) {
                    case "STRING-STRING":
                        producer.send(new ProducerRecord<>(topic_name, skey, sval));
                        break;
                    case "STRING-BYTE":
                        producer.send(new ProducerRecord<>(topic_name, skey, sval.getBytes()));
                        break;
                    case "BYTE-STRING":
                        producer.send(new ProducerRecord<>(topic_name, skey.getBytes(), sval));
                        break;
                    case "BYTE-BYTE":
                        producer.send(new ProducerRecord<>(topic_name, skey.getBytes(), sval.getBytes()));
                        break;
                    default:
                        SendMessage("ProducerRecord cannot be created with : [" + keyType + "]  [" + valType + "]");
                        return false;
                }
            }
        } catch(Exception e) {
            SendMessage("send batch error: " + e.getMessage());
            producer.abortTransaction();
            success = false;
        }

        // commit or abort the transaction
        try {
            if (success) {
                SendMessage("commit batch (" + nbrLoops + ")");
                producer.commitTransaction();
                producer.flush();
                runningTot = runningTot + nbrLoops;
            }
        } catch (Exception e) {
            SendMessage("batch commit error: " + e.getMessage());
            producer.abortTransaction();
            success = false;
        }

        CloseProducer();
        // -----------------------------------------
        return success;
    }

    private static boolean SendToSQL() {
        boolean okay = true;
        int nbrLoops = messages.size();
        String cmd;
        ArrayList<String> cmds = new ArrayList<>();
        for (int i=0 ; i<nbrLoops ; i++) {
            cmd = sqlInsert;
            cmd = cmd.replace("$id$", msgKeys.get(i));
            cmd = cmd.replace("$event$", messages.get(i));
            cmds.add(cmd);
        }
        SqlCommands.SetBatching(true);
        SqlCommands.ExecuteSQL(cmds);
        SqlCommands.SetBatching(false);
        if (NamedCommon.ZERROR) okay = false;
        NamedCommon.ZERROR = false;
        cmds.clear();
        return okay;
    }

    private static String ErrorHandler(String err) {
        return err;
    }

    private static void SetAndSend(HttpServerExchange exchange, String status, String reply) {
        if (reply != null) {
            exchange.getResponseHeaders().put((Headers.CONTENT_TYPE), contentType);
            exchange.getResponseHeaders().put((Headers.STATUS), status);
            exchange.getResponseHeaders().put((Headers.CONTENT_LENGTH), reply.length());
            exchange.setStatusCode(Integer.valueOf(status));
            exchange.getResponseSender().send(reply);
        } else {
            SendMessage("No response.");
        }
    }

    private static KeyStore loadKeyStore(String name) throws Exception {
        String storeLoc = System.getProperty(name);
        if (keystoreNames.indexOf(name) < 0) {
            keystoreNames.add(name);
            keystoreLocns.add(storeLoc);
        }
        final InputStream stream;
        if (storeLoc == null) {
            stream = Http2Server.class.getResourceAsStream(name);
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

    private static SSLContext CreateContext(KeyStore kstore, KeyStore tstore) throws Exception {
        KeyManager[] keyManagers;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try {
            keyManagerFactory.init(kstore, password(keystoreName));
        } catch (UnrecoverableKeyException e) {
            SendMessage(">>>");
            SendMessage(">>> SECURITY ABORT: SSLContext for KeyStore: invalid password ");
            SendMessage(">>>");
            System.exit(1);
        }
        keyManagers = keyManagerFactory.getKeyManagers();
        TrustManager[] trustManagers;
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(tstore);
        trustManagers = trustManagerFactory.getTrustManagers();
        sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, null);
        return sslContext;
    }

    private static char[] password(String name) {
        String pw = name + ".password";
        if (keystoreNames.indexOf(name) < 0) {
            SendMessage("location for [" + name + ".password] has not been provided.");
            System.exit(1);
        } else {
            pw = keystoreLocns.get(keystoreNames.indexOf(name)) + ".password";
        }
        String pword = ReadDiskRecord(pw);
        while (pword.endsWith("\n")) { pword = pword.substring(0,pword.length()-1); }
        if (pword.startsWith("ENC(")) {
            pword = pword.substring(4, pword.length());
            while (!pword.endsWith(")") && pword.length() > 0) {
                pword = pword.substring(0, pword.length() - 1);
            }
            if (pword.endsWith(")")) pword = pword.substring(0, pword.length() - 1);
            pword = uCipher.Decrypt(pword);
        }
        return pword.toCharArray();
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
                SendMessage("read FAIL on " + infile);
                SendMessage(e.getMessage());
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    SendMessage("read FAIL on " + infile);
                    SendMessage(e.getMessage());
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                SendMessage("File Close FAIL on " + infile);
                SendMessage(e.getMessage());
            }
        } catch (IOException e) {
            SendMessage("-------------------------------------------------------------------");
            SendMessage("File Access FAIL :: " + infile);
            SendMessage(e.getMessage());
            SendMessage("-------------------------------------------------------------------");
        }
        return rec;
    }

    private static void SetSerdes(String key, String val) {
        keySerdes = SER_STRINGS;
        valSerdes = SER_STRINGS;
        if (key.toUpperCase().equals("BYTE")) {
            keyType = key.toUpperCase();
            keySerdes = SER_B_ARRAY;
        }
        if (val.toUpperCase().equals("BYTE")) {
            valType = val.toUpperCase();
            valSerdes = SER_B_ARRAY;
        }
    }

    private static Producer GetProducer() {
        if (producer != null) CloseProducer();
        if (configProperties != null) configProperties = null;
        configProperties = new Properties();

        if (sasl) {
            configProperties.put("security.protocol","SASL_SSL");
            configProperties.put("sasl.mechanism","PLAIN");
            if (saslKey.equals("")) {
                configProperties.put("sasl.jaas.config", saslAWS);
            } else {
                configProperties.put("sasl.jaas.config", saslAzure);
            }
        }

        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        configProperties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        configProperties.put("group.id", GROUP_ID_CONFIG);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerdes);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valSerdes);
        //
        // setting ACKS to "all" means that all brokers must acknowledge the update before this process can continue.
        //
        configProperties.put(ProducerConfig.ACKS_CONFIG, acks);
        //
        // Setting retries to max_value means it will keep retrying until the event is successfully produced.
        // May need to implement a failure handler;
        //      * set reties to nbr brokers - 1
        //      * when failure - send messages to a sql database - NOT a kafka topic !!
        //      * create a background handler - try to reproduce these messages
        //      * try not to create a dead-letter-topic !!
        //
        configProperties.put(ProducerConfig.RETRIES_CONFIG, retries);
        //
        if (batching) {
            configProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, TRUE);
            configProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, CLIENT_ID + COLON + epoch);
            configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaBatchSize);
            configProperties.put(ProducerConfig.LINGER_MS_CONFIG, LINGER);
            configProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxblockms);
            configProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, (kafkaBatchSize * KBYTE));
            if (!compression.equals("")) configProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
            epoch++;
            if (epoch > 10000) epoch = 0;
        }
        // ------------------------------------
        String combo = keyType + HYPHEN + valType;
        switch (combo) {
            case "STRING-STRING":
                producer = new KafkaProducer<>(configProperties, new StringSerializer(), new StringSerializer());
                break;
            case "STRING-BYTE":
                producer = new KafkaProducer<>(configProperties, new StringSerializer(), new ByteArraySerializer());
                break;
            case "BYTE-STRING":
                producer = new KafkaProducer<>(configProperties, new ByteArraySerializer(), new StringSerializer());
                break;
            case "BYTE-BYTE":
                producer = new KafkaProducer<>(configProperties, new ByteArraySerializer(), new ByteArraySerializer());
                break;
            default:
                SendMessage("Producer cannot be created with key-value type : [" + keyType + "-" + valType + "]");
                return null;
        }
        if (batching) {
            //
            //  For KAFKA     setup: /opt/kafka/config/server.properties
            //                listeners MUST use IP address for networking to work !
            //
            try {
                producer.initTransactions();
                SendMessage("producer transactions initialised.");
            } catch (TimeoutException te) {
                SendMessage("*********************************************");
                SendMessage(" Kafka producer initTransactions has FAILED.");
                SendMessage("*********************************************");
                producer.close();
                producer = null;
            }
        }
        // ------------------------------------
        return producer;
    }

    private static void CloseProducer() {
        if (producer == null) return;
        producer.flush();
        producer.close();
        producer = null;
    }

    private static void KafkaConnector() {

        SendMessage("Connecting to "+ brokers);
        SendMessage("     ClientID " + CLIENT_ID);
        SendMessage( "       Group " + GROUP_ID_CONFIG);

        GetProducer();

        SendMessage("Kafka producer created for brokers: " + brokers);

        // the lines below exist ONLY to check that the brokers are valid, active and running.
        // if they are - kMap will be populated then cleared.
        //    else     - kafka will throw a TimeoutException. Use this to trap broker errors.

        Properties cProps = configProperties;
        String keyDeser = DES_STRINGS, valDeser = DES_STRINGS;
        if (keySerdes.equals(SER_B_ARRAY)) keyDeser = DES_B_ARRAY;
        if (valSerdes.equals(SER_B_ARRAY)) valDeser = DES_B_ARRAY;
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeser);
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDeser);
        KafkaConsumer<String, String> kCons = new KafkaConsumer<>(cProps);
        SendMessage("checking broker status.");

        int backstop=0;
        boolean pass = false;
        while (backstop < 10) {
            try {
                // listTopics will show all topics in GROUP_ID_CONFIG ... if any exist.
                // A timeout may simply mean that the broker is not started.
                Map<String, List<PartitionInfo>> kMap = kCons.listTopics(Duration.ofMillis(2000));
                kMap.clear();
                kMap = null;
                pass = true;
                break;
            } catch (TimeoutException te) {
                backstop++;
                SendMessage("Fail: " + backstop + " of 10");
                if (backstop == 1) {
                    SendMessage(STAR);
                    SendMessage("   ***********************************************************");
                    SendMessage("   - Kafka brokers MAY not be running on: [" + brokers + "] !");
                    SendMessage("   - No topics were found in Consumer Group " + GROUP_ID_CONFIG);
                    SendMessage("   ***********************************************************");
                    SendMessage(STAR);
                }
            }
        }
        kCons.close();
        kCons= null;

        if (!pass) return;
        configProperties.clear();
        if (producer != null) SendMessage("k-Producer created ****************");
    }

    private static void SQLConnector() {
        SqlCommands.ConnectSQL();
        if (NamedCommon.ZERROR) return;
        String DB=NamedCommon.rawDB, SCH="cdc", TBL="KLOAD_FAILURES";
        String tcols[] = new String[] {"..Seqn", "-MsgID", " Message"};

        String cmd = SqlCommands.CreateTable(DB, SCH, TBL, tcols);
        ArrayList<String> cmds = new ArrayList<>();
        cmds.add(cmd);
        SqlCommands.ExecuteSQL(cmds);
        cmds.clear();
        cmd="";
        tcols=null;
        sqlInsert = "INSERT INTO [demo].[cdc].[KLOAD_FAILURES] (MsgID, Message) Values('$id$', '$event$')";
    }
    
    private static void Initialise(String cfile) {
        SetSASL(false, "", "", "");
        Properties props = uCommons.LoadProperties(cfile);
        SendMessage("Configure: " + cfile + " -----------------------------------");
        // -----------------------------------------------------------------------------------
        String runtype = props.getProperty("runtype", "uplift");
        batching    = (props.getProperty("batching", "false").toLowerCase().equals("true"));
        GROUP_ID_CONFIG = props.getProperty("group", "");
        topic_name  = props.getProperty(runtype, "NoTopicGiven");
        brokers     = props.getProperty("brokers", "");
        compression = props.getProperty("compression", "");
        coreApp     = props.getProperty("core.application", "");
        inHost      = props.getProperty("url.host", "");
        inPath      = props.getProperty("url.path", "");
        acks        = props.getProperty("acks", "all");

        inPort = 443;
        try {
            inPort = Integer.parseInt(props.getProperty("url.port", ""));
        } catch (NumberFormatException nfe) {
            SendMessage("url.port size must be an integer!! Default to 443");
        }
        retries = "";
        try {
            String sChk = props.getProperty("retries", "");
            int iChk = Integer.valueOf(sChk);
            retries = sChk;
        } catch (NumberFormatException nfe) {
            retries = Integer.toString(Integer.MAX_VALUE);
        }
        kafkaBatchSize = 1048576;
        try {
            kafkaBatchSize = Integer.valueOf(props.getProperty("kbatch", "1048576"));
        } catch (NumberFormatException nfe) {
            SendMessage("kbatch size must be an integer!!");
        }
        maxblockms = 30000;
        try {
            maxblockms = Integer.valueOf(props.getProperty("maxblockms", "30000"));
        } catch (NumberFormatException nfe) {
            SendMessage("maxblockms size must be an integer!!");
        }
        // -----------------------------------------------------------------------------------
        CLIENT_ID = "cdrReceiver:"+runID;
        IOthreads = 2;
        workerThreads = 4;
        status = "200";
        reply = "";
        inRequest = "";
        contentType = "text/*";
        keystoreNames = new ArrayList<>();
        keystoreLocns = new ArrayList<>();
        msgKeys = new ArrayList<>();
        messages = new ArrayList<>();
        thisHDR = "https://"+inHost+":"+inPort+ inPath;
        SetSerdes("BYTE", "BYTE");
        epoch = 1;
    }

    public static void SetSASL(boolean isSASL, String u, String p, String k) {
        saslUser = "";
        saslPass = "";
        saslKey  = "";      // Azure ONLY !!!
        saslAWS  = "";
        saslAzure= "";
        sasl = isSASL;
        if (sasl) {
            saslUser = u;
            saslPass = p;
            saslKey  = k;
            saslAzure= "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://drlcevents.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey="+saslKey+"\";";
            saslAWS  = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+saslUser+"\" password=\""+saslPass+"\";";
        }
    }
    
    private static void SendMessage(String msg) {
//        System.out.println(new Date() + " " + msg);
        uCommons.uSendMessage(msg + spc);
    }

    public static void main(String[] args) {
        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            uCommons.SetMemory("domain", "rfuel22");
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            NamedCommon.upl = NamedCommon.BaseCamp;
            NamedCommon.slash = "/";
        }
        //
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        //
        String uuid = UUID.randomUUID().toString();
        runID = RandomStringUtils.random(4, uuid);
        String configFile = System.getProperty("conf", "kafkaMaster.properties");
        if (configFile.equals("")) configFile = "kafkaMaster.properties";   // in case -Dconf= is empty
        Initialise(configFile);
        //
        SendMessage(STAR);
        SendMessage("[ Connect with Kafka    ] ---------------------------------------");
        KafkaConnector();
        if (producer == null) System.exit(1);
        CloseProducer();
        SendMessage(STAR);
        SendMessage("[ Connect with SQL      ] ---------------------------------------");
        SQLConnector();
        if (!NamedCommon.tConnected) System.exit(1);
        SecureProcess();
    }

}
