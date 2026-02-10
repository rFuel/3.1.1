package com.unilibre.kafka;


import com.unilibre.commons.uCommons;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

class uDelta2kafka implements Callable {

    private final String TMARK="<tm>", IMark = "<im>";
    private final String BASELOG = "{\"passport\":\"$passport\",\"sourceinstance\":\"$source\",\"sourceaccount\":\"$account\",\"date\":\"$date\",\"time\":\"$time\",\"item\":\"$item\",\"file\":\"$file\",\"record\":\"$record\"}";
    private final String SER_STRINGS = StringSerializer.class.getName();        // Serdes.String().getClass().getName();
    private final String DES_STRINGS = StringDeserializer.class.getName();      // Serdes.String().getClass().getName();
    private final String SER_B_ARRAY = BytesSerializer.class.getName();         // Serdes.ByteArray().getClass().getName();
    private final String DES_B_ARRAY = BytesDeserializer.class.getName();       // Serdes.ByteArray().getClass().getName();
    private final int    LINGER = 200;
    //
    // The following MUST be specific for each thread - they may be different each time.
    //
    private String host, topic, processor;
    private String brokers="", topic_name="", GROUP_ID_CONFIG="", CLIENT_ID="", compression, coreApp;
    private String saslUser="", saslPass="",saslKey="", saslAWS  = "", saslAzure= "", logKey="";
    private String retries = Integer.toString(Integer.MAX_VALUE), acks="all";
    private String keySerdes, valSerdes, keyType, valType;
    private int tNumber, TransID, maxBlockingMS, proccnt, runningTot, batchTot;
    private boolean showItems, batching, sasl, verbose;
    private Producer producer;
    private ArrayList<String> logEvents;
    private ArrayList<String> messages;
    private ArrayList<String> msgKeys;
    private ArrayList<String> deleteList;
    private ArrayList<String> returnList;
    private ArrayList<String> reclaimList;
    private int batchSize;
    private int batchCnt;

    public uDelta2kafka(String[] args, int tno) {
        this.tNumber = tno;
        this.producer = null;
        this.showItems = false;
        this.batching=false;
        this.messages = new ArrayList<>();
        this.msgKeys  = new ArrayList<>();
        this.deleteList = new ArrayList<>();
        this.processor = "uD2K-" + tNumber;
        this.topic_name ="";
        this.GROUP_ID_CONFIG="";
        this.CLIENT_ID="";
        this.saslUser="";
        this.saslPass="";
        this.saslKey="";
        this.saslAWS="";
        this.saslAzure= "";
        this.compression="";
        this.coreApp="";
        this.sasl=false;
        this.TransID=0;
        this.batchCnt=0;
        this.batchSize = 8096;
        this.maxBlockingMS = 30000;
        this.proccnt = 100;
        this.keySerdes = SER_STRINGS;
        this.valSerdes = SER_B_ARRAY;
        //
        // there is nolonger a need to pass data through args - use setters instead.
        //
        this.host = args[0];
        this.topic = args[13];
        this.brokers= args[14];
        this.verbose= args[16].equals("true");

        this.logEvents = new ArrayList<>(Arrays.asList(args[10].split("\\|")));

        if (args[12].equalsIgnoreCase("true")) this.showItems = true;

    }

    // THIS when callable (to return values)
    @Override
    public Object call() {
        jobHandler();
        returnList.addAll(reclaimList);
        return returnList;
    }

    // THIS when runable (does not return values)
    public void run() {
        jobHandler();
    }

    // THIS when run as a pojo method
    public void containerRunner() {
        jobHandler();
    }

    private void jobHandler() {
//        producer = ConnectKafka();
//        if (producer == null) return;
        System.out.println(new Date() + " " + processor + " started");
        String passport = "not~in", issuer = "use", event;
        String sRec, dt, tm, acct, file, item, recd, json;
        boolean eop = false, eof = false, success;
        int cnt = 0, pcnt = 0;
        runningTot = 0;
        messages = new ArrayList<>();       // these are the json deltas which are sent to kafka
        msgKeys  = new ArrayList<>();       // these are the keys to id each message in kafka
        deleteList = new ArrayList<>();     // this will become redundant once Callable is tested and hardened.
        deleteList.add("DELETE");           // FORCE into item 0 of list.
        returnList = new ArrayList<>();     // this list will be returned to the caller.
        reclaimList = new ArrayList<>();    // when kafka producer fails, reclaim the items in uDELTA.LOG
        reclaimList.add("RECLAIM");         // FORCE into item 0 of list.
        int batchLen = 0;

        try {
            int progress=0;
            batchTot = logEvents.size();
            if (batchTot < 1) eop = true;

            while (!eop) {
                if (cnt == batchTot) {
                    eop = true;
                    continue;
                }
                event = logEvents.get(cnt);
                if (event.equals("")) {
                    System.out.println(new Date() + " " + processor + "   skipping empty event.");
                    continue;
                }
                cnt++;

                sRec  = FieldOf(event, TMARK, 2);
                event = FieldOf(event, TMARK, 1);

                dt = FieldOf(event, "\\.", 1);
                dt = oconvD(dt, "D4-");
                tm = FieldOf(event, "\\.", 2);
                tm = oconvM(tm, "MTS");

                acct = FieldOf(sRec, IMark, 2);
                file = FieldOf(sRec, IMark, 3);
                item = FieldOf(sRec, IMark, 4);
                recd = FieldOf(sRec, IMark, 5);

                json = BASELOG;
                json = json.replace("$passport", passport + "~" + issuer);
                json = json.replace("$source", host);
                json = json.replace("$account", acct);
                json = json.replace("$date", dt);
                json = json.replace("$time", tm);
                json = json.replace("$file", file);
                json = json.replace("$item", item);
                json = json.replace("$record", recd);

                switch (coreApp) {
                    case "ultracs":
                        logKey = file + ">" + item;
                        break;
                    case "era":
                        logKey = host + ">" + acct + ">" + file + ">" + item;
                        break;
                    default:
                        logKey = acct + ">" + file + ">" + item;
                }

                if (batching) {
                    // ----------------------------------------------
                    // TRIAL: let Kafka manage the batches
                    // if this works - pcnt is redundant
                    // ----------------------------------------------
//                    batchLen = batchLen + json.length();
//                    if (batchLen > batchSize) {
//                        // send the batch before adding this message to it.
//                        success = kBatch();
//                        if (success) {
//                            pcnt = pcnt + messages.size();
//                            returnList.addAll(deleteList);
//                            deleteList.clear();
//                            deleteList.add("DELETE");           // FORCE into item 0 of list.
//                        } else {
//                            deleteList.set(0, "RECLAIM");
//                            reclaimList.addAll(deleteList);
//                        }
//                        messages.clear();
//                        msgKeys.clear();
//                        messages.add(json);
//                        msgKeys.add(logKey);
//                        deleteList.add(event);
//                        batchLen = json.length();
//                        batchCnt = 0;
//                        progress = 0;
//                    } else {
                        messages.add(json);
                        msgKeys.add(logKey);
                        deleteList.add(event);
                        batchCnt++;
                        if (verbose) {
                            progress++;
                            if (progress >= proccnt) {
                                System.out.println(new Date() + " " + processor + "   added " + batchCnt + " events to batch");
                                progress = 0;
                            }
                        }
//                    }
                } else {
                    if (producer == null) producer = ConnectKafka();
                    if (producer == null) return;
                    deleteList.add(event);
                    ProducerRecord<String, Field.Str> payload = new ProducerRecord(topic, logKey, json);
                    producer.send(payload);
                    if (showItems) System.out.println(processor + "  " + event);
                    payload = null;
                    pcnt++;
                }
            }

            if (messages.size() >0) {
                success = kBatch();
                if (success) {
                    pcnt = pcnt + messages.size();
                    returnList.addAll(deleteList);
                    messages.clear();
                    msgKeys.clear();
                    deleteList.clear();
                    if (verbose) System.out.println(new Date() + " " + processor + "   sent " + batchCnt + " events");
                } else {
                    reclaimList.addAll(deleteList);
                }
            }

            Close();

        } catch (Exception e) {
            reclaimList.addAll(deleteList);
            System.out.println(new Date() + " " + processor + " ------------------------------------------------------------------------------------");
            System.out.println(new Date() + " " + processor + " " + e.getMessage());
            System.out.println(new Date() + " " + processor + " ------------------------------------------------------------------------------------");
            BufferedWriter bWriter = null;
            File outFile = new File("./conf/STOP");
            try {
                bWriter = new BufferedWriter(new FileWriter(outFile));
                bWriter.write("stop");
                bWriter.newLine();
                bWriter.flush();
                bWriter.close();
            } catch (IOException bwe) {
                System.out.println(new Date() + " " + processor + " " + bwe.getMessage());
            }
        }

    }

    public String oconvD(String base, String cc) {
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
                System.out.println(processor +"oconvD Parse error: " + e.getMessage());
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

    public String oconvM(String base, String cc) {
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
                System.out.println(processor +"Bad conversion code: " + cc + " Setting to 0 decimal places.");
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

    public boolean isNumeric(String str) {
        boolean ans = true;
        try {
            double d = Double.parseDouble(str);
        } catch (NumberFormatException nfe) {
            ans = false;
        }
        return ans;
    }

    public String FieldOf(String str, String findme, int occ) {
        String ans, chkr;
        int iChk;
        String[] tmpStr;
        try {
            chkr = str;
        } catch (NullPointerException npe) {
            System.out.println(processor +" ERROR: FieldOf() received a null string value");
            return "";
        }
        try {
            iChk = occ;
        } catch (NullPointerException npe) {
            System.out.println(processor +" ERROR: FieldOf() received a null occurance value");
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

    private Producer<String, String> ConnectKafka() {

        // NOTES:
        //      at all times below, when you see kafka, it may be the CONFLUENT PLATFORM
        //      Download from: https://docs.confluent.io/platform/current/installation/installing_cp/zip-tar.html
        // Kafka     is "okay" for fire-and-forget services - extremely SLOW
        // Confluent is more mature with client connectivity.

        if (verbose) {
            System.out.println(new Date() + " " + processor + " Connecting to Kafka with ClientID " + CLIENT_ID + " in group " + GROUP_ID_CONFIG);
        }

        Properties configProperties = new Properties();

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
            configProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            configProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, CLIENT_ID + "-" + TransID);
            configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);          // should be in K-bytes
            configProperties.put(ProducerConfig.LINGER_MS_CONFIG, LINGER);
            configProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockingMS);
            configProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, (batchSize * 1024));
            if (!compression.equals("")) configProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        }
        // ------------------------------------
        String combo = keyType + "-" + valType;
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
                System.out.println(new Date() + " " + processor + " Producer cannot be created with key-value type : [" + keyType + "-" + valType + "]");
                return null;
        }

        if (verbose) System.out.println(new Date() + " " + processor + " Kafka producer created for brokers: " + brokers);

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
        if (verbose) System.out.println(new Date() + " " + processor + " checking broker status.");

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
                System.out.println(new Date() + " " + processor + " Fail: " + backstop + " of 10");
                if (backstop == 1) {
                    System.out.println();
                    System.out.println("   ***********************************************************");
                    System.out.println("   - Kafka brokers MAY not be running on: [" + brokers + "] !");
                    System.out.println("   - No topics were found in Consumer Group " + GROUP_ID_CONFIG);
                    System.out.println("   ***********************************************************");
                    System.out.println();
                }
            }
        }
        kCons.close();
        kCons= null;

        if (!pass) return null;

        if (verbose) {
            String rightNow;
            rightNow = uCommons.GetLocaltimeStamp(); // new Date();
            System.out.println(rightNow + " " + processor + " ------------------------------------------------");
            System.out.println(rightNow + " " + processor + " Connected to " + brokers);
            System.out.println(rightNow + " " + processor + " ------------------------------------------------");
            rightNow = null;
        }

        //
        // With KAFKA     initTransactions will only work with
        //                kafka v 3.5.0 and kafka-client v 3.4.0 !!
        // With CONFLUENT initTransactions works as documented, but is REST based !!!
        //
        if (batching) {
            //
            //  For KAFKA     setup: /opt/kafka/config/server.properties
            //                listeners MUST use IP address for networking to work !
            //
            try {
                producer.initTransactions();
                if (verbose) {
                    System.out.println(new Date() + " " + processor + " producer transactions initialised.");
                }
            } catch (TimeoutException te) {
                System.out.println("*********************************************");
                System.out.println(" Kafka producer initTransactions has FAILED.");
                System.out.println("*********************************************");
                producer.close();
                producer = null;
            }
        }

        configProperties.clear();
        if (verbose && producer != null) System.out.println(new Date() + " " + processor + " k-Producer created ****************");
        return producer;
    }

    public boolean kBatch() {
        // get a new Producer ----------------------
        if (producer != null) Close();
        producer = ConnectKafka();
        if (producer == null) return false;
        // -----------------------------------------

        int nbrLoops = messages.size();
        String combo = keyType + "-" + valType;
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
                        System.out.println(new Date() + " " + processor + " ProducerRecord cannot be created with : [" + keyType + "]  [" + valType + "]");
                        return false;
                }
            }
        } catch(Exception e) {
            System.out.println(new Date() + " " + processor + " send batch error: " + e.getMessage());
            producer.abortTransaction();
            success = false;
        }

        // commit or abort the transaction
        try {
            if (success) {
                System.out.println(new Date() + " " + processor + " commit batch (" + nbrLoops + ")");
                producer.commitTransaction();
                producer.flush();
                if (verbose) System.out.println(new Date() + " " + processor + " commit complete.");
            }
            runningTot = runningTot + nbrLoops;
        } catch (Exception e) {
            System.out.println(new Date( ) + " " + processor + " batch commit error: " + e.getMessage());
            producer.abortTransaction();
            success = false;
        }

        Close();
        return success;
    }

    public boolean kafkaIsReady() {
        boolean ans = false;
        if (ConnectKafka() != null) {
            ans = true;
            Close();
        }
        return ans;
    }

    public void Close () {
        if (producer == null) return;
        producer.flush();
        producer.close();
        producer = null;
        if (verbose) System.out.println(new Date() + " " + processor + " k-Producer closed  ****************");
    }

    public ArrayList<String> GetDeleteables() {return deleteList;}

    public void SetSerdes(String key, String val) {
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
    
    public void SetVerbose(boolean inval) { verbose = inval; }

    public void SetBroker(String inval) {brokers = inval;}

    public void SetSASL(boolean isSASL, String u, String p, String k) {
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

    public void SetTopic(String inval) {
        topic_name = inval;
    }

    public void SetGroup(String inval) {
        GROUP_ID_CONFIG = inval.replaceAll("\\:", "_");
    }

    public void SetClientID(String inval) {CLIENT_ID = inval.replaceAll("\\:", "_");}

    public void SetBatchSize(int inval) {batchSize = inval;}

    public void SetBatching(boolean inval) { batching = inval; }

    public void SetTransID(int inval) {TransID = inval;}

    public void SetBlockingMS(int inval) {maxBlockingMS = inval;}

    public void SetProcCnt(int inval) {proccnt = inval;}

    public void SetAcks(String inval) { acks = inval; }

    public void SetRetries(String inval) { retries = inval; }

    public void SetCompression(String inval) {compression = inval;}

    public void SetCoreApp(String inval) {coreApp = inval.toLowerCase();}

}
