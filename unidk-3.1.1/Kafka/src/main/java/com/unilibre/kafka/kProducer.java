package com.unilibre.kafka;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class kProducer {

    private static boolean sasl = false;
    private static String brokers = "", saslUser="", saslPass="",saslKey="", saslAWS  = "", saslAzure= "";
    private static String CLIENT_ID="stdClient";
    private static String GROUP_ID_CONFIG="stdGroup";
    private static String TOPIC_NAME="rFuel";
    private static String ACKS = "all"; // Strongest setting for message acknowledgement in producers
    private static Properties configProperties;
    private static ArrayList<String> topicArr = new ArrayList<>();
    private static ArrayList<String> keyArr = new ArrayList<>();
    private static ArrayList<String> valArr = new ArrayList<>();
    private static AdminClient admin= null;
    private static int batchSize=1, lingerMS=500, TransID=0;
    private static long last = System.currentTimeMillis(), now, diff;
    public static Producer producer = null;

    public static String GetBrokers() { return brokers; }

    public static void SetBroker(String inval) {brokers = inval;}

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

    public static void SetTopic(String inval) {
        TOPIC_NAME = inval;
    }

    public static void SetGroup(String inval) {
        GROUP_ID_CONFIG = inval;
    }

    public static void SetClientID(String inval) { CLIENT_ID = inval; }

    public static boolean isKafkaReady() {
        boolean ans = false;
        if (producer != null) ans = true;
        return ans;
    }

    public static void SetBatchSize(int inval) {batchSize = inval;}

    public static void SetLinger(int inval) {lingerMS = inval;}

    public static void SetTransID(int inval) {TransID = inval;}

    public static Producer kConnect() {
        System.out.println(" ");
        CLIENT_ID = CLIENT_ID.replaceAll("\\:", "_");
        GROUP_ID_CONFIG = GROUP_ID_CONFIG.replaceAll("\\:", "_");

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
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.ACKS_CONFIG, ACKS);
        configProperties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        // if batching ------------------------
        if (batchSize > 1) {
            configProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            configProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, CLIENT_ID + "-" + TransID);
            configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            configProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        }
        // ------------------------------------
        Producer<String, String> prod = new KafkaProducer<>(configProperties, new StringSerializer(), new StringSerializer());
        System.out.println(" ");
        System.out.println(" created kafka producer for brokers: " + brokers);
        System.out.println("    .> CLIENT_ID_CONFIG        " + CLIENT_ID);
        System.out.println("    .> GROUP_ID_CONFIG         " + GROUP_ID_CONFIG);
        if (batchSize > 1) {
            System.out.println("    .> TRANSACTIONAL_ID_CONFIG " + CLIENT_ID + "-" + TransID);
            System.out.println("    .> BATCH_SIZE_CONFIG       " + batchSize);
        }
        System.out.println(" ");

        // the lines below exist only to check that the brokers are valid, active and running.
        // if they are - kMap will be populated then cleared.
        //    else     - kafka will throw a TimeoutException. So use this to trap broker errors.

        Properties cProps = configProperties;
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> kCons = new KafkaConsumer<>(cProps);
        System.out.println(" check if the broker is running.");
        try {
            // listTopics will work show all topics in GROUP_ID_CONFIG ... if any exist.
            // A timeout may simply mean that this job will create the topic
            Map<String, List<PartitionInfo>> kMap = kCons.listTopics(Duration.ofMillis(900));
            kMap.clear();
            kMap = null;
            System.out.println(" CONFIRMED  - broker is running.");
        } catch (TimeoutException te) {
            System.out.println();
            System.out.println("   ***********************************************************");
            System.out.println("   - Kafka brokers MAY not be running on: ["+brokers+"] !");
            System.out.println("   - Or, no topics were found in Consumer Group " + GROUP_ID_CONFIG);
            System.out.println("   ***********************************************************");
            System.out.println();
        }

        producer = prod;
        if (batchSize > 1) {
            //
            // With KAFKA     initTransactions will only work with
            //                kafka v 3.5.0 and kafka-client v 3.4.0 !!
            //                /opt/kafka/config/server.properties
            //                  listeners MUST use IP address for networking to work!
            //
            int tries=1, max=25;
            boolean okay=false;
            while (!okay) {
                if (tries > max) {
                    System.out.println(" ");
                    System.out.println("*********************************************");
                    System.out.println("Doesn't seem that a transaction connection ");
                    System.out.println(" can be established with Kafka instance on: "+brokers);
                    System.out.println("*********************************************");
                    System.out.println(" ");
                    break;
                } else {
                    System.out.println(" ");
                    System.out.println("Try # " + tries + " of " + max);
                }
                try {
                    producer.initTransactions();
                    System.out.println(" ");
                    System.out.println(" producer transactions initialised.");
                    System.out.println(" ");
                    okay = true;
                } catch (IllegalStateException ise) {
                    System.out.println("*********************************************");
                    System.out.println(" Kafka producer initTransactions has FAILED.");
                    System.out.println("   > " + ise.getMessage());
                    System.out.println("*********************************************");
                } catch (TimeoutException te) {
                    System.out.println("*********************************************");
                    System.out.println(" Kafka producer initTransactions has FAILED.");
                    System.out.println("   > " + te.getMessage());
                    System.out.println("*********************************************");
                }
                tries++;
            }
            if (!okay) {
                producer.close();
                producer = null;
            }
        }

        configProperties.clear();
        cProps.clear();
        prod = null;
        kCons.close();
        kCons= null;
        return producer;
    }

    public static boolean kBatchCollector(String topic, String key, String value) {
        //  message will be sent in this structure;
        //  {site}-{host}-{dacct}-{file}-{version}-{item} IMark {json payload}
        //  e.g. qhealth-hospital-ward-observations-v1-123456<IM>{....payload....}
        topicArr.add(topic);
        keyArr.add(key);
        valArr.add(value);      //  add one message at a time
        if (valArr.size() >= batchSize) return kBatch();
        return true;
    }

    public static boolean kBatch() {
        if (producer == null) producer = kConnect();
        if (batchSize <= 1) {
            int eom = valArr.size();
            for (int m=0 ; m < eom ; m++) {
                if (!kSend(topicArr.get(m), keyArr.get(m), valArr.get(m))) return false;
            }
            topicArr.clear();
            keyArr.clear();
            valArr.clear();
            return true;
        }

        // Synchronous producer:- the "Atleast Once" method
        // Events are sent in batches - like doing a Bulk Insert.
        int retry=0;
        while (true) {
            try {
                int nbrLoops = valArr.size();
                if (nbrLoops > 0) {
                    System.out.println(" ");
                    System.out.println("kBatch flushing " + nbrLoops + " events.");
                    System.out.println(" ");
                    producer.beginTransaction();
                    ProducerRecord<String, Field.Str> payload;
                    for (int i = 0; i < nbrLoops; i++) {
                        payload = new ProducerRecord(topicArr.get(i), keyArr.get(i), valArr.get(i));
                        producer.send(payload);
                        payload = null;
                    }
                    producer.commitTransaction();
                    topicArr.clear();
                    keyArr.clear();
                    valArr.clear();
                }
                break;
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                // FATAL ERRORS: We can't recover from these, so close the producer and exit.
                //             : data is safe in the arraylists until a producer is re-established
                //             : may need to raise ALERT so we don't blow memory.
                Close();
                return false;
            } catch (KafkaException e) {
                retry+=1;
                if (retry > 10) return false;
                if (retry > 3) {
                    System.out.println("... recovery not successful. Reconnecting to Kafka.");
                    System.out.println("1. Disconnect this (bad) connection.");
                    Close();
                    SetTransID(TransID + 1);
                    System.out.println("2. Reconnect to brokers.");
                    kConnect();
                    retry = 10;
                }
                System.out.println("... recovery attempt # "+retry+" to produce the kafka message.");
                // For all other exceptions, just abort the transaction and try again.
                producer.abortTransaction();
                try {
                    Thread.sleep(150);
                } catch (InterruptedException ie) {
                    System.out.println(ie.getMessage());
                }
                return false;
            }
        }
        return true;
    }

    public static boolean kSend (String topic, String key, String value) {
        // fire and forget method of kafka production - resilient but VERY slow !!
        if (producer == null) producer = kConnect();
        if (producer == null) {
            System.out.println("FATAL ERROR : Kafka connection cannot be established.");
            System.exit(1);
        }
        if (topic.equals("") || topic.equals("topic")) topic=TOPIC_NAME;
        ProducerRecord<String, Field.Str> msg = new ProducerRecord(topic, key, value);
        producer.send(msg);
        producer.flush();
        msg = null;
        key = null;
        return true;
    }

    public static void Close () {
        if (producer == null) return;
        producer.close();
        producer = null;
        configProperties.clear();
        configProperties = null;
    }

    public static void kMontior() {
        // This method is called from responder.WaitForMessage()
        now = System.currentTimeMillis();
        diff = Math.abs(now - last) / 1000;
        if (diff > 30) {
            kBatch();
            last = now;
        }
    }
}
