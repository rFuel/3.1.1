package com.unilibre.kafka;

import com.unilibre.commons.kafkaCommons;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class kConsumer {

    private static final String FALSE="false", READ_COMMITTED="read_committed";

    // redundant for now but may use again, leaving here rather than git hunting later.
    private static String OFFSET_RESET_LATEST = "latest";
    private static int PARTITION_COUNT = 50;
    private static int MESSAGE_PARTITION = 0;
    private static ArrayList<String> topics = new ArrayList<>();
    private static ArrayList<String> partitions = new ArrayList<>();

    private static Integer TIMEOUT_FACTOR = 1000;
    private static String ACKS = "all"; // Strongest setting for message acknowledgement in producers
    private static boolean kCommitted=true, firstSW=true, silent=false;
    private static Properties configProperties;
    private static KafkaConsumer<String, String> kafkaConsumer;
//    public static Consumer<Long, String> consumer;
    private static String OFFSET_RESET_EARLIER = "earliest";
    private static boolean sasl = false, topicExists=false;
    private static String saslUser="", saslPass="",saslKey="", saslAWS  = "", saslAzure= "";
    private static String brokers = "";
    private static String TOPIC_NAME = "";
    private static String CLIENT_ID;
    private static String GROUP_ID_CONFIG;

    public static String[] tCols;
    public static int pause = 250;
    public static Integer MAX_POLL_RECORDS = 10;
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static boolean didNothing = true;
    public static ArrayList<String> updates;
    public static ArrayList<String> offsets = new ArrayList<>();

    public static String GetBrokers() { return brokers; }

    public static KafkaConsumer GetConsumer() { return kafkaConsumer;}

    public static void SetBroker(String inval) { brokers = inval; }

    public static void SetTopic(String inval) { TOPIC_NAME = inval; }

    public static void SetClientID(String inval) { CLIENT_ID = inval; }

    public static void SetSASL(boolean inval, String u, String p, String k) {
        saslUser = "";
        saslPass = "";
        saslKey  = "";      // Azure ONLY !!!
        saslAWS  = "";
        saslAzure= "";
        sasl = inval;
        if (sasl) {
            saslUser = u;
            saslPass = p;
            saslKey  = k;
            saslAzure= "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://drlcevents.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey="+saslKey+"\";";
            saslAWS  = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+saslUser+"\" password=\""+saslPass+"\";";
        }
    }

    public static void SetGroup(String inval) { GROUP_ID_CONFIG = inval; }

    public static void SetSilent(boolean inval) { silent = inval; }

    public static KafkaConsumer kConnect() {

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

        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        configProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        configProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (MAX_POLL_RECORDS * TIMEOUT_FACTOR));
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);
        configProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_COMMITTED);
        configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, FALSE);

        kafkaConsumer = new KafkaConsumer<>(configProperties, new StringDeserializer(), new StringDeserializer());
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        try {
            Map<String, List<PartitionInfo>> kMap = kafkaConsumer.listTopics();
            kMap.clear();
            kMap = null;
            if (!silent) {
                System.out.println("Consumer has been created and is subscribed to: " + TOPIC_NAME);
            }
        } catch (TimeoutException te) {
            System.out.println();
            System.out.println("   ***********************************************************");
            System.out.println("   - Kafka brokers are not running on: ["+brokers+"] !");
            System.out.println("   ***********************************************************");
            System.out.println();
            kafkaConsumer = null;
        }

        offsets.add("");
        updates = new ArrayList<>();
        return kafkaConsumer;
    }

    public static ArrayList<String> kConsume() {
        if (kafkaConsumer == null) kafkaConsumer = kConnect();
        didNothing = true;
        updates.clear();
        ConsumerRecords<String, String> records = kafkaConsumer.poll(MAX_POLL_RECORDS);
        String value;   // don't grab the key - the data is in the value.
        for (ConsumerRecord<String, String> record : records) {
            value = record.value().toString();
            updates.add(value);
        }
        if (updates.size() > 0) didNothing = false;
        value=null;
        return updates;
    }

    public static boolean Commit() {
        try {
            if (kafkaConsumer != null) kafkaConsumer.commitSync();
            kCommitted = true;
        } catch (CommitFailedException cfe) {
            // this is 'usually' caused by too long a time ... > MAX_POLL_INTERVAL_MS_CONFIG
            // will get this when debugging so set TIMEOUT_FACTOR extra large.
            kafkaCommons.uSendMessage("RFUEL: kafkaConsumer.CommitFailedException(cfe) ERROR: " + cfe.getMessage());
            kCommitted = false;
        } catch (RetriableCommitFailedException rcf) {
            kafkaCommons.uSendMessage("RFUEL: kafkaConsumer.RetriableCommitFailedException(rcf) ERROR: " + rcf.getMessage());
            kCommitted = false;
        } catch (DisconnectException dex) {
            kafkaCommons.uSendMessage("RFUEL: kafkaConsumer.DisconnectException(dex) ERROR: "+dex.getMessage());
            // the commitAsync is a hail Mary trying to recover. It may throw exceptions!
            // can commitAsync but Async in Kafka is scary !
//            if (kafkaConsumer != null) kafkaConsumer.commitAsync();
//            kCommitted = true;
            kCommitted = false;
        }
        return kCommitted;
    }

    public static void Close() {
        if (kafkaConsumer == null) return;
        if (!kCommitted) Commit();
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
        kafkaConsumer = null;
    }

}
