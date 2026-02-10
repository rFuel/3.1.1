package com.unilibre.kafka;

import com.unilibre.commons.kafkaCommons;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.*;

import java.util.Date;
import java.util.Properties;

public class kafkaServices {

    private final String SER_STRINGS = StringSerializer.class.getName();        // Serdes.String().getClass().getName();
    private final String DES_STRINGS = StringDeserializer.class.getName();      // Serdes.String().getClass().getName();
    private final String SER_B_ARRAY = BytesSerializer.class.getName();         // Serdes.ByteArray().getClass().getName();
    private final String DES_B_ARRAY = BytesDeserializer.class.getName();       // Serdes.ByteArray().getClass().getName();

    private String saslAzure, saslAWS, keySerdes, valSerdes, keyType, valType, brokers, topic_name;
    private String GROUP_ID_CONFIG, CLIENT_ID, acks, retries, processor;
    private int batchSize, TransID, maxBlockingMS, proccnt;
    private boolean verbose, batching, sasl, kCommitted;

    private Producer producer;
    private Consumer consumer;

    // =============================================================================================

    public Producer CreateProducer(String combo, String p) {

        processor = p;
        Properties props = Configure("producer");

        switch (combo) {
            case "STRING-STRING":
                producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
                break;
            case "STRING-BYTE":
                producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
                break;
            case "BYTE-STRING":
                producer = new KafkaProducer<>(props, new ByteArraySerializer(), new StringSerializer());
                break;
            case "BYTE-BYTE":
                producer = new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
                break;
            default:
                System.out.println(new Date() + " " + processor + " Producer cannot be created with : [" + combo + "] ");
                producer = null;
        }
        return producer;
    }

    public void CloseProducer(Producer producer) {
        if (producer == null) return;
        producer.flush();
        producer.close();
        producer = null;
        if (verbose) System.out.println(new Date() + " " + processor + " k-Producer closed  ****************");
    }

    public Consumer createConsumer(String combo, String p) {

        processor = p;
        Properties props = Configure("consumer");
        switch (combo) {
            case "STRING-STRING":
                consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
                break;
            case "STRING-BYTE":
                consumer = new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer());
                break;
            case "BYTE-STRING":
                consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new StringDeserializer());
                break;
            case "BYTE-BYTE":
                consumer = new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
                break;
            default:
                System.out.println(new Date() + " " + processor + " Consumer cannot be created with : [" + combo + "] ");
                consumer = null;
        }
        return consumer;
    }

    public void CloseConsumer(Consumer consumer) {
        if (consumer == null) return;
        if (!kCommitted) {
            try {
                if (consumer != null) consumer.commitSync();
                consumer.commitSync();
            } catch (KafkaException e) {
                kafkaCommons.uSendMessage("================================================================");
                kafkaCommons.uSendMessage("kConsumer.Close() could not commitSync().");
                kafkaCommons.uSendMessage("EXCEPTION: " + e.getMessage());
                kafkaCommons.uSendMessage("Probably committed by the last fetch operation.");
                kafkaCommons.uSendMessage("================================================================");
            }
        }
        try {
            if (consumer != null)   consumer.close();
        } catch (KafkaException e) {
            System.out.println("================================================================");
            System.out.println("Did not close connection to Kafka broker(s)");
            System.out.println(e.getMessage());
            System.out.println("================================================================");
        }
        consumer = null;

    }

    public Properties Configure(String type) {

        if (verbose) {
            System.out.println(new Date() + " " + processor + " Connecting to Kafka with ClientID " + CLIENT_ID + " in group " + GROUP_ID_CONFIG);
        }

        Properties configProperties = new Properties();
        Properties cProps = new Properties();
        Properties pProps = new Properties();

        if (sasl) {
            configProperties.put("security.protocol","SASL_SSL");
            configProperties.put("sasl.mechanism","PLAIN");
            if (!saslAWS.equals("")) {
                configProperties.put("sasl.jaas.config", saslAWS);
            }
            if (!saslAzure.equals(""))  {
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
            configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            configProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockingMS);
        }
        pProps = configProperties;
        cProps = configProperties;
        configProperties = null;
        String keyDeser = DES_STRINGS, valDeser = DES_STRINGS;
        if (keySerdes.equals(SER_B_ARRAY)) keyDeser = DES_B_ARRAY;
        if (valSerdes.equals(SER_B_ARRAY)) valDeser = DES_B_ARRAY;
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeser);
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDeser);

        type = type.toUpperCase();
        switch (type) {
            case "PRODUCER":
                configProperties = cProps;
                break;
            case "CONSUMER":
                configProperties = pProps;
                break;
            default:
                configProperties = null;
        }
        return configProperties;
    }

    // =============================================================================================

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
    public void SetSASL(boolean isSASL, String user, String pass, String key) {
        sasl = isSASL;
        if (user.equals("")) return;
        if (!key.equals("")) {
            saslAzure = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://drlcevents.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=" + key + "\";";
            saslAWS = "";
        } else {
            saslAWS = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + user + "\" password=\"" + pass + "\";";
            saslAzure = "";
        }
    }

    public void SetTopic(String inval) {
        topic_name = inval;
    }
    public void SetGroup(String inval) {
        GROUP_ID_CONFIG = inval.replaceAll("\\:", "_");
    }
    public void SetClientID(String inval) { CLIENT_ID = inval.replaceAll("\\:", "_"); }
    public void SetBatchSize(int inval) {batchSize = inval;}
    public void SetBatching(boolean inval) { batching = inval; }
    public void SetTransID(int inval) {TransID = inval;}
    public void SetBlockingMS(int inval) {maxBlockingMS = inval;}
    public void SetProcCnt(int inval) {proccnt = inval;}
    public void SetAcks(String inval) { acks = inval; }
    public void SetRetries(String inval) { retries = inval; }

}
