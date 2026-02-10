package com.unilibre.kafka.dev;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ksMain {

    final CountDownLatch latch = new CountDownLatch(1);

    private String BROKERS;
    private String APPID;
    private String inpTOPIC;
    private String outTOPIC;
    private Topology topology;
    private Properties ksConfig;
    private KafkaStreams ioStream;
    private KStream<String, String> source;

    public void setAPPID(String inval) {this.APPID = inval;}

    public void setBROKERS(String inval) {this.BROKERS = inval;}

    public void setInpTOPIC(String inval) {this.inpTOPIC = inval;}

    public void setOutTOPIC(String inval) {this.outTOPIC = inval;}

    public boolean CreateStream() {
        ksConfig = new Properties();
        ksConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        ksConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APPID);
        ksConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        ksConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        // simply copies everything from inp to out
//        builder.stream(inpTOPIC).to(outTOPIC);

        this.source = builder.stream(inpTOPIC);
        KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
        words.to(outTOPIC);

        this.topology = builder.build();
        this.ioStream = new KafkaStreams(topology, ksConfig);

        System.out.println(topology.describe());
        builder  = null;
        ksConfig = null;
        return true;
    }

    public boolean Start() {
        Runtime.getRuntime().addShutdownHook(new Thread("ksMain-shutdown-hook") {
            @Override
            public void run() {
                ioStream.close();
                latch.countDown();
            }
        });
        try {
            ioStream.start();
            latch.await();
            return true;
        } catch (Throwable e) {
            System.out.println(e.getMessage());
        }
        return false;
    }
}
