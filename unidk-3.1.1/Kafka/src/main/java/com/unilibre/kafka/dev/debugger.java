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

public class debugger {

    public static void main(String[] args) {
        dbgDirect();
        System.exit(1);
    }

    public static void ksMainDBG() {
        ksMain ksm = new ksMain();
        ksm.setAPPID("MyDebugger");
        ksm.setBROKERS("kafka2:9092");
        ksm.setInpTOPIC("upl-dev-025-v1");
        ksm.setOutTOPIC("upl-dev-025-v2");
        ksm.CreateStream();
        ksm.Start();
    }

    public static void dbgDirect() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "MyDebugger");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka2:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("upl-dev-025-v1");
        source.flatMapValues(value -> Arrays.asList(value.split("\\W+"))).to("upl-dev-025-v3");

        final Topology topology = builder.build();
        final KafkaStreams kstream = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kstream.close();
                latch.countDown();
            }
        });

        try {
            kstream.start();
            latch.await();
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }

    }
}
