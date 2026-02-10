package com.unilibre.confluent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class cProducer {

    private Properties props = null;
    private Producer<String, String> producer = null;

    public static Properties loadConfig(final String configFile) {
        if (!Files.exists(Paths.get(configFile))) {
            System.out.println(configFile + " not found.");
        }
        final Properties cfg = new Properties();

        try {
            InputStream inputStream = new FileInputStream(configFile);
            cfg.load(inputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cfg;
    }

    public boolean Config(String cfgFile) {
        props = loadConfig(cfgFile);
        if(props ==null) return false;
        return true;
    }

    public boolean CreateProducer() {
        try {
            producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public void CloseProducer() {
        producer.close();
    }

    public boolean SendTest(String topic, String key, String value) {
        try {
            producer.send(new ProducerRecord<String, String>(topic, key, value));
        } catch (Exception e) {
            return false;
        }
        return true;
    }

}
