package com.unilibre.kafka.dev;

import com.unilibre.commons.uCommons;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class cRebalanceListener implements ConsumerRebalanceListener {

    private KafkaConsumer consumer;

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public cRebalanceListener(KafkaConsumer cons) {this.consumer = cons;}

    public void addOffset(String topic, int part, long offset) {
        currentOffsets.put(new TopicPartition(topic, part), new OffsetAndMetadata(offset, "commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        uCommons.uSendMessage("onPartitionsRevoked() - rebalancing offsets.");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        consumer.commitSync(currentOffsets);
        uCommons.uSendMessage("onPartitionsAssigned() - clearing offsets.");
        currentOffsets.clear();
    }
}
