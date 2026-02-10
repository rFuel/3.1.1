package com.unilibre.kafka.dev;

import com.unilibre.commons.kafkaCommons;
import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kConsumer;
import com.unilibre.kafka.redundant.uDataLoader;

import java.util.ArrayList;

public class uSNowUpdater {

    private static boolean stopSW = false;
    private static String brokers = "";
    private static String topic = "";
    private static String cFile = "";

    public static void main(String[] args) {
        cFile = System.getProperty("conf", "uKafka.properties");
        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage("uSNowUpdater(): Will pull DB events FROM uStreams");
        uCommons.uSendMessage("        Using: " + cFile);
        uCommons.uSendMessage("===============================================================");

        int lCnt=0, fCnt=0;
        kConsumer.updates = new ArrayList<String>();
        uDataLoader.SetValues(cFile);
        if (kafkaCommons.KERROR) {
            System.exit(1);
        } else {
            uCommons.uSendMessage("   Using uStreams broker(s): [" + brokers + "]");
            uCommons.uSendMessage("   Using uStreams topic    : [" + topic + "]");
            uCommons.uSendMessage("===============================================================");
            uCommons.uSendMessage(" ");
        }

        EventConsumer();

    }

    private static void EventConsumer() {
        int lCnt=0, fCnt=0;
        while (!stopSW) {
            try {
                kConsumer.kConsume();
                if (kConsumer.didNothing) {
                    lCnt++;
                    if (lCnt > uDataLoader.heartbeat) {
                        kafkaCommons.Sleep(uDataLoader.pause);
                        uDataLoader.DisconnectUV();
                        uDataLoader.SetValues(cFile);
                        uCommons.uSendMessage("uDataLoader   <<Heartbeat>>");
                        lCnt = 0;
                    }
                } else {
                    lCnt = 0;

//                    BLAH BLAH BLAH here
//                    ProcessUpdates(kConsumer.updates);
                    kConsumer.updates.clear();
                }
            } catch (IllegalStateException ise) {
                fCnt++;
                uCommons.uSendMessage(fCnt+"  Cannot connect to Kafka broker(s). "+ise.getMessage());
                kafkaCommons.Sleep(5000);
                if (fCnt >= 10) {
                    uCommons.uSendMessage("Seems Kafka brokers are unavailable. Stopping now.");
                    stopSW = true;
                }
            }
        }
        kConsumer.Close();
        if (uDataLoader.uConnected) uDataLoader.DisconnectUV();
        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage("Master stop switch turned ON. Stopping now.");
        uCommons.uSendMessage("===============================================================");
        uCommons.uSendMessage(" ");
    }

}
