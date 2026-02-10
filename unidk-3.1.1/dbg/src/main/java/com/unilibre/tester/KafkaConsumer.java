package com.unilibre.tester.tester;

import com.unilibre.commons.GarbageCollector;
import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kConsumer;
import java.util.Date;

public class KafkaConsumer {

    private static final int MAX_TRIES = 100;
    private static int thisID=0;

    public static void main(String[] args) {
        // ------------------------------------[ Notes ] ----------------------------------------------
        // It will only consume when;
        //  1.  it has read_committed messages in the topic
        //  2.  these messages have a LAG greater than the current-offset
        //  3.  these messages are less than the log-end-offset (others are uncommitted)
        //
        // kdrescribe {topic}
        // TOPIC    PARTITION  CURRENT-OFFSET   LOG-END-OFFSET  LAG     CONSUMER-ID         HOST            CLIENT-ID
        // myTopic  0          0                250             250     ConsTest-0-[blah]   192.168.48.1    ConsTest-0
        //
        // Must kill ALL consumers to the group BEFORE krewind can work !!!
        //
        // "krewind" {topic} with no warnings will set the current-offset to 0
        //
        // NB: kafka admin commands are NOT instantaneous - you have to wait a minute or two
        // --------------------------------------------------------------------------------------------

        Connect2Kafka();
        if (kConsumer.GetConsumer() == null) System.exit(0);

        GarbageCollector.setStart(System.nanoTime());
        GarbageCollector.setCollection(30);

        int tries=0;
        while (true) {
            kConsumer.kConsume();
            if (kConsumer.didNothing) {
                tries++;
                if (tries >= MAX_TRIES) {
                    SendMessage(" ");
                    SendMessage("******************************************************");
                    SendMessage("Inactive consumer : resetting connection.");
                    SendMessage("******************************************************");
                    SendMessage(" ");
                    kConsumer.Close();
                    GarbageCollector.CleanUp();
                    thisID++;
                    Connect2Kafka();
                    Sleep(5);
                    kConsumer.kConnect();
                    tries=0;
                } else {
                    Sleep(1);
                }
            }  else {
                tries = 0;
                SendMessage(kConsumer.updates.size() + " logs read.");
            }
        }
    }

    private static void Connect2Kafka() {
        kConsumer.SetBroker("192.168.48.135:9092");
        kConsumer.SetTopic("DeltaLog");
        kConsumer.SetClientID("ConsTest-" + thisID);
        kConsumer.SetGroup("uStreams");
        kConsumer.SetSASL(false, "", "", "");
        kConsumer.kConnect();
    }

    private static void SendMessage( String msg ) {
//        Date rightnow;
        String rightnow;
        rightnow = uCommons.GetLocaltimeStamp(); // new Date();
        System.out.println(rightnow + "  " + msg);
        rightnow = null;
    }

    private static void Sleep(int sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            // blah blah
        }
    }
}
