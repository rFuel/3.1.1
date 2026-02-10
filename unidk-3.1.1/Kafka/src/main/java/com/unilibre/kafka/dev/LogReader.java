package com.unilibre.kafka.dev;


import com.unilibre.cipher.uCipher;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.kafkaCommons;
import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kConsumer;
import org.json.JSONObject;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
//import static com.unilibre.kafka.commons.LoadProperties;
//import static com.unilibre.kafka.commons.uSendMessage;

public class LogReader {

    private static boolean encrypt = false;

    public static void main(String[] args) {
        if (!kafkaCommons.isLicenced()) System.exit(1);

        String pid = ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0];
        String cfile = System.getProperty("conf", "ksUplift.properties");
        Properties  props = kafkaCommons.LoadProperties(cfile);
        String brokers   = props.getProperty("brokers", "");
        String topic     = props.getProperty("topic", "unknown");
        String group     = props.getProperty("group", "unknown");
        String mTemplate = props.getProperty("message", "");

        encrypt = props.getProperty("encrypt", "unknown").toLowerCase().equals("true");

        kConsumer.updates = new ArrayList<String>();
        kConsumer.SetBroker(brokers);
        kConsumer.SetTopic(topic);
        kConsumer.SetGroup(group);
        kConsumer.SetClientID("LogReader:"+pid);
        kConsumer.pause = 5;

        kafkaCommons.uSendMessage("===============================================================");
        kafkaCommons.uSendMessage("Connecting to Kafka ...");
        kConsumer.kConnect();
//        kConsumer.consumer = kConsumer.kConnect();
        kafkaCommons.uSendMessage("===============================================================");
        ArrayList<String> kItems = new ArrayList<String>();
        boolean stopSW = false;
        String passport="", record="", encStr="", answer="", resp="", inst="", acct="", file="";
        String issuer="", message="", itemId="", timedate="", logid="", payload="", encSeed;
        Scanner sc = new Scanner(System.in);

        while (!stopSW) {
            kItems.clear();
            kItems = kConsumer.kConsume();
            if (!kConsumer.didNothing) {
                for (int i=0 ; i < kItems.size() ; i++) {
                    System.out.println(" ");
                    System.out.println(" ");
                    JSONObject jso = new JSONObject(kItems.get(i));
                    inst     = jso.getString("sourceinstance");
                    acct     = jso.getString("sourceaccount");
                    file     = jso.getString("file");
                    passport = jso.getString("passport");
                    record   = jso.getString("record");

                    itemId   = jso.getString("item");
                    timedate = jso.getString("time") + jso.getString("date");
                    logid    = inst;

                    payload   = acct + NamedCommon.IMark + file + NamedCommon.IMark + itemId + NamedCommon.IMark + record;

                    if (encrypt) {
                        encSeed = uCommons.FieldOf(passport, "~", 2);
                        encStr  = record + "~" + encSeed;
                        answer  = uCipher.Decrypt(encStr);
                        encSeed = "";
                    } else {
                        encStr  = record;
                        answer  = encStr;
                    }

                    message     = mTemplate.replace("$map$", file);
                    message     = message.replace("$passport$", passport);
                    message     = message.replace("$issuer$", issuer);
                    message     = message.replace("$item$", itemId);
                    message     = message.replace("$dacct$", acct);
                    message     = message.replace("$loadts$", timedate);
                    message     = message.replace("$logid$", logid);
                    message     = message.replace("$record$", payload);

                    passport = "";
                    record   = "";
                    encStr   = "";
                    System.out.println("  Event : " + kItems.get(i));
                    System.out.println(" ");
                    System.out.println("Message : " + message);
                    System.out.println(" ");
                    System.out.println("Instance: " + inst);
                    System.out.println("Account : " + acct);
                    System.out.println("File    : " + file);
                    System.out.println("Record  : " + answer);
                    System.out.print("Press C to continue or Q to quit : ");
                    resp = sc.nextLine();
                    if (resp.toLowerCase().equals("q")) break;
                    System.out.println("-------------------------------------------------------------------------------");
                }
                stopSW = true;
            } else {
                uCommons.Sleep(1);
            }
        }
    }
}
