package com.unilibre.runcontrol;

/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import com.unilibre.commons.MessageInOut;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import com.unilibre.core.coreCommons;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

public class HeartBeat {

    private static String writerMsg = "055Heartbeat.msv";
    private static String writerQue = "055_writer_001";
    private static String readerMsg = "050Heartbeat.map";
    private static String readerQue = "050_reader_001";
    private static String payloadTmp = "<InBound><BaseData><ID>$id$</ID><SessionID>$session$</SessionID><UsedBy>UniLibre</UsedBy><Source>HeartBeat</Source><Date>$date$</Date><Time>$time$</Time></BaseData></InBound>";
    private static int interval = 60;
    private static int counter  = 0;
    private static String shost;
    private static MessageInOut mio = new MessageInOut();

    private static void SendWriter() {
        counter++;
        String mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
        String mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
        String session = NamedCommon.pid+"-"+counter;
        String payload = payloadTmp.replace("$id$", NamedCommon.pid);
        payload = payload.replace("$session$", session);
        payload = payload.replace("$date$", mDate);
        payload = payload.replace("$time$", mTime);
        String corrid = "HeartBeat-"+counter;
        String message = "task<is>055<tm>shost<is>"+shost+"<tm>" +
                "<tm>format<is>json<tm>Correlationid<is>"+corrid+"<tm>" +
                "replyto<is>HeartBeat<tm>mscat<is>UPLQA/"+writerMsg+"<tm>" +
                "payload<is>" + payload;
        uCommons.uSendMessage(message);
        uCommons.MessageToAPI(message);
        String answer = mio.SendAndReceive(NamedCommon.messageBrokerUrl, writerQue, "HeartBeat", corrid, message, "HeartBeat");
        System.out.println(answer);
    }


    private static void SendReader() {
        counter++;
        String mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
        String mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
        String session = NamedCommon.pid+"-"+counter;
        String corrid = "HeartBeat-"+counter;
        String message = "task<is>050<tm>shost<is>"+shost+"<tm>" +
                "<tm>format<is>json<tm>Correlationid<is>"+corrid+"<tm>" +
                "replyto<is>HeartBeat<tm>map<is>UPLQA/uRest/"+readerMsg+"<tm>item<is>"+NamedCommon.pid+"<tm>";
        uCommons.uSendMessage(message);
        uCommons.MessageToAPI(message);
        String answer = mio.SendAndReceive(NamedCommon.messageBrokerUrl, readerQue, "HeartBeat",  corrid, message, "HeartBeat");
        System.out.println(answer);
    }


    public static void main(String[] args) {
        if (NamedCommon.BaseCamp.contains("/home/andy")) NamedCommon.BaseCamp = NamedCommon.DevCentre;
        NamedCommon.Broker = "heartbeat.bkr";
        Properties Props = uCommons.LoadProperties(NamedCommon.Broker);
        uCommons.BkrCommons(Props);
//        NamedCommon.messageBrokerUrl = Props.getProperty("url", "");
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        shost = "data-uv01"; // runProps.getProperty("u2host", "ERROR");
        boolean stopNow = coreCommons.StopNow();
        while (!stopNow) {
            uCommons.uSendMessage("******************************************");
            uCommons.uSendMessage("*");
            SendWriter();
            uCommons.Sleep(1);
            SendReader();
            uCommons.uSendMessage("*");
            stopNow = coreCommons.StopNow();
            if (!stopNow) uCommons.Sleep(interval);
            NamedCommon.MQgarbo.gc();
        }
    }

}
