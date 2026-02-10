package com.unilibre.webDS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class logger {

    private static Logger logger = LoggerFactory.getLogger(logger.class);

    public static void uSendMessage(String inMsg) {
        if (inMsg == null) return;
        logthis(inMsg);
    }

    public static void logthis(String output) {
        int ThisMS = Calendar.getInstance().get(Calendar.MILLISECOND);
        String MSec = (ThisMS + "000").substring(0, 4);
        String mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
        output = ("[" + mTime + "." + MSec + "] " + output);
        logger.info(output);
    }
}
