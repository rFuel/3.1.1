package com.unilibre.gui;


import org.apache.log4j.MDC;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class mdcLogger {

    private  String date, time, message;

    public void SetDate(String inVal) { this.date = inVal; }

    public void SetTime(String inVal) { this.time = inVal; }

    public void SetMessage(String inVal) { this.message = inVal; }

    public void NewEvent() {
        String mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
        String mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
        int   ThisMS = Calendar.getInstance().get(Calendar.MILLISECOND);
        String  MSec = "." + (ThisMS + "000").substring(0, 3);
        SetDate(mDate);
        SetTime(mTime);
    }

    public void LogEvent() {
        MDC.put("Date", date);
        MDC.put("Time", time);
        MDC.put("Info", message);
    }

    public void Clear() {
        MDC.clear();
    }


}
