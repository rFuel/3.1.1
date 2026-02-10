package com.unilibre.tester.tester;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class OBDateFilter {

    public static void main(String[] args) {

        // Coding for re-use
        // -------------------------------------------------
        //      json string         :   jsonStr
        //      primary field-name  :   data
        //      json array          :   transactions
        //      array field-name    :   executionDateTime
        //      date boundary       :   sixMonthsAgo
        // -------------------------------------------------
        // public static String JsonDateFilter(String jsonStr, String primaryField, String arrayField, String arrayItem)
        // usage example ...
        // String newJstring = JsonDateFilter(jString, "data", "transactions", "executionDateTime", -6);

        long MAGIC = 86400000L;
        Calendar today = Calendar.getInstance();
        int sixMonthsAgo = -6;
        today.add(Calendar.MONTH, sixMonthsAgo);
        Date checkDate = today.getTime();
        long backThen = checkDate.getTime();
        int earliestDate = (int) (backThen / MAGIC);
        int trxDate;

        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");

        // normally, you would pass the jsonStr into here via a method call
        // edit some dates in the executionDateTime field to test the date-range include / exclude

        String jsonStr = "{\"--comment\":\"GetTransactions(100001) Objects\",\"data\":{\"transactions\":[{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:18:48\",\"valueDateTime\":\"02-01-2005 06:16:12\",\"executionDateTime\":\"02-04-2020 06:18:48\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"38**1\",\"merchantCategoryCode\":\"38**1\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:18:40\",\"valueDateTime\":\"02-01-2005 06:13:27\",\"executionDateTime\":\"02-07-2020 06:18:40\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"19**28\",\"merchantCategoryCode\":\"19**28\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:15:45\",\"valueDateTime\":\"02-01-2005 06:13:22\",\"executionDateTime\":\"02-01-2005 06:15:45\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"43**12\",\"merchantCategoryCode\":\"43**12\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:15:06\",\"valueDateTime\":\"02-01-2005 06:10:37\",\"executionDateTime\":\"02-01-2005 06:15:06\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"17**93\",\"merchantCategoryCode\":\"17**93\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:11:47\",\"valueDateTime\":\"02-01-2005 06:08:15\",\"executionDateTime\":\"02-01-2005 06:11:47\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"34**25\",\"merchantCategoryCode\":\"34**25\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:10:45\",\"valueDateTime\":\"02-01-2005 06:07:33\",\"executionDateTime\":\"02-01-2005 06:10:45\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"27**64\",\"merchantCategoryCode\":\"27**64\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:08:30\",\"valueDateTime\":\"02-01-2005 06:06:51\",\"executionDateTime\":\"02-01-2005 06:08:30\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"63**1\",\"merchantCategoryCode\":\"63**1\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:06:06\",\"valueDateTime\":\"02-01-2005 06:05:09\",\"executionDateTime\":\"02-01-2005 06:06:06\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"40**50\",\"merchantCategoryCode\":\"40**50\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:07:28\",\"valueDateTime\":\"02-01-2005 06:03:30\",\"executionDateTime\":\"02-01-2005 06:07:28\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"39**20\",\"merchantCategoryCode\":\"39**20\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:05:26\",\"valueDateTime\":\"02-01-2005 06:01:32\",\"executionDateTime\":\"02-01-2005 06:05:26\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"22**13\",\"merchantCategoryCode\":\"22**13\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:04:36\",\"valueDateTime\":\"02-01-2005 06:00:26\",\"executionDateTime\":\"02-01-2005 06:04:36\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"33**7\",\"merchantCategoryCode\":\"33**7\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001L57\",\"transactionId\":\"100001L57\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"02-01-2005 06:03:47\",\"valueDateTime\":\"02-01-2005 06:00:00\",\"executionDateTime\":\"02-01-2005 06:03:47\",\"amount\":\"234.66\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"18**72\",\"merchantCategoryCode\":\"18**72\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:51\",\"valueDateTime\":\"10-10-2004 06:02:09\",\"executionDateTime\":\"10-10-2004 06:02:51\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"27**92\",\"merchantCategoryCode\":\"27**92\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{\"accountId\":\"100001S31\",\"transactionId\":\"100001S31\",\"DorC\":\"C\",\"isDetailAvailable\":true,\"type\":\"FEE\",\"status\":\"PENDING\",\"description\":\"\",\"postingDateTime\":\"10-10-2004 06:02:03\",\"valueDateTime\":\"10-10-2004 06:00:00\",\"executionDateTime\":\"10-10-2004 06:02:03\",\"amount\":\"419.14\",\"currency\":\"AUD\",\"reference\":\"CDR\",\"merchantName\":\"25**98\",\"merchantCategoryCode\":\"25**98\",\"billerCode\":\"BILLER_CODE\",\"billerName\":\"BILLER_NAME\",\"crn\":\"CRN\",\"apcaNumber\":\"APCA\"},{}]},\"links\":{\"self\":\"\",\"first\":\"\",\"prev\":\"\",\"next\":\"\",\"last\":\"\"},\"meta\":{\"totalRecords\":0,\"totalPages\":0}}";
        
        try {
            // get the root object
            JSONObject rootObj = new JSONObject(jsonStr);

            // get the "data" element from the root object
            String data = rootObj.getString("data");
            JSONObject dataObj = new JSONObject(data);

            // get the "transactions" array from the data element
            JSONArray tranArray = dataObj.getJSONArray("transactions");
            JSONArray newArray  = new JSONArray();

            String dte, msg;
            Date tranDate;
            JSONObject arrElement;
            int endOfList = tranArray.length();

            for (int i = 0; i < endOfList; ++i) {
                arrElement = tranArray.getJSONObject(i);
                if (arrElement.names() == null) continue;

                dte = arrElement.getString("executionDateTime");
                dte = FieldOf(dte, " ", 1);
                tranDate = sdf.parse(dte);
                backThen = tranDate.getTime();
                trxDate  =  (int) (backThen / MAGIC);

                // println some stuff to see what's happening
                if (trxDate < earliestDate) {
                    msg = "not in range";
                } else {
                    msg = "    in range";
                    newArray.put(arrElement);
                }
                System.out.println(dte + "  " + msg);
            }
            arrElement = null;
            tranDate = null;

            // tranArray has ALL transactions returned by rFuel
            // newArray  has ONLY those transaction in the date range

            // replace the tranArray inside the data element
            dataObj.remove("transactions");
            dataObj.put("transactions", newArray);

            // now replace the data element inside the root object
            rootObj.remove("data");
            rootObj.put("data", dataObj);

            System.out.println(rootObj.toString());

            newArray  = null;
            tranArray = null;
            dataObj = null;
            rootObj = null;
        } catch (JSONException | ParseException e) {
            System.out.println(" ");
            System.out.println(e.getMessage());
            System.out.println(" ");
        }
        System.exit(0);
    }

    private static String FieldOf(String var, String chr, int occ) {
        String answer="";
        String[] tmpStr = var.split(chr);
        if (occ <= tmpStr.length) answer = tmpStr[occ - 1];
        tmpStr = null;
        return answer;
    }
}
