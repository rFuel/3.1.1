package com.unilibre.kafka.dev;

import com.unilibre.cipher.uCipher;
import com.unilibre.kafka.kConsumer;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Properties;

public class cKafkOra {

    private static String pid = ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0];
    private static String  BRK="", TOPIC="", passport, DB, SC, dbType;
    private static boolean ENC = false;
    private static long finishM=0, startM;
    private static double laps, div = 1000000000.00;
    private static Connection con = null;
    private static Statement logStmt = null;
    private static ResultSet logItems = null;
    private static ArrayList<String> DDL = new ArrayList<>();

    private static void StopNow (String reason) {
        System.out.println(ShowDateTime() + "  " + reason);
        kConsumer.Close();
        System.exit(1);
    }

    private static String GetDateTime() {
        String dts="";
        String mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
        String mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
        String mSecs = String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND));
        dts = mDate+" "+mTime+"."+(mSecs+"000").substring(0,4);
        mDate="";
        mTime="";
        mSecs="";
        return dts;
    }

    private static String ShowDateTime() {
        String ans = GetDateTime();
        return ans.substring(0, ans.indexOf("."));
    }

    private static Properties LoadProps(String cfile) {
        Properties props = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(cfile);
            props.load(is);
            is.close();
            is = null;
        } catch (FileNotFoundException e) {
            StopNow(e.getMessage());
        } catch (IOException e) {
            StopNow(e.getMessage());
        }
        return props;
    }

    private static void SetValues(String cfile) {
        Properties props = LoadProps(cfile);
        BRK = props.getProperty("broker", "");
        TOPIC=props.getProperty("stream", "");
        ENC = props.getProperty("encrypt", "").toLowerCase().equals("true");
        DB  = props.getProperty("tgtdbs", "");
        SC  = props.getProperty("tgtsch", "");
        if (DB.equals("") || SC.equals("")) StopNow("Database or Schema definition missing in config file");
        String URL = props.getProperty("tgturl", "");
        String USR = props.getProperty("tgtusr", "");
        String PWD = props.getProperty("tgtpwd", "");
        String DVR = props.getProperty("tgtdvr", "");

        try {
            System.out.println(ShowDateTime() + "  Loading Class for Driver " + DVR);
            Class.forName(DVR);
            System.out.println(ShowDateTime() + "  Establish JDBC connection to " + URL);
            con = DriverManager.getConnection(URL, USR, PWD);
        } catch (ClassNotFoundException e) {
            StopNow(e.getMessage());
        } catch (SQLException throwables) {
            StopNow(throwables.getMessage());
        }
        if (DVR.toLowerCase().contains("oracle")) dbType = "ORACLE";
        if (DVR.toLowerCase().contains("microsoft")) dbType = "MSSQL";
        kConsumer.SetBroker(BRK);
        kConsumer.SetTopic(TOPIC);
        kConsumer.pause = 100;
        kConsumer.SetClientID("Consumer:PID:"+pid);
        kConsumer.SetGroup("Group:for: Consumer:PID:"+pid);
        kConsumer.MAX_NO_MESSAGE_FOUND_COUNT=1000;
    }

    private static boolean CheckStop(String cfile) {
        Properties props = LoadProps(cfile);
        boolean ans = !props.getProperty("stop", "").toLowerCase().equals("true");
        return ans;
    }

    private static String GetPassport(ArrayList<String> keys, ArrayList<String> vals) {
        passport="";
        if (keys.indexOf("passport")>=0) passport = vals.get(keys.indexOf("passport"));
        return passport;
    }

    private static String GetItem(String key, ArrayList<String> keys, ArrayList<String> vals) {
        String ans="";
        if (keys.indexOf(key)>=0) ans = vals.get(keys.indexOf(key));
        if (!passport.equals("")) {
            ans = uCipher.v2UnScramble(uCipher.keyBoard, ans, passport);
        }
        return ans;
    }

    private static String StringifyCols(String[] clist) {
        String ans="";
        for (int i=0 ; i<clist.length ; i++) {
            ans += ", " + clist[i];
        }
        ans = ans.substring(2, ans.length());
        return ans;
    }

    private static String StringifyData(String[] dlist) {
        String ans="", var;
        for (int i=0 ; i<dlist.length ; i++) {
            var = dlist[i];
            if (var.contains("'")) var = var.replaceAll("\\'", "''");
            ans += ", '" +var + "'";
        }
        var = "";
        ans = ans.substring(2, ans.length());
        return ans;
    }

    private static void ProcessUpdates(ArrayList<String> updates) {
        startM = System.nanoTime();
        String src, sch, tbl, colStr, datStr, payload, dbitem;
        String sqlInsert = "INSERT INTO ! (#) VALUES ($)";
        String sqlCmd="";
        DDL.clear();
//        PreparedStatement ps=null;
//        try {
//            String prepSql = "INSERT INTO ? (?) VALUES (?)";
//            ps = con.prepareStatement(prepSql);
//        } catch (SQLException throwables) {
//            StopNow(throwables.getMessage());
//        }
        String[] cList;
        String[] dList;
        ArrayList<String> keys = new ArrayList<>();
        ArrayList<String> vals = new ArrayList<>();
        int updCnt=0;
        for (int i=0 ; i < updates.size() ; i++) {
            payload = updates.get(i);
            JSONObject obj = new JSONObject(payload);
            if (obj instanceof JSONObject) {
                Iterator<String> Keys = obj.keys();
                while (Keys.hasNext()) {
                    String key = Keys.next();
                    vals.add(obj.getString(key));
                    keys.add(key.toLowerCase());
                }
                Keys=null;
            }
            passport = GetPassport(keys, vals);

            src   = GetItem("source", keys, vals);
            sch   = GetItem("schema", keys, vals);
            tbl   = GetItem("table", keys, vals);
            dbitem= "ERROR";
            switch (dbType) {
                case "ORACLE":
                    dbitem = sch + "." + tbl;
                    break;
                case "MSSQL":
                    dbitem = "[" + DB + "].[" + SC + "].[" + tbl + "]";
                    break;
                default:
                    StopNow("Unfamiliar database type.");
            }
            colStr= GetItem("columns", keys, vals);
            if (colStr.equals("")) continue;
            datStr= GetItem("data", keys, vals);
            if (datStr.equals("")) continue;
            cList = colStr.split("<ft>");
            dList = datStr.split("<ft>");
            colStr= StringifyCols(cList);
            datStr= StringifyData(dList);

            sqlCmd = sqlInsert.replace("!", dbitem);
            sqlCmd = sqlCmd.replace("#", colStr);
            sqlCmd = sqlCmd.replace("$", datStr);
            DDL.add(sqlCmd);
//            try {
//                ps.setString(1, dbitem);
//                ps.setString(2, colStr);
//                ps.setString(3, datStr);
//                ps.addBatch();
//                updCnt++;
//            } catch (SQLException throwables) {
//                StopNow(throwables.getMessage());
//            }
            obj=null;
            keys.clear();
            vals.clear();
        }

        if (DDL.size() > 0) {
            try {
                Statement st = con.createStatement();
                for (int i=0 ; i < DDL.size() ; i++) { st.executeUpdate(DDL.get(i)); }
                st.close();
                st=null;
                finishM = System.nanoTime();
                laps = (finishM - startM) / div;
                System.out.println(ShowDateTime() + "  Batch of " + DDL.size() + " rows inserted in " + laps + " seconds");
            } catch (SQLException throwables) {
                StopNow(throwables.getMessage());
            }
            DDL.clear();
        }

//        try {
//            if (updCnt > 0) {
//                ps.executeBatch();
//                con.commit();
//                ps.clearParameters();
//                finishM = System.nanoTime();
//                laps = (finishM - startM) / div;
//                System.out.println(ShowDateTime() + "  Batch of " + updCnt + " rows inserted in " + laps + " seconds");
//                updCnt=0;
//            }
//        } catch (SQLException throwables) {
//            StopNow(throwables.getMessage());
//        }
    }

    public static void main(String[] args) {
        String cFile = System.getProperty("conf", "kafkora.properties");
        System.out.println(ShowDateTime() + "  ===============================================================");
        SetValues(cFile);
        System.out.println(ShowDateTime() + "  Target DB Connected on PID " + pid);
        System.out.println(ShowDateTime() + "  ===============================================================");
        System.out.println(ShowDateTime() + "  cKafkOra(): Will consume database deltas FROM Kafka streams");
        System.out.println(ShowDateTime() + "  Using Config file    : [" + cFile + "]");
        System.out.println(ShowDateTime() + "  Using Kafka broker(s): [" + BRK + "]");
        System.out.println(ShowDateTime() + "  Using Kafka topic    : [" + TOPIC + "]");
        System.out.println(ShowDateTime() + "  ===============================================================");
        System.out.println(ShowDateTime() + " ");
        int tcnt=0, nodata=0;
        kConsumer.updates = new ArrayList<String>();

        boolean okay = CheckStop(cFile);
        while (okay) {
            kConsumer.kConsume();
            if (kConsumer.didNothing) {
                nodata++;
                if (nodata > 100) {
                    System.out.println(ShowDateTime() + "  Heartbeat");
                    nodata = 0;
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    StopNow(e.getMessage());
                }
            } else {
                tcnt = 0;
                ProcessUpdates(kConsumer.updates);
                kConsumer.updates.clear();
            }
            okay = CheckStop(cFile);
        }
        kConsumer.Close();
        System.out.println(ShowDateTime() + " ");
        System.out.println(ShowDateTime() + "  ===============================================================");
        System.out.println(ShowDateTime() + "  Master stop switch turned ON. Stopping now.");
        System.out.println(ShowDateTime() + "  ===============================================================");
        System.out.println(ShowDateTime() + " ");
    }

}
