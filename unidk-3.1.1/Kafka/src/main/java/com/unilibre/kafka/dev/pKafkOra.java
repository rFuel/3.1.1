package com.unilibre.kafka.dev;


import com.unilibre.cipher.uCipher;
import com.unilibre.kafka.kProducer;
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
import java.util.Properties;

public class pKafkOra {

    private static String pid = ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0];
    private static Connection con = null;
    private static Statement logStmt = null;
    private static ResultSet logItems = null;
    private static int lastID=0, loadsize=100, loadwait=0;
    private static long finishM=0, startM;
    private static double laps, div = 1000000000.00;
    private static String SRC="", BRK="", TOPIC="", prevSelect="";
    private static boolean ENC = false;
    private static ArrayList<String> dbObject = new ArrayList<>();
    private static ArrayList<String> columns = new ArrayList<>();

    private static void StopNow (String reason) {
        System.out.println(reason);
        kProducer.Close();
        System.exit(1);
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

    private static void ConnectSource(String cfile) {
        Properties props = LoadProps(cfile);
        try {
            loadsize = Integer.valueOf(props.getProperty("loadsize"));
        } catch (NumberFormatException nfe) {
            loadsize = 100;
        }
        try {
            loadwait = Integer.valueOf(props.getProperty("loadwait"));
        } catch (NumberFormatException nfe) {
            loadwait = 5000;
        }
        SRC = props.getProperty("source", "");
        BRK = props.getProperty("broker", "");
        TOPIC=props.getProperty("stream", "");
        ENC = props.getProperty("encrypt", "").toLowerCase().equals("true");
        String URL = props.getProperty("srcurl", "");
        String USR = props.getProperty("srcusr", "");
        String PWD = props.getProperty("srcpwd", "");
        String DVR = props.getProperty("srcdvr", "");

        try {
            System.out.println(ShowDateTime() + "  Loading Class for Driver " + DVR);
            Class.forName(DVR);
            System.out.println(ShowDateTime() + "  Establish JDBC connection to " + URL);
            con = DriverManager.getConnection(URL, USR, PWD);
            System.out.println(ShowDateTime() + "  Source DB Connected");
        } catch (ClassNotFoundException e) {
            StopNow(e.getMessage());
        } catch (SQLException throwables) {
            StopNow(throwables.getMessage());
        }
        kProducer.SetBroker(BRK);
        kProducer.SetTopic(TOPIC);
        kProducer.SetClientID("Producer:PID:" + pid);
    }

    private static void SelectLogs() {
        String encQry = "SELECT * FROM UNILIBRE.AUD_TRXLOGS WHERE ID > " + lastID + " ORDER BY ID ASC";
        if (!encQry.equals(prevSelect)) {
            System.out.println(ShowDateTime() + "  " + encQry);
            prevSelect = encQry;
        }

        try {
            if (logStmt != null) logStmt.close();
            if (logItems != null) logItems.close();
            logItems = null;
            logStmt = null;
            logStmt = con.createStatement();
            logItems = logStmt.executeQuery(encQry);
        } catch (SQLException throwables) {
            StopNow(throwables.getMessage());
        }
    }

    private static String[] GetCols(String sch, String tbl) {
        String dbitem = sch+"."+tbl;
        int pos = dbObject.indexOf(dbitem);
        if (pos < 0) {
            String cmd = "select col.column_name from sys.all_tab_columns col " +
                    "where col.owner = '" + sch + "' " +
                    "and col.table_name = '" + tbl + "' " +
                    "order by col.column_id";
            String colName, strCols="";
            try {
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(cmd);
                while (rs.next()) {
                    colName = rs.getString("COLUMN_NAME");
                    strCols += "," + colName;
                }
                strCols = strCols.substring(1, strCols.length());
                dbObject.add((sch+"."+tbl).toUpperCase());
                columns.add(strCols);
                rs.close();
                rs = null;
                stmt.close();
                stmt=null;
                cmd="";
                colName="";
                strCols="";
            } catch (SQLException throwables) {
                StopNow(throwables.getMessage());
            }
            pos = dbObject.indexOf(dbitem);
            if (pos < 0) StopNow("Cannot work with "+dbitem);
        }
        return columns.get(pos).split("\\,");
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

    private static String[] GetDetails(String dbitem, String rid, String[] cList) {
        String cmd = "select * from " + dbitem + " where rowid = '" + rid + "'";
        String strData="", strCols="", datum="";
        ResultSet selList = null;
        try {
            Statement stmt = con.createStatement();
            selList = stmt.executeQuery(cmd);
            while (selList.next()) {
                for (int i=0; i < cList.length; i++) {
                    datum = selList.getString(cList[i]);
                    strData += "<ft>"+datum;
                    strCols += "<ft>"+cList[i];
                }
                cList = null;
                datum="";
            }
            selList.close();
            selList=null;
            stmt.close();
            stmt=null;
        } catch (SQLException throwables) {
            StopNow(throwables.getMessage());
        }
        String[] ans = new String[2];
        ans[0] = strCols;
        ans[1] = strData;
        strCols="";
        strData="";
        return ans;
    }

    private static String BuildPayload(String sch, String tbl, String strCols, String strData) {
        if (strCols.startsWith("<ft>")) strCols = strCols.substring(4,strCols.length());
        if (strData.startsWith("<ft>")) strData = strData.substring(4,strData.length());
        String encSeed="";
        if (ENC) {
            encSeed = uCipher.GetCipherKey();
            sch = uCipher.v2Scramble(uCipher.keyBoard, sch, encSeed);
            tbl = uCipher.v2Scramble(uCipher.keyBoard, tbl, encSeed);
            strCols = uCipher.v2Scramble(uCipher.keyBoard, strCols, encSeed);
            strData = uCipher.v2Scramble(uCipher.keyBoard, strData, encSeed);
        }

        JSONObject jObj = new JSONObject();
        jObj.put("Source", SRC);
        jObj.put("Dts", GetDateTime());
        jObj.put("Passport", encSeed);
        jObj.put("Schema", sch);
        jObj.put("Table", tbl);
        jObj.put("Columns", strCols);
        jObj.put("Data", strData);
        String rec = jObj.toString();
        jObj = null;
        return rec;
    }

    private static boolean CheckStop(String cfile) {
        Properties props = LoadProps(cfile);
        try {
            loadsize = Integer.valueOf(props.getProperty("loadsize"));
        } catch (NumberFormatException nfe) {
            loadsize = 100;
        }
        try {
            loadwait = Integer.valueOf(props.getProperty("loadsize"));
        } catch (NumberFormatException nfe) {
            loadwait = 5000;
        }
        boolean ans = !props.getProperty("stop", "").toLowerCase().equals("true");
        return ans;
    }

    private static void DropRow(String sqn) {
        try {
            String cmd = "delete AUD_TRXLOGS where ID = '" + sqn + "'";
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(cmd);
            rs.close();
            rs = null;
            stmt.close();
            stmt=null;
        } catch (SQLException throwables) {
            StopNow(throwables.getMessage());
        }
    }

    public static void main(String[] args) {
        String cFile = System.getProperty("conf", "");
        if (cFile.equals("")) StopNow("No conf parameter");
        System.out.println(" ");
        System.out.println(ShowDateTime() + "  ===============================================================");
        ConnectSource(cFile);
        System.out.println(ShowDateTime() + "  ===============================================================");
        System.out.println(ShowDateTime() + "  pKafkOra(): Will push database deltas into uStreams  ");
        System.out.println(ShowDateTime() + "  Using Config file    : [" + cFile + "]");
        System.out.println(ShowDateTime() + "  Using Kafka broker(s): [" + BRK + "]");
        System.out.println(ShowDateTime() + "  Using Kafka topic    : [" + TOPIC + "]");
        System.out.println(ShowDateTime() + "  ===============================================================");
        System.out.println(" ");

        String dbitem, sqn, sch, tbl, rid, rec, encSeed;
        int pos=0, tcnt=0, nodata=0, gtot=0, lastCnt=0;
        boolean logged=false, okay = CheckStop(cFile);
        System.out.println(ShowDateTime() + "  Logging in progress.");
        System.out.println("  ");
        startM = System.nanoTime();
        while(okay) {
            SelectLogs();
            tcnt=0;
            try {
                // ------------- [ Kafka Pump ] ----------------------------------------------------------------
                while (logItems.next()) {
                    sqn = logItems.getString("ID");
                    sch = logItems.getString("SCH");
                    tbl = logItems.getString("TBL");
                    rid = logItems.getString("RID");
                    if (sqn==null || sch==null || tbl==null || rid==null) continue;
                    dbitem = sch + "." + tbl;
                    String[] cList  = GetCols(sch, tbl);
                    String[] jLoad  = GetDetails(dbitem, rid, cList);
                    rec             = BuildPayload(sch, tbl, jLoad[0], jLoad[1]);
                    logged          = kProducer.kBatchCollector("topic", "key", rec);
//                    logged          = kProducer.kSend(rec);
                    if (logged)       DropRow(sqn);
                    try { lastID = Integer.valueOf(sqn); } catch (NumberFormatException nfe) { lastID = 0; }
                    cList=null;
                    jLoad=null;
                    gtot++;
                    tcnt++;
                    if (tcnt>=loadsize) break;
                }
                // ---------------------------------------------------------------------------------------------
                try {
                    Thread.sleep(loadwait);
                } catch (InterruptedException e) {
                    StopNow(e.getMessage());
                }
                if (tcnt==0) {
                    if (gtot != lastCnt) {
                        System.out.println(ShowDateTime() + "  " + gtot + " events have been streamed.");
                        lastCnt = gtot;
                    }
                    nodata++;
                    if (nodata > 100) {
                        System.out.println(ShowDateTime() + "  Heartbeat");
                        nodata = 0;
                    }
                } else {
                    finishM = System.nanoTime();
                    laps = (finishM - startM) / div;
                    System.out.println(ShowDateTime() + "     .) " + tcnt + " deltas logged in " + laps + " seconds");
                    startM = finishM;
                }
            } catch (SQLException throwables) {
                StopNow(throwables.getMessage());
            }
            okay = CheckStop(cFile);
        }

        kProducer.Close();
        System.out.println(" ");
        System.out.println(ShowDateTime() + "  ===============================================================");
        System.out.println(ShowDateTime() + "  Master stop switch turned ON. Stopping now.");
        System.out.println(ShowDateTime() + "  ===============================================================");
        System.out.println(" ");
    }

}
