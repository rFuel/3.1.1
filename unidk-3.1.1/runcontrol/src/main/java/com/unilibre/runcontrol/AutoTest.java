package com.unilibre.runcontrol;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class AutoTest {

    private static boolean debug = false;
    private static boolean StopOnFail = false;
    private static String useID;
    private static String endpoint = "";
    private static String idTag = "~ID~";
    private static String bkr;
    private static String inTask= "";
    private static int timeout=100;
    private static String usePID = NamedCommon.pid;

    public static void main(String[] args) {
        if (args.length == 5) {
            debug = args[0].toLowerCase().equals("true");
            StopOnFail = args[1].toLowerCase().equals("true");
            useID = args[2];
            inTask = args[3];
            bkr = args[4];
        } else {
            debug = (System.getProperty("debug", "").toLowerCase().equals("true"));
            StopOnFail = (System.getProperty("failstop", "").equals("true"));
            useID = System.getProperty("id", "1");
            inTask = System.getProperty("task", "");
            bkr = System.getProperty("bkr", "");
        }
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
//        usePID = NamedCommon.IAM;
        if (bkr.equals("")) {NamedCommon.Broker = "autotest.bkr";} else { NamedCommon.Broker = bkr;}
        MessageInOut.debug = NamedCommon.debugging;

        System.out.println("  ");
        uCommons.uSendMessage("Initialise Autotests("+usePID+") -------------------------------------------");
        uCommons.uSendMessage("debugging is "+debug);
        uCommons.uSendMessage("Broker    is "+NamedCommon.Broker);

        NamedCommon.AutoTests = true;
        String slash = "";
        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            String host = "rfuel22";
            NamedCommon.BaseCamp = "\\\\" + host + "\\all\\upl";
        }
        NamedCommon.gmods = NamedCommon.BaseCamp + "\\lib\\";
        NamedCommon.upl = NamedCommon.BaseCamp;
        if (NamedCommon.upl.contains("/")) slash = "/";
        if (NamedCommon.upl.contains("\\")) slash = "\\";
        NamedCommon.slash = slash;
        String basecamp = NamedCommon.BaseCamp + NamedCommon.slash;
        String conf = basecamp + "conf" + NamedCommon.slash;
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("RUN ERROR -----------------------------------------");
            uCommons.uSendMessage(NamedCommon.Zmessage);
            System.exit(0);
        }

        Properties bProps = uCommons.LoadProperties(conf + NamedCommon.Broker);
        if (NamedCommon.ZERROR) {
            uCommons.uSendMessage("RUN ERROR -----------------------------------------");
            uCommons.uSendMessage(NamedCommon.Zmessage);
            System.exit(0);
        }
        uCommons.BkrCommons(bProps);
//        NamedCommon.messageBrokerUrl = bProps.getProperty("url");
        NamedCommon.jdbcCon = bProps.getProperty("jdbcCon", NamedCommon.jdbcCon);
        String tOut = bProps.getProperty("timeout", "");
        if (!tOut.equals("")) timeout = Integer.valueOf(tOut);
        endpoint = bProps.getProperty("endpoint", "");

        if (License.GetDomain("hostname")) NamedCommon.hostname = License.domain;
        APImsg.instantiate();
        DoTests(bProps);
        uCommons.uSendMessage("*** Stopping ***");
        System.exit(0);
    }

    public static void SetDebug(boolean inVal) {
        debug = inVal;
    }

    private static void DoTests(Properties props) {
        String fqfn="", vTopic="";
        ArrayList<String> TasksArray = null, qNameArray=null;
        if (endpoint.equals("")) {
            TasksArray = new ArrayList<>(Arrays.asList(props.getProperty("tasks").split("\\,")));
            qNameArray = new ArrayList<>(Arrays.asList(props.getProperty("qname").split("\\,")));
            vTopic = props.getProperty("topic", "");
            MessageInOut.messageBrokerUrl = NamedCommon.messageBrokerUrl;
            fqfn = props.getProperty("table", "");
        }
        String command = props.getProperty("command", "");
        String reps = props.getProperty("repeats", "1");
        int iLoops = reps.length()+1, iMcnt=0;
        int repeats = Integer.valueOf(reps);
        if (repeats < 1) repeats = 1;
        if (fqfn.equals("") && command.equals("")) return;

        SqlCommands.ConnectSQL();

        String url, usr, pwd, key, val, line;

        url = APImsg.APIget("jdbccon:base-sql");
        usr = APImsg.APIget("jdbcUsr:base-sql");
        pwd = APImsg.APIget("jdbcPwd:base-sql");
        String jdbcDBI = APImsg.APIget("sqldb:base-sql");
        String jdbcSCH = APImsg.APIget("schema:base-sql");
        if (!url.endsWith(NamedCommon.jdbcSep)) url += NamedCommon.jdbcSep;
        if (!NamedCommon.jdbcAdd.equals("")) url += NamedCommon.jdbcAdd;
        if (!url.endsWith(NamedCommon.jdbcSep)) url += NamedCommon.jdbcSep;

        Statement stmt = null;
        ResultSet rs;
        try {
            ConnectionPool.AddToPool(url, usr, pwd);
            if (NamedCommon.ZERROR) {
                uCommons.uSendMessage("RUN ERROR -----------------------------------------");
                uCommons.uSendMessage(NamedCommon.Zmessage);
                System.exit(0);
            }
            NamedCommon.uCon = ConnectionPool.getConnection(url);
            if (NamedCommon.uCon == null) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                return;
            }
            String chk = url + "+" + jdbcDBI + "+" + jdbcSCH;
            if (ConnectionPool.objPool.indexOf(chk) < 0 ) ConnectionPool.objPool.add(chk);
            stmt = NamedCommon.uCon.createStatement();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            System.exit(0);
        }


        String cmd="", cnt="";
        if (command.equals("")) {
            if (!fqfn.equals("")) {
                cmd = "select * from " + fqfn + " order by cast(MessageID as int);";
                cnt = "select count(*) as Count from " + fqfn;
            } else {
                uCommons.uSendMessage("ERROR - \"table=\" key:value pair in "+NamedCommon.Broker);
                System.exit(0);
            }
        } else {
            cmd = command;
            cnt = "";
        }
        String mID, mDesc, message, expected, actual, queue="", task, mLine;
        String corrid, lCnt, nProto;
        GarbageCollector.setStart(System.nanoTime());
        long stime, etime;
        double laps, div = 1000000000.00;
        int tPos, fCnt=0, pCnt=0, mCnt=0, tmCnt=0, nbr=0;
        boolean passed;
        MessageInOut mio = null;
        if (endpoint.equals("")) mio = new MessageInOut();

        try {
            uCommons.uSendMessage("   ***********************************************");
            uCommons.uSendMessage("   ********          Auto Tests          *********");
            uCommons.uSendMessage("   ***********************************************");
            for (int r=0; r<repeats; r++) {
                System.out.println("  ");
                System.out.println((r+1)+">  "+cmd);
                System.out.println("  ");
                if (!cnt.equals("")) {
                    rs = stmt.executeQuery(cnt);
                    rs.next();
                    nbr = rs.getInt("Count");
                    iMcnt = String.valueOf(nbr).length();
                    System.out.println(nbr + " test messages to process.");
                    System.out.println("  ");
                } else {
                    iMcnt = 5;
                }
                rs = stmt.executeQuery(cmd);

                lCnt = uCommons.RightHash("000"+(r+1), iLoops);
                mCnt=0;
                while (rs.next()) {
                    mCnt++;
                    tmCnt++;
                    mID = uCommons.RightHash("000000"+rs.getString("MessageID"), iMcnt);
                    mDesc = rs.getString("Description");
                    message = rs.getString("Message");
                    nProto  = rs.getString("notProtocol");
                    if (message.contains(idTag)) message = message.replace(idTag, useID);
                    APImsg.instantiate();
                    uCommons.MessageToAPI(message);
                    if (NamedCommon.ZERROR) continue;

                    if (debug) APImsg.APIset("debug", "true");
                    if (!inTask.equals("")) if (!inTask.equals(APImsg.APIget("task"))) continue;

                    corrid = (lCnt+"-"+mID+"-"+mDesc).replace("\\ ", "");

                    NamedCommon.CorrelationID = usePID + "-" + corrid;
                    NamedCommon.reply2Q       = APImsg.APIget("replyto");
                    APImsg.APIset("correlationid", NamedCommon.CorrelationID);

                    APImsg.APIset("replyto", NamedCommon.reply2Q);
                    APImsg.APIset("correlationid", NamedCommon.CorrelationID);

                    expected = rs.getString("Expected");
                    expected = expected.replaceAll("\\r?\\n", "");
                    task = APImsg.APIget("task");
                    if (endpoint.equals("")) {
                        tPos = TasksArray.indexOf(task);
                        if (!vTopic.equals("")) {
                            queue = "VirtualTopic." + vTopic;
                        } else {
                            queue = qNameArray.get(tPos) + "_001";
                        }
                        NamedCommon.inputQueue = queue;
                    }

                    mLine = "["+mCnt+"] ["+lCnt+"-"+mID + "] " + mDesc;
                    uCommons.uSendMessage(mLine);
                    mLine = "";

                    if (nProto.contains(NamedCommon.protocol)) {
                        uCommons.uSendMessage("   ) SKIP this message for protocol: [" + nProto + "]");
                        System.out.println(" ");
                        continue;
                    }

                    if (endpoint.equals("")) {
                        uCommons.uSendMessage("   ) On URL   : " + NamedCommon.messageBrokerUrl);
                        uCommons.uSendMessage("   ) SendTo   : " + queue);
                        uCommons.uSendMessage("   ) ReceiveOn: " + NamedCommon.reply2Q);
                    } else {
                        uCommons.uSendMessage("   ) Endpoint : " + endpoint); // + "    this is a muleAPI test ************");
                    }
                    uCommons.uSendMessage("   ) Correl ID: " + NamedCommon.CorrelationID);
                    APImsg.APIset("CorrelationID", NamedCommon.CorrelationID);
                    stime = System.nanoTime();
                    message = "";
                    for (int ii = 0; ii < APImsg.GetMsgSize(); ii++) {
                        key = APImsg.APIgetKey(ii);
                        val = APImsg.APIgetVal(ii);
                        if (!val.equals("")) message += key + "<is>" + val + "<tm>";
                    }
                    for (int ii = 0; ii < APImsg.GetVLISTSize(); ii++) {
                        key = APImsg.APIgetVLISTKey(ii);
                        val = APImsg.APIgetVLISTVal(ii);
                        if (!val.equals("")) message += "use." + key + "<is>" + val + "<tm>";
                    }
                    for (int ii = 0; ii < APImsg.GetU2PSize(); ii++) {
                        key = APImsg.APIgetU2PKey(ii);
                        val = APImsg.APIgetU2PVal(ii);
                        if (!val.equals("")) message += "u2p." + key + "<is>" + val + "<tm>";
                    }

                    if (!message.startsWith("{")) message = msgCommons.jsonifyMessage(message);

                    if (endpoint.equals("")) {
                        actual = mio.SendAndReceive(NamedCommon.messageBrokerUrl, queue, NamedCommon.reply2Q, NamedCommon.CorrelationID, message, usePID + "-AutoTest");
                    } else {
                        actual = muleCommons.sendToEndpoint(endpoint, message + "debug<is>true<tm>");
                        if (actual.equals("")) {
                            actual = muleCommons.TryAgain(endpoint);
                            uCommons.uSendMessage("   ) got back an empty reply.");
                        }
                    }
                    etime = System.nanoTime();
                    laps = (etime - stime) / div;

                    actual = actual.replaceAll("\\r?\\n", "");

                    if (expected.equals(actual)) {
                        mLine += "PASS     : ";
                        passed = true;
                        pCnt++;
                    } else {
                        if (!endpoint.equals("") && actual.contains(expected)) {
                            mLine += "PASS     : ";
                            passed = true;
                            pCnt++;
                        } else {
                            mLine += "FAIL     : ";
                            passed = false;
                            fCnt++;
                        }
                    }
                    mLine += laps;
                    uCommons.uSendMessage("   ) " + mLine);
                    if (!passed || debug) {
                        System.out.println(" Message: " + message);
                        System.out.println("Expected: " + expected);
                        System.out.println("  Actual: " + actual);
                        if (!passed) {
                            if (!actual.equals("")) {
                                int max = expected.length();
                                String ch1, ch2, spc = "";
                                if (actual.length() > max) max = actual.length();
                                for (int lx = 0; lx < max; lx++) {
                                    ch1 = expected.substring(lx, lx + 1);
                                    ch2 = actual.substring(lx, lx + 1);
                                    if (!ch1.equals(ch2)) break;
                                    spc += "*";
                                }
                                System.out.println("   ERROR: " + spc + "^");
                            }
                        }
                        if (StopOnFail && !passed) System.exit(0);
                    }
                    if (timeout > 0) {
                        try {
                            Thread.sleep(timeout);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.exit(0);
                        }
                    }
                    System.out.println("   ");
                    GarbageCollector.CleanUp();
                }
            }
            uCommons.uSendMessage("*********************************************");
        } catch (SQLException e) {
            System.out.println("SQL Error: "+e.getMessage());
            System.exit(1);
        }
        NamedCommon.AutoTests = false;
        uCommons.uSendMessage(tmCnt+" messages sent:  "+pCnt+" passed  "+fCnt+" failed");
    }

    public static void SetStop(boolean setting) {
        StopOnFail = setting;
    }
}
