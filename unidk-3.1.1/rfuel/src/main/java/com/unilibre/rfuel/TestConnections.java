package com.unilibre.rfuel;

import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniObjectsTokens;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSessionException;
import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.artemisMQ;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SourceDB;
import com.unilibre.commons.SqlCommands;
import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kProducer;

import javax.jms.JMSException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

public class TestConnections {

    private static boolean amqConnected = false;
    private static String dash = "-------------------------------------------------------------------------------------";

    public static void main(String[] args) throws JMSException {
        if (NamedCommon.upl.equals("")) {
            NamedCommon.upl = System.getProperty("user.dir");
            NamedCommon.BaseCamp = NamedCommon.upl;
            if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
            if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";
        }

        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
        }

        System.out.println(" ");
        System.out.println(" ");
        boolean sqlOK=true, uvOK=true, mqOK=true;

        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(runProps);

        String pass = "successful";
        String fail=  "--failed--";
        String ustat=fail, sstat=fail, kstat=fail, mstat=fail;

        System.out.println(("Trying SourceDB "+dash+dash).substring(0,99));
        TrySourceDB();
        System.out.println(" ");
        System.out.println(("Trying SQL DB "+dash+dash).substring(0,99));
        TrySQLDB();
        System.out.println(" ");
        System.out.println(("Trying AMQ"+dash+dash).substring(0,99));
        TryAMQ();
        System.out.println(" ");
        String kBroker = System.getProperty("kbroker", "");
        if (!kBroker.equals("")) {
            TryKafka(kBroker);
            if (kProducer.producer != null) kstat = pass;
        }

        uvOK = NamedCommon.sConnected;
        sqlOK = NamedCommon.tConnected;
        mqOK  = amqConnected;
        if (uvOK)  ustat=pass;
        if (sqlOK) sstat=pass;
        if (mqOK)  mstat=pass;

        System.out.println(" ");
        System.out.println("-------------------------------------------------");
        System.out.println("SourceDB : " + ustat + " connection");
        System.out.println("TargetDB : " + sstat + " connection");
        if (!kBroker.equals("")) System.out.println("   Kafka : " + kstat + " connection");
        System.out.println("ActiveMQ : " + mstat + " connection");
        System.out.println("-------------------------------------------------");
        System.out.println(" ");
        System.out.println(" ");
        System.out.println(" ");

        if (!NamedCommon.tConnected) System.exit(0);

        NamedCommon.RunType = "REFRESH";
        NamedCommon.BulkLoad = true;
        System.out.println(" ");
        System.out.println("... now checking Authorization ----------------------------------");
        System.out.println(" ");
        ArrayList<String> DDL = new ArrayList<>();
        String[] tcols = new String[3];
        tcols[0] = "Col1";
        tcols[1] = "Col2";
        tcols[2] = "Col3";
        String schema = "zzz", table = "TEMPTABLE";
        String create = "";
        String delete = "IF EXISTS (SELECT * FROM sys.schemas WHERE name = '"+schema+"') DROP SCHEMA ["+schema+"];";
        System.out.println("Sending: delete schema\n" + delete);
        System.out.println(" ");
        DDL.add(delete);
        SqlCommands.ExecuteSQL(DDL);
        if (NamedCommon.ZERROR) {
            System.out.println("ERROR");
            System.exit(1);
        } else {
            uCommons.Sleep(1);
        }
        DDL.clear();
        create = SqlCommands.CreateTable(NamedCommon.rawDB, schema, table, tcols);
        System.out.println("Sending: create table\n" + create);
        System.out.println(" ");
        DDL.add(create);
        SqlCommands.ExecuteSQL(DDL);
        if (NamedCommon.ZERROR) {
            System.out.println("ERROR");
            System.exit(1);
        } else {
            uCommons.Sleep(1);
        }
        DDL.clear();
        String drop = SqlCommands.DropTable(NamedCommon.rawDB, schema, table);
        System.out.println("Sending: drop table\n" + drop);
        System.out.println(" ");
        DDL.add(drop);
        SqlCommands.ExecuteSQL(DDL);
        if (NamedCommon.ZERROR) {
            System.out.println("ERROR");
            System.exit(1);
        } else {
            uCommons.Sleep(1);
        }
        DDL.clear();
        System.out.println("Sending: delete schema\n" + delete);
        System.out.println(" ");
        DDL.add(delete);
        SqlCommands.ExecuteSQL(DDL);
        if (NamedCommon.ZERROR) {
            System.out.println("ERROR");
            System.exit(1);
        } else {
            uCommons.Sleep(1);
        }
        DDL.clear();
        System.exit(1);
    }

    private static void TrySourceDB() {
        SourceDB.ConnectSourceDB();
    }

    private static void TrySourceDB_old() {
        NamedCommon.sConnected = false;
        NamedCommon.uJava = null;
        NamedCommon.uSession = null;
        NamedCommon.uJava = new UniJava();
        NamedCommon.uSession = new UniSession();
        int contype = 0;
        if (NamedCommon.CPL) {
            NamedCommon.uJava.setIdleRemoveThreshold(60000);    // max session idle time  = 60 seconds
            NamedCommon.uJava.setIdleRemoveExecInterval(15000); // look for idle sessions = 15 seconds
            NamedCommon.uJava.setOpenSessionTimeOut(3000);      // wait 3 seconds for a session to open
            NamedCommon.uJava.setUOPooling(true);
            try {
                int minSize = Integer.valueOf(NamedCommon.minPoolSize);
                int maxSize = Integer.valueOf(NamedCommon.maxPoolSize);
                NamedCommon.uJava.setMinPoolSize(minSize);
                NamedCommon.uJava.setMaxPoolSize(maxSize);
            } catch (NumberFormatException nfe) {
                return;
            }
            if (NamedCommon.uSecure) contype = UniObjectsTokens.SECURE_SESSION;
        }
        try {
            NamedCommon.uSession = NamedCommon.uJava.openSession(contype);
        } catch (UniSessionException e) {
            return;
        }
        NamedCommon.uSession.setHostName(NamedCommon.dbhost);
        NamedCommon.uSession.setHostPort(Integer.parseInt(NamedCommon.dbPort));
        NamedCommon.uSession.setUserName(NamedCommon.dbuser);
        NamedCommon.uSession.setPassword(NamedCommon.passwd);
        NamedCommon.uSession.setAccountPath(NamedCommon.dbpath);
        String dbtype = NamedCommon.ServiceType;
        if (NamedCommon.ServiceType.equals("")) {
            if (NamedCommon.databaseType.equals("UNIVERSE")) {
                dbtype = "uvcs";
            } else {
                dbtype = "udcs";
            }
        }
        NamedCommon.uSession.setConnectionString(dbtype);

        System.out.println("   ) Connect: " + NamedCommon.dbhost+":"+NamedCommon.dbPort + " usr: " + NamedCommon.dbuser
        + " pwd: " + NamedCommon.passwd + " home: "+NamedCommon.dbpath);
        try {
            NamedCommon.uSession.connect();
        } catch (UniSessionException e) {
            System.out.println("UniObjects Error: " + e.getMessage());
            return;
        }
        NamedCommon.sConnected = true;
    }

    private static void TrySQLDB_old() {
        NamedCommon.tConnected = false;
        String[] jdbcParts = NamedCommon.jdbcCon.split(NamedCommon.jdbcSep);
        String usr="", pwd="", key="", val="", line="";
        String[] lparts;
        for (int p = 0; p < jdbcParts.length; p++) {
            line = jdbcParts[p];
            if (line.contains("=ENC(")) {
                int lx = line.indexOf("=ENC(");
                String tmp = line.substring(lx + 5, (line.length() - 1));
                line = line.substring(0, lx + 1) + uCipher.Decrypt(tmp);
            }

            lparts = line.split("\\=");
            val = "";
            if (lparts.length == 1) {
                val = lparts[0] + NamedCommon.jdbcSep + NamedCommon.jdbcAdd;
            } else {
                val = lparts[1];
            }
            key = lparts[0];
            switch (key) {
                case "user":
                    usr = val;
                    break;
                case "password":
                    pwd = val;
                    break;
            }
        }

        if (!NamedCommon.jdbcDvr.equals("")) {
            try {
                Class.forName(NamedCommon.jdbcDvr);
                System.out.println("   ) Using: " + NamedCommon.jdbcDvr);
            } catch (ClassNotFoundException e) {
                System.out.println("Class.forName() failed - " + e.getMessage());
                return;
            }
        }

        try {
            System.out.println("   ) URL  : " + NamedCommon.jdbcCon);
            Connection con = DriverManager.getConnection(NamedCommon.jdbcCon);
        } catch (SQLException e) {
            System.out.println("Jdbc/SQLDB Error: " + e.getMessage());
            return;
        }
        NamedCommon.tConnected = true;
    }

    private static void TrySQLDB() {
        NamedCommon.tConnected = false;
        SqlCommands.ConnectSQL();
    }

    private static void TryKafka(String kBroker) {
        kProducer.SetBroker(kBroker);
        kProducer.SetClientID("Test_Connection_" + NamedCommon.pid);
        kProducer.producer = kProducer.kConnect();
    }

    private static void TryAMQ() throws JMSException {
        Properties runProps = uCommons.LoadProperties("this.server");
        String broker = runProps.getProperty("brokers", "");
        runProps = uCommons.LoadProperties(broker);
        String ip = runProps.getProperty("ip.address", "");
        String url = runProps.getProperty("url", "");
        String user = runProps.getProperty("bkruser", "");
        String passwd = runProps.getProperty("bkrpword", "");

        if (url.contains("$ip")) url = url.replace("$ip", ip);

        if (url.startsWith("ENC("))       url = uCipher.Decrypt(url);
        if (user.startsWith("ENC("))     user = uCipher.Decrypt(user);
        if (passwd.startsWith("ENC(")) passwd = uCipher.Decrypt(passwd);

        if (NamedCommon.artemis) {
            artemisMQ.cPrepare(url, user, passwd, "1");
            if (artemisMQ.isConnected()) amqConnected = true;
        } else {
            if (activeMQ.rfConsumer(url, user, passwd, "", NamedCommon.testQ) != null) amqConnected = true;
        }
    }

}
