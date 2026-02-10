package com.unilibre.tester;

/* * Copyright UniLibre on 2015. ALL RIGHTS RESERVED  **/

// Execute a select statement and send rows to a Kafka Topic

// Config file ----------------------------------------------------
//
//      Input   : A config file.
//              : broker={kafka broker connection string}
//              : topic={topic to write it to}
//              : command={file holding the sql}
//
//      Process : Build select statement
//                Place columns in an array.
//                Add "from" line to the end of the statement
//
//      Execute : Using standard rFuel.properties constring
//
//      Output  : to kafka topic
//
// ----------------------------------------------------------------


import com.unilibre.cipher.uCipher;
import com.unilibre.commons.ConnectionPool;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SqlCommands;
import com.unilibre.commons.uCommons;
import com.unilibre.kafka.kProducer;
import org.apache.kafka.clients.producer.Producer;
import org.json.JSONObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;

public class SelectToKafka {

    private static boolean verbose = true;
    private static String SqlCmd = "";
    private static String topic = "";
    private static String[] cols;
    private static ResultSet rs = null;
    private static Statement stmt = null;
    private static int batchsize = 999;
    private static int showAt = 1000;

    public static void main(String[] args) throws SQLException, IOException {
        if (NamedCommon.BaseCamp.contains("/home/andy")) NamedCommon.BaseCamp = NamedCommon.DevCentre;

        // validate inbound args ------------------------------------------------------------------------------------
        if (args.length == 0) {
            uCommons.uSendMessage("Usage:  SelectToKafka.sh {fully qualified path to file} {mode=background}");
            return;
        }
        if (args.length > 1) {
            if (args[1].toLowerCase().contains("mode")) {
                // mode=background
                String mode = uCommons.FieldOf(args[1], "=", 2);
                if (mode.equalsIgnoreCase("background")) verbose = false;
            }
        }

        // load rFuel.properties ------------------------------------------------------------------------------------
        NamedCommon.Reset();
        uCipher.SetAES(false, "", "");
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) return;
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);

        // connect to sql server -----------------------------------------------------------------------------------
        if (!NamedCommon.tConnected) {
            if (verbose) uCommons.uSendMessage("Initialise SQL DB -------------------------------------");
            boolean okay = SqlCommands.ConnectSQL();
            if (!okay) return;
            NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
            NamedCommon.uCon.setClientInfo("responseBuffering", "adaptive");
            if (NamedCommon.uCon == null) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return;
            }
            if (verbose) System.out.println();
        }

        // get run properties from config file ---------------------------------------------------------------------
        if (!args[0].startsWith(NamedCommon.BaseCamp)) {
            if (!NamedCommon.BaseCamp.endsWith(NamedCommon.slash)) {
                args[0] = NamedCommon.BaseCamp + NamedCommon.slash + args[0];
            } else {
                args[0] = NamedCommon.BaseCamp + args[0];
            }
        }
        String fqfn = args[0];

        if (verbose) uCommons.uSendMessage("-------------------------------------------------------");
        if (verbose) uCommons.uSendMessage("Getting properties [" + fqfn + "]");
        if (fqfn.isEmpty()) System.exit(1);
        //
        //          MUST be a fully qualified file name !!!!
        //
        Properties props = uCommons.LoadProperties(fqfn);
        String[] clid = fqfn.split("/");
        String broker = uCommons.GetValue(props, "broker", ""); // e.g. 192.168.48.136:9092
        topic         = uCommons.GetValue(props, "topic", ""); // e.g. ultracs.import.s4
        String group = "SelectToKafka";
        String ClientID = clid[clid.length-1];
        String saslKey = uCommons.GetValue(props, "saslkey", "");
        String saslUsr = "";
        String saslPwd = "";
        // add sasl vars ONLY is saslKey # ""
        boolean isSasl = false;
        if (!saslKey.isEmpty()) {
            isSasl = true;
            saslKey = uCipher.Decrypt(saslKey);
            saslUsr = uCipher.Decrypt(uCommons.GetValue(props, "saslUsr", ""));
            saslPwd = uCipher.Decrypt(uCommons.GetValue(props, "saslPwd", ""));
        }

        batchsize = Integer.parseInt(uCommons.GetValue(props, "batchsize", "999"));
        showAt = Integer.parseInt(uCommons.GetValue(props, "show.at", "999"));
        SqlCmd = uCommons.GetValue(props, "command", "");
        if (SqlCmd.isEmpty()) {
            uCommons.uSendMessage("Fail - no SQL command to execute.");
            System.exit(1);
        }

        // initialise Kafka ----------------------------------------------------------------------------------------
        if (verbose) uCommons.uSendMessage("Initialise Kafka -------------------------------------");
        kProducer.SetBroker(broker);
        kProducer.SetTopic(topic);
        kProducer.SetClientID(ClientID);
        kProducer.SetGroup(group);
        kProducer.SetSASL(isSasl, saslUsr, saslPwd, saslKey);
        kProducer.SetBatchSize(batchsize);

        if (verbose) uCommons.uSendMessage("Create Kafka Producer:");
        Producer kProucer = kProducer.kConnect();
        if (kProucer != null) {
            if (verbose) uCommons.uSendMessage("Done.");
        } else {
            if (verbose) uCommons.uSendMessage("Failed.");
            System.exit(1);
        }

        // execute sql query and send to Kafka ---------------------------------------------------------------------
        ProcessSqlCommand(args[0]);
        WriteToKafka();
        rs.close();
        stmt.close();
        NamedCommon.uCon.close();
    }

    private static void ProcessSqlCommand(String fqfn) throws IOException, SQLException {
        if (verbose) uCommons.uSendMessage("Processing SQL to kafka ------------------------------");
        if (SqlCmd.toLowerCase().endsWith(".sql")) {
            fqfn = fqfn.replace('\\', '/');
            String[] jArr = fqfn.split("/");
            jArr[jArr.length - 1] = SqlCmd;
            String fqsn = String.join("/", jArr);
            SqlCmd = new String(Files.readAllBytes(Paths.get(fqsn)));
            fqsn = "";
            jArr = null;
            //  run a single query
            if (verbose) uCommons.uSendMessage("   ) Getting command(s) from " + fqsn);
            stmt = null;
            rs = null;
            ExecuteSQL(SqlCmd);
        } else {
            uCommons.uSendMessage("Please store SQL query in a *.sql file");
            return;
        }
    }

    private static void ExecuteSQL(String cmd) throws SQLException {
        if (!NamedCommon.tConnected) {
            if (verbose) System.out.println(" ");
            if (verbose) uCommons.uSendMessage("   ) Connecting to SQL host");
            boolean okay = SqlCommands.ConnectSQL();
            if (!okay) return;
            NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
            if (NamedCommon.uCon == null) return;
            NamedCommon.uCon.setClientInfo("responseBuffering", "adaptive");
        }

        if (verbose) System.out.println(" ");
        if (verbose) uCommons.uSendMessage("   ) Executing SQL query");

        try {
            stmt = NamedCommon.uCon.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(cmd);
            rs.setFetchSize(batchsize);
            if (verbose) uCommons.uSendMessage("   ) Done.");
        } catch (SQLException e) {
            uCommons.uSendMessage(e.getMessage());
        }
    }

    private static void WriteToKafka() throws SQLException {
        String key, value;
        if (cols == null) GetAllColumns();
        if (cols == null) {
            uCommons.uSendMessage("   ) Cannot find columns.");
            return;
        }
        int cLen = cols.length, cnt=0;
        boolean okay = true;

        while (rs.next()) {
            JSONObject json = new JSONObject();

            cnt++;
            if ((cnt % showAt) == 0 && verbose) uCommons.uSendMessage(uCommons.oconvM(String.valueOf(cnt), "MD0,"));

            for (int i = 1; i <= cLen; i++) {
                key   = cols[i-1];
                value = rs.getString(i);
                json.put(key, value);
            }

            key = String.valueOf(cnt);
            okay = kProducer.kBatchCollector(topic, key, json.toString());
            json = null;
            if (!okay) return;
        }
        kProducer.kBatch();
    }

    private static void GetAllColumns() {
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int nbrCols = metaData.getColumnCount();
            cols = new String[nbrCols];
            for (int i = 1; i <= nbrCols; i++) { cols[i - 1] = metaData.getColumnName(i); }
        } catch (SQLException e) {
            uCommons.uSendMessage(e.getMessage());
        }
    }

}