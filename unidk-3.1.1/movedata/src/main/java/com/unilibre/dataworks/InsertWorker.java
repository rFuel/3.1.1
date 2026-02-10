package com.unilibre.dataworks;

// This process consumes from a 901.SQL table queue
// inbound messages contain the table name and the fully qualified path name + table name
// builds the bulk insert command, executes it then deletes the dat file

import com.unilibre.MQConnector.activeMQ;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.ConnectionPool;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SqlCommands;
import com.unilibre.commons.uCommons;
import com.unilibre.core.coreCommons;
import org.json.JSONObject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Properties;

public class InsertWorker {

    private static final String src = NamedCommon.slash+"ins"+NamedCommon.slash;
    private static final String tgt = NamedCommon.slash+"out"+NamedCommon.slash;
    private static final int ONE_MEGABYTE = 1048576;
    private static final int MAX_LOGS=10;
    private static final int MAXTRIES=10;
    private static String table, role, fromQue, logFname, pfx="_";
    private static Path logDir;
    private static DecimalFormat df = new DecimalFormat("#0.00");
    private static int rollcnt=0;
    private static boolean krbCheck=false;

    private static void DestroySelf() {
        uCommons.uSendMessage("["+role+"] worker for table " + table + " completed");
        uCommons.uSendMessage("Exiting queue "+fromQue);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        table = args[0];
        role  = args[1];
        fromQue = "901.SQL.inserts."+table;

        for (int i=0 ; i<3; i++) { System.out.println(" "); }

        String opsys = System.getProperty("os.name");
        if (opsys.toLowerCase().contains("windows")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
            String slash = "/";
            NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
        }
        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);
        krbCheck = NamedCommon.krbDefault;

        Properties sProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/this.server");
        String broker = sProps.getProperty("brokers", "");
        Properties bkrProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/"+broker);
        uCommons.BkrCommons(bkrProps);
        activeMQ.SetTransacted(true);

        for (int i=0 ; i<3; i++) { System.out.println(" "); }

        uCommons.uSendMessage("Listening for tasks on queue: " + fromQue);
        uCommons.uSendMessage("Acting in role of "+role);
        uCommons.uSendMessage("Broker   "+NamedCommon.messageBrokerUrl);
        System.out.println(" ");
        Properties runProps;
        while (!NamedCommon.tConnected) {
            uCommons.uSendMessage("Waiting for target DB connection.");
            uCipher.SetAES(false, "", "");
            runProps = uCommons.LoadProperties("rFuel.properties");
            uCommons.SetCommons(runProps);
            uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
            SqlCommands.ConnectSQL();
            if (NamedCommon.ZERROR) {
                NamedCommon.runSilent = true;
                uCommons.Sleep(10);
            }
            NamedCommon.ZERROR = false;
            ConnectionPool.ConFails.clear();
        }

        ArrayList<String> DDL = new ArrayList<>();

        logFname = NamedCommon.BaseCamp +"/logs/901-SQL-inserts.log";
        logDir   = Paths.get(logFname);
        int idleCnt=0, masterCnt=0, logChk=0, delTry=0;
        boolean okay, destroy=false;
        okay=false;
        long sent, got;
        double laps, div=1000000000.00;
        String message, tableName, fqpn, BulkImport, path2data, fileName, fBytes;
        int wait = 1000;            // 1 second
        int workerIdle = 30;
        int masterIdle = 5;
        activeMQ.SetConsumerWait(wait);
        if (role.toLowerCase().equals("master")) activeMQ.SetPriority(true, 10);
        while (!coreCommons.StopNow()) {
            // activemq.consume waits for message arrival, else returns empty string.
            message = activeMQ.consume(NamedCommon.messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, "InsertDispatcher", fromQue);
            if (message.equals("")) {
                idleCnt++;
                if (idleCnt > workerIdle) {
                    if (role.equals("master")) {
                        masterCnt++;
                        if (masterCnt > masterIdle) destroy=true;
                    } else {
                        destroy=true;
                    }
                }
                if (destroy) DestroySelf();
                continue;
            }
            JSONObject jMessage = new JSONObject(message);
            tableName = jMessage.getString("table");
            fqpn = jMessage.getString("fqpn");
            jMessage = null;
            if (NamedCommon.BaseCamp.equals(NamedCommon.DevCentre)) {
                fqpn = NamedCommon.BaseCamp + fqpn.substring(4, fqpn.length());
            }
            //
            // Create and execute Bulk Inserts
            //
            path2data = fqpn.substring(0, fqpn.indexOf(tableName));
            fileName  = fqpn.substring(path2data.length(), fqpn.length());

            sent = System.nanoTime();
            BulkImport = SqlCommands.BulkImport(fileName, NamedCommon.datPath);
            DDL.add(BulkImport);
            SqlCommands.ExecuteSQL(DDL);
            DDL.clear();
            if (NamedCommon.StopNow.contains("<<PASS>>") && !NamedCommon.ZERROR) {
                File rTmp = new File(fqpn);
                delTry=0;
                rTmp.delete();
                while (rTmp.exists() && delTry < MAXTRIES) {
                    uCommons.Sleep(0);
                    rTmp.delete();
                    delTry++;
                }
                if (rTmp.exists()) {
                    uCommons.uSendMessage("                  : "+fqpn);
                    uCommons.uSendMessage("           ERROR  : file processed BUT will not delete !!");
                    okay=false;
                } else {
                    okay=true;
                }
                rTmp = null;
            } else {
                // put the file back into data/ins - DO NOT LOSE the file !!
                String file = fqpn.replace(tgt, src);
                Path source = Paths.get(fqpn);
                Path target = Paths.get(file);
                if (!InsertCollecter.safeMove(source, target, fileName)) {
                    NamedCommon.ZERROR = true;
                    uCommons.uSendMessage("Tried to move "+fileName);
                    uCommons.uSendMessage("      from    "+source);
                    uCommons.uSendMessage("        to    "+target);
                    uCommons.uSendMessage("It has fallen out of the inserter loop");
                    NamedCommon.ZERROR = false;
                }
            }
            if (okay) {
                got = System.nanoTime();
                laps = (got - sent) / div;
                System.out.println(" ");
//                uCommons.uSendMessage("Loading   " + fqpn);
                uCommons.uSendMessage(NamedCommon.StopNow + "  " + fqpn);
//                uCommons.uSendMessage("           Deleted: "+ uCommons.oconvM(fBytes, "MD0~")+" byte data file.");
                uCommons.uSendMessage("           Runtime: " + df.format(laps) + " seconds");
            }

            activeMQ.Acknowledge();
            idleCnt=0;
            masterCnt=0;
            logChk++;
            if (logChk > 25) {
                if (Files.size(logDir) >ONE_MEGABYTE) rotateLogs();
                logChk=0;
            }
            coreCommons.SlowDown(pfx+table);
        }
    }

    private static void rotateLogs() throws IOException {
        boolean underDEV = false;
        if (underDEV) return;
        // will rotate logFname (held in Path logDir)
        uCommons.uSendMessage("Rotating logs ...");
        Path src, dst;
        int start = MAX_LOGS - 1;
        for (int i = start; i >= 1; i--) {
            src = logDir.resolveSibling(logDir.getFileName() + "." + i);
            dst = logDir.resolveSibling(logDir.getFileName() + "." + (i + 1));
            if (Files.exists(src)) {
//                uCommons.uSendMessage(src.getFileName() + "  -->  "+dst.getFileName());
                Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING);
            }
        }

        src=null;
        dst=null;

//        uCommons.uSendMessage(logDir.getFileName() + "  -->  "+logDir.getFileName()+".1");
        Files.copy(logDir, logDir.resolveSibling(logDir.getFileName() + ".1"), StandardCopyOption.REPLACE_EXISTING);

        // empty the log file
        new FileOutputStream(logDir.toFile()).close(); // Truncates the file

        rollcnt++;
        if (rollcnt > 20) {
            if (krbCheck) CheckKrbTicket();
            rollcnt = 0;
        }
    }

    private static void CheckKrbTicket() {
        Properties rProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);
        boolean krbCheck = NamedCommon.krbDefault;
        if (krbCheck) {
            String krbUser = uCommons.GetValue(rProps, "jdbcUsr", "");
            String krbPass = uCommons.GetValue(rProps, "jdbcPwd", "");
            if (krbUser.contains("\\")) {
                String[] usrParts = krbUser.split("\\\\");
                krbUser = usrParts[usrParts.length - 1];
                usrParts = null;
            }
            MoveData.SetKRBdetails(krbUser, krbPass, krbCheck);
            MoveData.SetKrbControls();
        }
    }

}
