package com.unilibre.tester;


import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.*;
import com.unilibre.MQConnector.activeMQ;
import com.unilibre.MQConnector.artemisMQ;
import com.unilibre.cipher.AESEncryption;
import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.core.MessageProtocol;
import com.unilibre.core.OBMethods;
import com.unilibre.runcontrol.AutoTest;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.jms.JMSException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.LogManager;

import static com.unilibre.MQConnector.commons.inputQueue;


public class debugger {

    private static String conf;
    public static ArrayList<String> messages;
    public static ArrayList<String> containers;
    public static ArrayList<String> reqKeys = new ArrayList<>();
    public static ArrayList<String> reqVals = new ArrayList<>();
    public static ArrayList<ArrayList<String>> valuesV2;
    private static String parent;
    private static boolean repeat = false;
    private static boolean sendReply = false;
    private static boolean sendViaMQ = false;

    public static void main(String[] args) throws InterruptedException {

        LogManager.getLogManager().reset();

        boolean testSSL = false;
        if (testSSL) {
            try {
                UniJava.setIdleRemoveThreshold(60000);    // max session idle time  = 60 seconds
                UniJava.setIdleRemoveExecInterval(15000); // look for idle sessions = 15 seconds
                UniJava.setOpenSessionTimeOut(3000);      // wait 3 seconds for a session to open
                UniJava.setUOPooling(true);
                UniJava.setMinPoolSize(1);
                UniJava.setMaxPoolSize(8);
                UniJava uj = new UniJava();
                UniSession us = null;
                //
                // Use X-Admin to create a crt on the UV host
                //      I usually make the passwords
                //          *   "UniLibre" on the UV host and
                //          *   "passw0rd" on the rFuel host
                // copy the cert to the rFuel host
                //
                // use keytool import it :-
                //      sudo keytool -import -file ./UniLibre.crt -keypass "passw0rd" -alias rfuel \
                //                   -keystore /usr/lib/jvm/java-11-amazon-corretto/lib/security/cacerts \
                //                   -storepass "changeit"
                // Once imported, rFuel can trust the UV host - the import put it in the truststore (cacerts)
                //
                String defaultTrust = "/usr/lib/jvm/java-11-amazon-corretto/lib/security/cacerts";
                uj.openSession(UniObjectsTokens.SECURE_SESSION);
                System.setProperty("javax.net.sslTrustStore", defaultTrust);
                System.setProperty("javax.net.trustStorePassword", "changeit");
                us = uj.openSession();
                us.setHostName("3.27.146.199");
                us.setHostPort(31438);
                us.setAccountPath("/data/uv/RFUEL");
                us.setUserName("rfuel");
                us.setPassword("un1l1br3");
                us.setConnectionString("uvcs");
                us.connect();
            } catch (UniSessionException e) {
                System.out.println("********************************************************************");
                System.out.println("UniSessionException: [" + e.getErrorCode() + "]   " + e.getMessage());
                System.out.println("********************************************************************");
                System.exit(1);
            }
        }

//        RenameDATfiles();
        NamedCommon.slash = "/";
        NamedCommon.dbgPfx = "";
        Properties runProps;
        String result = "";
        String host = "", amqhost="", ext="", dbgDomain="";
        String base = "rfuel14";
        switch (base) {
            case "rfuel14":
                host = "/mnt/rfuel14"; amqhost = "rfuel14"; ext = "/upl/"; dbgDomain = "rfuel14";
                base = "local";
                break;
            case "rfuel22":
                host = "rfuel22\\all"; amqhost = "rfuel22"; ext = "/upl"; dbgDomain = "rfuel22";
                base = "local";
                break;
            case "cdr":
                host = "rfuel22\\all"; amqhost = "rfuel22"; ext = "\\cdr"; dbgDomain = "rfuel22";
                break;
            case "aws-redhat":
                host = "3.25.93.208";  amqhost = host;      ext = "\\upl"; dbgDomain = "rfuel22";
                break;
            case "aws-ubuntu":
                // use ShowInstances to get the IP
                host = "52.64.41.22"; amqhost = "ip-172-31-7-25.ap-southeast-2.compute.internal";      ext = "\\upl"; dbgDomain = "rfuel22";
                break;
        }

        NamedCommon.BaseCamp = host + ext;
        NamedCommon.gmods = NamedCommon.BaseCamp + "lib/";
        NamedCommon.upl = NamedCommon.BaseCamp;
        String slash = "";
        if (NamedCommon.upl.contains("/")) slash = "/";
        if (NamedCommon.upl.contains("\\")) slash = "\\";       // Windows only
        NamedCommon.slash = slash;
        conf = NamedCommon.BaseCamp + slash + "conf" + slash;

        NamedCommon.inputQueue = "debug-Q-inBound";
        NamedCommon.StructuredResponse = "055,050,";
        NamedCommon.messageBrokerUrl = "tcp://"+amqhost+":61616";

        System.out.println(" ");
        System.out.println(" ");
        System.out.println(" ");
        System.out.println("====== This is version 3.0.0 =============");
        System.out.println("rFuel host = "+host);
        System.out.println("amq host   = "+ amqhost);
        System.out.println("basecamp   = " + ext);
        System.out.println("==========================================");
        System.out.println(" ");
        System.out.println(" ");

        Properties rProps = uCommons.LoadProperties(NamedCommon.BaseCamp + "/conf/rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);
        if (NamedCommon.BaseCamp.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
        }

        String junk9 = SourceDB.ConnectSourceDB();

        boolean testGenesis = false;
        if (testGenesis) {
            SqlCommands.ConnectSQL();
//            NamedCommon.xMap = "uBulk/200-Transaction.gog";
            NamedCommon.xMap = "uBulk/000-Test.grp";
            String geneMsg = "task<is>010<tm>" +
                    "sqldb<is>initial<tm>" +
                    "schema<is>lnd<tm>" +
                    "correlationid<is>TEST<tm>" +
                    "replyto<is>019_Finish<tm>" +
                    "dacct<is>RFUEL<tm>" +
                    "RunType<is>incr<TM>" +
                    "proceed<is>true<tm>" +
                    "map<is>" + NamedCommon.xMap + "<tm>";
            uCommons.Message2Properties(geneMsg);
            uCommons.GetMap(NamedCommon.BaseCamp + "/maps/" + NamedCommon.xMap);
            Properties test = uCommons.LoadProperties(NamedCommon.BaseCamp + "/maps/" + NamedCommon.xMap);
            uCommons.DeleteFile(NamedCommon.BaseCamp + "/conf/Genesis-" + test.getProperty("genesis"));
            MessageProtocol.maps = NamedCommon.BaseCamp + "/maps/";
            MessageProtocol.conf = NamedCommon.BaseCamp + "/conf/";

            NamedCommon.BatchID = uCommons.MakeBatchID();
//            NamedCommon.BatchID = new SimpleDateFormat("yyyyMMddHHmmssSSSSSS").format(Calendar.getInstance().getTime());
            MessageProtocol.SetupGenesis(test.getProperty("genesis"));

            if (test.getProperty("grps") == null) test.setProperty("grps", NamedCommon.xMap);
            String[] grps = test.getProperty("grps").split(",");
            String[] maps;
            for (int g = 0; g < grps.length; g++) {
                test = uCommons.LoadProperties(NamedCommon.BaseCamp + "/maps/" + grps[g]);
                maps = test.getProperty("maps").split(",");
                for (int m = 0; m < maps.length; m++) {
                    NamedCommon.xMap = maps[m];
                    uCommons.uSendMessage("   >. " + NamedCommon.xMap);
                    NamedCommon.BatchID = uCommons.MakeBatchID();
//                    NamedCommon.BatchID = new SimpleDateFormat("yyyyMMddHHmmssSSSSSS").format(Calendar.getInstance().getTime());
                    MessageProtocol.ManageGenesis();
                }
            }
            System.exit(1);
        }

        System.out.println(" ");
        System.out.println(" ");
        uCommons.uSendMessage("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*");
        uCommons.uSendMessage("Pre-loading encryption libraries .... this may take a while.");
        String clearText = "test of the encryption factory and iteration vector";
        String encrText = uCipher.Encrypt(clearText);
        String decrText = uCipher.Decrypt(encrText);
        if (!decrText.equals(clearText)) {
            uCommons.uSendMessage("Encryption issue - cannot proceed");
            System.exit(1);
        }
        uCommons.uSendMessage("done.");
        uCommons.uSendMessage("*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*");
        System.out.println(" ");

        Cryptics();

        boolean licChk = false;
        if (licChk) {
            boolean isValid = License.IsValid();
            System.out.println("Licence Test: Pass="+isValid);
        }

//        UniFile uplBP  = u2Commons.uOpen("BP.UPL");
//        if (!NamedCommon.ZERROR) {
//            u2Commons.uWriter(uplBP, "STOP", "");
//        } else {
//            uCommons.uSendMessage(NamedCommon.Zmessage);
//            System.exit(1);
//        }

        NamedCommon.ShowDateTime = true;
        Initalise();

//        StringHandler();
//        System.exit(1);

        String instr = "";
        String shost = "dev-aws";
        NamedCommon.ThisRunKey.clear();
        NamedCommon.ThisRunVal.clear();
        if (!base.equals("local")) NamedCommon.dbgPfx = "public_";

        String pinger = "{\n" +
                "   \"request\": {\n" +
                "      \"task\": \"999\",\n" +
                "      \"--debug\": \"true\",\n" +
                "      \"format\": \"json\",\n" +
                "      \"replyto\": \"059_My_Responses\",\n" +
                "      \"CorrelationID\": \"heartbeat1\"\n" +
                "   }\n" +
                "}";

        String opsys = System.getProperty("os.name");
        uCommons.SetMemory("os.name", opsys);
        uCommons.uSendMessage("Running on " + opsys);
        NamedCommon.SmartMovers = true;
        String task = "014";
        u2Commons.uWriter(u2Commons.uOpen("BP.UPL"), "STOP", "");

        uCommons.SetMemory("domain", dbgDomain);
        uCommons.SetMemory("inhouse", "true");

        repeat = false;
//        if (repeat) BuildMessagePool();
        boolean multiHost = false;
        NamedCommon.isDocker = false;
        NamedCommon.debugging = false;
        boolean autoTest = false;
        if (autoTest) {
            AutoTest.SetStop(true);
            AutoTest.SetDebug(false);
            String[] sendArgs = new String[]{"false", "true", "1", "", "autotest.bkr"};
            AutoTest.main(sendArgs);
            System.exit(0);
        }

        NamedCommon.MessageID = "jDebugger";
        NamedCommon.isWhse = true;
        NamedCommon.isRest = false;
        NamedCommon.Broker = "";
        if (task.startsWith("01")) NamedCommon.Broker = "bulk.bkr";
        if (task.startsWith("02")) NamedCommon.Broker = "kiwibank.bkr";
        if (task.startsWith("05")) {
            NamedCommon.isWhse = false;
            NamedCommon.isRest = true;
            NamedCommon.Broker = "kiwibank.bkr"; //"remote.bkr";
        }
        if (task.startsWith("9")) NamedCommon.Broker = "bulk.bkr";
        if (NamedCommon.Broker.equals("")) System.exit(0);

        instr = "";

        if (task.startsWith("01")) {
            instr = "task<is>"+task+"<tm>" +
                    // Snowflake
//                    "sqldb<is>BANKING_LZ<tm>" +
//                    "schema<is>STG<tm>" +
                    // MSSQL
                "sqldb<is>demo<tm>" +
                "schema<is>lnd<tm>" +
                "correlationid<is>test<tm>" +
                "replyto<is>019_Finish<tm>" +
                "dacct<is>DATA<tm>";
        }

        if (multiHost) instr = "thost<is>rFuel-sql,BIRCU-sql<tm>";

        switch (task) {
            case "010":
                instr = "task<is>010<tm>" +
                        "sqldb<is>initial<tm>" +
                        "schema<is>lnd<tm>" +
                        "correlationid<is>TEST<tm>" +
                        "replyto<is>019_Finish<tm>" +
                        "dacct<is>DATA<tm>" +
                        "RunType<is>incr<TM>" +
                        "proceed<is>false<tm>" +
//                        "map<is>s4/MyTest.grp<tm>";
                        "map<is>s4/TEST.grp";
                break;
            case "012":
                instr = "task<is>012<tm>" +
                        "sqldb<is>initial<tm>" +
                        "schema<is>lnd<tm>" +
                        "correlationid<is>VNDA<tm>" +
                        "replyto<is>019_Finish<tm>" +
                        "dacct<is>RFUEL<tm>" +
                        "RunType<is>refresh<TM>" +
//                        "preuni<is>COPY FROM uLISTS TO SL NO<tm>" +
//                        "u2File<is>CLIENT<tm>" +
//                        "sqlTable<is>CLIENT<tm>" +
//                        "list=uBulk/CLIENT.csv<tm>" +
                        "proceed<is>false<tm>" +
                        "map<is>uBulk/CLIENT.map<tm>";
//                        "map<is>UPLQA/uBulk/014_ClientSplit.grp";
                break;
            case "014":
                instr = "task<is>014<tm>" +
                        "sqldb<is>initial<tm>" +
                        "schema<is>lnd<tm>" +
                        "correlationid<is>TEST<tm>" +
                        "replyto<is>019_Finish<tm>" +
                        "dacct<is>RFUEL<tm>" +
                        "RunType<is>incr<TM>" +
//                        "MAP<IS>S4/CLIENT1.prt";
                        "map<is>uBulk/CLIENT.map<tm>";
                break;
            case "015":
                instr += "RunType<is>REFRESH<tm>" +
                        "map<is>UPLQA/uBulk/010_Tran.map<tm>";
                break;
            case "017":
                instr = "task<is>017<tm>" +
                        "sqldb<is>initial<tm>" +
                        "schema<is>lnd<tm>" +
                        "correlationid<is>TEST<tm>" +
                        "replyto<is>019_Finish<tm>" +
                        "dacct<is>RFUEL<tm>" +
                        "RunType<is>incr<TM>" +
                        "proceed<is>true<tm>" +
                        "map<is>uBulk/full.paytable.map";
                break;
            case "022":
                instr += "task<is>022tm>";
                instr = "sqldb<is>extracts<tm>schema<is>lnd<tm>task<is>022<tm>runtype<is>NRT<tm>correlationid<is>AUTO-Streams<tm>dbhost<is>54.153.142.191<tm>dacct<is>RFUEL<tm>loadts<is>10072023031203<tm>passport<is>not~in~use<tm>issuer<is><tm>item<is>ITEM:4873<tm>map<is>/uStream/MYFILE.map<tm>payload<is>RFUEL<im>MYFILE<im>ITEM:4873<im>Updated 4873 on 07-10-2023  at 03:12:03<tm>";
                break;
            case "025":
                NamedCommon.kafkaAction = "050";
                instr += "task<is>025<tm>";
                instr = "sqldb<is>extracts<tm>schema<is>lnd<tm>task<is>025<tm>runtype<is>NRT<tm>correlationid<is>AUTO-Streams<tm>dbhost<is>54.153.142.191<tm>dacct<is>RFUEL<tm>loadts<is>10072023031203<tm>passport<is>not~in~use<tm>issuer<is><tm>item<is>ITEM:4873<tm>map<is>/uStream/MYFILE.map<tm>payload<is>RFUEL<im>MYFILE<im>ITEM:4873<im>Updated 4873 on 07-10-2023  at 03:12:03<tm>";
                break;
            case "050":
                instr = "task<is>050<tm>\n" +
                        "map<is>uRest/nppNotification.map<tm>\n" +
                        "item<is>10407219<tm>\n" +
//                        "item<is>IamNotOnFile<tm>\n" +
                        "correlationid<is>Test-For-Ben<tm>\n" +
                        "replyto<is>059_Responses<tm>\n" +
//                        "shost<is>NewHost.cfg<tm>\n" +
                        "dacct<is>RFUEL<tm>\n" +
//                        "format<is>json<tm>\n" +
//                        "map<is>dms/CUSTOMER.map<tm>\n" +
//                        "item<is>100436<tm>\n" +
//                        "showlineage<is>true<tm>\n" +
                        "debug<is>false<tm>";

                instr = "{\n" +
                        "    \"request\": {\n" +
                        "        \"task\": \"050\",\n" +
                        "        \"replyto\": \"TestResults\",\n" +
                        "        \"ffformat\": \"xml\",\n" +
                        "        \"correlationid\": \"Andy-001\", \n" +
                        "        \"item\": \"100436\",\n" +
                        "        \"map\": \"mule/GetCustomer\"\n" +
                        "    }\n" +
                        "}";
                // ------------------------------------------------------------------------
                // Group Read tests
                // ------------------------------------------------------------------------
//                String grpTest = "{\n" +
//                        "  \"request\": {\n" +
//                        "    \"task\": \"050\",\n" +
//                        "    \"correlationid\": \"1234\",\n" +
//                        "    \"REPLYTO\": \"059_Responses\",\n" +
//                        "    \"FORMAT\": \"json\",\n" +
//                        "    \"grp\": [\n" +
//                        "        {\"map\": \"OB/v2/GetCustomerId\", \"item\": \"andy\"},\n" +
//                        "        {\"item\": \"andy:100436\", \"map\": \"OB/v2/GetCustomer\"}\n" +
//                        "      ]\n" +
//                        "  }\n" +
//                        "}";
//                instr = grpTest;
//                instr = pinger;
                break;
            case "055":
//                instr = "task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>" +
//                        "Correlationid<is>Create-Test-Record-1-M-M<tm>replyto<is>TestResults<tm>" +
//                        "mscat<is>UPLQA/CreateTestRecord-3.msv<tm>" +
//                        "payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"3\",\"Company\":\"Unilbre\",\"StaffID\":\"1@VM@2\",\"Name\":\"Consultant1@VM@Consultant2\",\"Contact\":\"0400112233@SM@consultant1@unilibre.com.au@VM@0433221100\",\"ContactType\":\"mobile@SM@email@VM@mobile\",\"Location\":\"Brisbane@VM@Perth\",\"State\":\"QLD@VM@WA\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>";
//                instr = jsonifyMessage(instr);
                instr = "{\"request\":{\"task\":\"055\",\"format\":\"json\",\"correlationid\":\"Demo-WRITER\",\"mscat\":\"mule/CUSTOMER.msv\",\"payload\":\"{'InBound':{'ID':'100001','LastName':'BARTZOO','StreetNumber':'19','StreetName':'Kinross Avenue','Suburb':'Kinross Central','Postcode':'4567','GivenNames':'VIANAH SOKHOM','Initials':'VS','Salutation':'MS','DoB':'21 MAY 1985','Gender':'F','ignore':'true'}}\"}}";
                //
                // Version 2 of Writer function
//                instr = "{\"request\":{\"task\":\"055\",\"format\":\"json\",\"correlationid\":\"Demo-WRITER\",\"mscat\":\"mule/CUSTOMER.msv\",\"version \":2,\"payload\":{\"Inbound\":[{\"_FILE\":\"CUSTOMER\",\"_ID\":\"100001\",\"data\":[{\"LastName\":\"BARTZEE\",\"StreetNumber\":\"19\",\"StreetName\":\"Kinross Central\",\"Suburb\":\"Kinross Central\",\"Postcode\":\"4567\",\"GivenNames\":\"VIANAH SOKHOM\",\"Initials\":\"VS\",\"Salutation\":\"MS\",\"DoB\":\"21 MAY 1985\",\"Gender\":\"F\",\"ignore\":\"true\"}]}]}}}";
//                DecodeInstruction("request", instr);
                break;
            case "910":
                instr = "task<is>910<tm>correlationid<is>Load-UDE-Programs<tm>replyto<is>019_Finish<tm>";
                break;
            case "920":
                instr = "task<is>920<tm>shost<is>src/aws2-empty<tm>" +
                        "correlationid<is>Load-rFuel-Programs<tm>replyto<is>019_Answers<tm>";
                break;
            case "930":
                instr = "task<is>930<tm>marker<is>(017) Transaction-FINISHED<tm>";
                break;
            case "990":
                instr = "task<is>990<tm>string<is>u2host=54.153.142.191<nl>\n" +
                        "u2path=/data/uv/DATA<nl>\n" +
                        "u2user=rfuel<nl>\n" +
                        "u2pass=un1l1br3<nl>\n" +
                        "u2acct=DATA<nl>\n" +
                        "dbtype=UNIVERSE<nl>\n" +
                        "protocol=u2cs<nl><tm>replyto<is>019_Finish<tm>";
                instr = "task<is>990<tm>string<is>helloworld<tm>replyto<is>019_Finish<tm>";
                break;
            case "999":
                instr = "task<is>999<tm>";
                instr = "{\"request\":{\"task\":\"999\",\"--debug\":\"true\",\"format\":\"json\",\"replyto\":\"059_CDR_Responses\",\"CorrelationID\":\"heartbeat1\"}}";
                break;
            case "9":
                String[] params = new String[5];
                params[0] = "31438"; //"u2host=uv01";
                params[1] = "54.153.142.191";  //"u2port=31222";
                params[2] = "command=Hello Andy";
                params[3] = "";
                params[4] = "";
                u2Sockets.main(params);
                System.exit(0);
                break;
        }

        if (NamedCommon.debugging) instr += "debug<is>true<tm>";

        ResetMemory();
//        runProps = uCommons.LoadProperties("rFuel.properties");
//        if (NamedCommon.ZERROR) System.exit(0);
//        uCommons.SetCommons(runProps);
//        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        uCipher.SetAES(false, "", "");
//        SqlCommands.DisconnectSQL();

        NamedCommon.Digest = "SHA-256";

//        APImsg.instantiate();
//        uCommons.MessageToAPI(instr);
//        task = APImsg.APIget("task");
//        String item = APImsg.APIget("item");
//        Prepare(task);
//        NamedCommon.RunType = APImsg.APIget("runtype").toUpperCase();
//        NamedCommon.task = task;

        switch (Integer.valueOf(task)) {
            case 91:
                ResetMemory();

                UniFile uvFile = u2Commons.uOpen("BP.UPL");
                UniString uvjunk = u2Commons.uRead( uvFile, "BLAH");

                if (NamedCommon.RunType.equals("REFRESH")) {
                    NamedCommon.aStep = NamedCommon.rStep;
                } else {
                    NamedCommon.aStep = NamedCommon.iStep;
                }

                NamedCommon.tHostList.clear();
                String okay = SourceDB.ConnectSourceDB();
                if (okay.contains("<<FAIL>>")) System.exit(0);
                String inVal, outVal = "", srtn;
                String bpDir = "D:\\WorkSpace\\Upl_Dev\\3.0.0\\BP-UPL-Code\\";
                String rec = uCommons.ReadDiskRecord(bpDir + "uDO.bas");

                srtn = "SRZERO";
                inVal = "LOAD BP.UPL uDO";
                System.out.println(inVal);

                String[] lines = rec.split("\\r?\\n");
                int nbrLines = lines.length;

                inVal = "[FI]";
                System.out.println("    " + outVal);

                inVal = "BASIC BP.UPL uDO";
                System.out.println(inVal);
                System.out.println("    " + outVal);

                inVal = "CATALOG BP.UPL uDO";
                System.out.println(inVal);
                System.out.println("    " + outVal);

                inVal = "uDO -H -K SRZERO";
                System.out.println(inVal);
                System.out.println("    " + outVal);
                break;
            case 90:
                NamedCommon.debugging = true;
                uCommons.StartUp();
                break;
            case 99:
                uCommons.ShutDown();
                break;
            case 19:
                String[] nothing = new String[]{"", ""};
                try {
                    com.unilibre.dataworks.MoveData.main(nothing);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:

                if (NamedCommon.RunType.equals("REFRESH")) {
                    NamedCommon.aStep = NamedCommon.rStep;
                } else {
                    NamedCommon.aStep = NamedCommon.iStep;
                }

//                NamedCommon.tHostList.clear();

                if (NamedCommon.ZERROR) {
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    return;
                }

                uCommons.uSendMessage("*** ");
                if (NamedCommon.ZERROR) System.exit(0);

                Properties Props = uCommons.LoadProperties(conf + NamedCommon.Broker);
                uCommons.BkrCommons(Props);
                if (NamedCommon.ZERROR) System.exit(0);

                NamedCommon.isWebs = false;
                if (!APImsg.APIget("vque").equals("")) NamedCommon.isWebs = true;

                String msgID = NamedCommon.MessageID;
                
                if (!repeat) {
                    long start = System.nanoTime();
                    double laps, div = 1000000000.00;
                    NamedCommon.MessageID = "First-Trip";

                    sendViaMQ = false;

                    if (sendViaMQ) {
                        MessageInOut.messageBrokerUrl = "tcp://192.168.48.107:61616";
                        String procQueue="";
                        switch (task) {
                            case "050":
                                procQueue = "rfuel.read050_001";
                                break;
                            case "055":
                                procQueue = "rfuel.write055_001";
                                break;
                            case "022":
                                procQueue = "VirtualTopic.rFuel";
                                break;
                        }

                        MessageInOut.sendQue = procQueue;
                        JMSProducer jmsp = new JMSProducer();
                        jmsp.PrepareConnector(NamedCommon.messageBrokerUrl, "inbound");
                        if (jmsp.TestConnection(NamedCommon.messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, "debugger", "activemq")) {
//                            jmsp.setup("RFUEL", "debugger");
                            jmsp.sendMsg(instr);
                        }

                    } else {
                        System.out.println(" ");
                        System.out.println("------------------------------------------------------------------------");
                        System.out.println(" ");
                        NamedCommon.que = "1";
                        result = MessageProtocol.handleProtocolMessage(task, "1", instr);
                    }

                    NamedCommon.ZERROR = false;
                    SourceDB.DisconnectSourceDB();
                    long finish = System.nanoTime();
                    laps = (finish - start) / div;
                    System.out.println("--> " + laps + " seconds");
                } else {
                    System.out.println(" ");
                    System.out.println(" ");
                    for (int i=0; i<2; i++) {
                        System.out.println(" ");
                        System.out.println("------------------------------------------------------------------------");
                        System.out.println(" ");
                        NamedCommon.que = "1";
                        result = MessageProtocol.handleProtocolMessage(task, "1", instr);
                    }
                    System.exit(1);
                    Properties bProps = uCommons.LoadProperties("autotest.bkr");
                    ArrayList<String> tasks = new ArrayList<>(Arrays.asList(bProps.getProperty("tasks").split("\\,")));
                    ArrayList<String> queus = new ArrayList<>(Arrays.asList(bProps.getProperty("qname").split("\\,")));
                    String nextBrk = "rest.bkr";
                    String basecamp = NamedCommon.BaseCamp + NamedCommon.slash;
                    conf = basecamp + "conf" + NamedCommon.slash;
                    Properties bkrProps = uCommons.LoadProperties(conf + nextBrk);
                    uCommons.BkrCommons(bkrProps);

                    ArrayList<String> TasksArray = new ArrayList<>(Arrays.asList(bkrProps.getProperty("tasks").split("\\,")));
                    ArrayList<String> qNameArray = new ArrayList<>(Arrays.asList(bkrProps.getProperty("qname").split("\\,")));

                    MessageInOut.messageBrokerUrl = "tcp://192.168.48.107:61616";
                    String procQueue="", message="";
                    int tPos = 0;
                    for (int mm=0; mm < messages.size(); mm++) {
                        instr = messages.get(mm);
                        System.out.println((mm + 1) + " --------------------------------------------------------------------------");
                        if (!NamedCommon.ZERROR) {
                            runProps = uCommons.LoadProperties("rFuel.properties");
                            if (NamedCommon.ZERROR) System.exit(0);
                            uCommons.SetCommons(runProps);

                            if (NamedCommon.RunType.equals("REFRESH")) {
                                NamedCommon.aStep = NamedCommon.rStep;
                            } else {
                                NamedCommon.aStep = NamedCommon.iStep;
                            }

                            if (NamedCommon.ZERROR) System.exit(0);
                            NamedCommon.tHostList.clear();
                            APImsg.instantiate();
                            uCommons.MessageToAPI(instr);
                            task = APImsg.APIget("task");
                            tPos = TasksArray.indexOf(task);

                            procQueue = qNameArray.get(tPos) + "_001";
                            if (task.equals("050")) {
                                procQueue = "rfuel.read050_001";
                            } else {
                                procQueue = "rfuel.write055_001";
                            }

                            NamedCommon.task = task;
                            NamedCommon.MessageID = msgID + "_" + (mm + 1);
                            System.out.println("  CorrID = " + APImsg.APIget("correlationid"));
                            System.out.println(" Message = " + instr);
                            System.out.println(" ");

                            String reply2Q = APImsg.APIget("replyto");

                            message = "";
                            int eom = APImsg.GetMsgSize();
                            for (int api=0 ; api < eom ; api++) {
                                if (!APImsg.APIgetKey(api).equals("")) {
                                    message += APImsg.APIgetKey(api) + "<is>" + APImsg.APIgetVal(api) + "<tm>";
                                }
                            }

                            sendViaMQ = false;

                            if (sendViaMQ) {
//                                Hop.start(instr, "", nextBrk, procQueue, reply2Q, APImsg.APIget("correlationid"));
                                MessageInOut.sendQue = procQueue;
                                JMSProducer jmsp = new JMSProducer();
//                                jmsp.setup("RFUEL", "debugger");
                                jmsp.sendMsg(message);
                                if (NamedCommon.ZERROR) uCommons.uSendMessage(NamedCommon.Zmessage);
                            } else {
                                result = MessageProtocol.handleProtocolMessage(task, "1", instr);
                            }

                            if (sendReply && !result.equals("")) {
                                if (reply2Q.startsWith("temp")) {
                                    Hop.start(result, "", nextBrk, reply2Q, "ACK", APImsg.APIget("correlationid"));
                                } else {
                                    Hop.start(result, "", nextBrk, reply2Q, "", APImsg.APIget("correlationid"));
                                }
                            }
                            System.out.println("  Result = " + result);
                            System.out.println(" ");
                            if (NamedCommon.ZERROR) {
                                System.out.println("Stop due to " + NamedCommon.Zmessage);
                                System.exit(0);
                            }
                            System.out.println(" ");
                            System.out.println(" ");
                            task = "";
                        } else {
                            System.out.println("Stop due to " + NamedCommon.Zmessage);
                            System.exit(0);
                        }
                    }
                    SourceDB.DisconnectSourceDB();
                    System.out.println(" ----------------------------------------------------------------------------");
                }
        }

        /* -------------------------------------------------------------------------------- */

        System.out.println(" ");
        System.out.println("Result = " + result);
        System.out.println(" ");
        System.exit(0);
    }

    private static void StringHandler() {
        long tStart, tFinish;
        double tLaps, tDiv = 1000000000.00;
        // -------------------------------------------------------------
        String raw = "";
        raw = AESEncryption.decrypt(raw, "rfuel22.unilibre.com.au", "20251231");
        String rStr = raw;

        tStart = System.nanoTime();
        UniDynArray myArray = uCommons.SQL2UVRec("879693S4" + NamedCommon.IMark + raw);
        tFinish = System.nanoTime();
        tLaps = (tFinish -tStart) / tDiv;
        System.out.println("SQL2UVRec()----------> " + tLaps + " seconds");
        System.exit(1);
        // -------------------------------------------------------------
        // Which is the most efficent String handlers
        //
        String largeString  = NamedCommon.loAplha + NamedCommon.upAlpha;
        largeString  += largeString ;
        largeString  += largeString ;
        largeString  += largeString ;
        largeString  += largeString ;
        int tMax = largeString.length();
        int rnd = ThreadLocalRandom.current().nextInt(111, tMax);
        String thisPart="";
        StringBuilder sb = new StringBuilder();
        for (int i=0 ; i<8001; i++) {
            thisPart = largeString .substring(0,rnd)+"\r\n";
            sb.append(thisPart.length()+" "+thisPart);
            rnd = ThreadLocalRandom.current().nextInt(275, tMax);
        }
        largeString = sb.toString();
        tMax = largeString.length();

//        ExtractManager.thisReply = largeString;
//        String[] tester;
//        int tot=0;
//        while (!ExtractManager.thisReply.isEmpty()) {
//            System.out.println("reply length "+uCommons.oconvM(String.valueOf(ExtractManager.thisReply.length()), "MD0,"));
//            tester = ExtractManager.GetLines();
//            tot += tester.length;
//            System.out.println("              batch of lines " + tester.length);
//            System.out.println("              lines returned " + tot);
//            tester = null;
//        }
//        System.out.println("all lines returned ");

        Properties testProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(testProps);

        System.out.println(" ------------------------------ String Tests ----------------------------------");

        System.out.println("LargeString: " + uCommons.oconvM(String.valueOf(largeString.length()), "MD0,") +"  bytes");

        boolean encTest = false;
        if (encTest) TestEncryption(largeString);

        sb=null;
        rnd=0;
        thisPart="";
        ArrayList<String> inLines = new ArrayList<>();

        tStart = System.nanoTime();
        String[] sarrLines = largeString.split("\\r?\\n");
        tFinish = System.nanoTime();
        tLaps = (tFinish -tStart) / tDiv;
        System.out.println("Split()----------> " + tLaps + " seconds");
        System.out.println(sarrLines.length+"  lines");
        System.out.println(" ");
        sarrLines = null;

        tStart = System.nanoTime();
        StringTokenizer tokenizer = new StringTokenizer(largeString, "\r?\n");
        String part="";
        while (tokenizer.hasMoreTokens()) {
            part = tokenizer.nextToken();
            inLines.add(part);
        }
        String[] tmpLines = inLines.toArray(new String[inLines.size()]);
        inLines.clear();
        tFinish = System.nanoTime();
        tLaps = (tFinish -tStart) / tDiv;
        tokenizer = null;
        part = "";
        System.out.println("StringTokenizer--> " + tLaps + " seconds");
        System.out.println(tmpLines.length+"  lines");
        System.out.println(" ");

        inLines.clear();
        Scanner scanner = new Scanner(largeString).useDelimiter("\\r?\\n");
        tStart = System.nanoTime();
        while (scanner.hasNext()) {
            part = scanner.next();
            inLines.add(part);
        }
        tFinish = System.nanoTime();
        tLaps = (tFinish -tStart) / tDiv;
        System.out.println("Scanner----------> " + tLaps + " seconds");
        System.out.println(inLines.size()+"  lines");
        System.out.println(" ");

        inLines.clear();
        tStart = System.nanoTime();
        int tFrom = 0;
        for (int i = 0; i < largeString.length(); i++) {
            if (largeString.charAt(i) == '\r' || largeString.charAt(i) == '\n') {
                if (tFrom != i) {
                    inLines.add(largeString.substring(tFrom, i));
                }
                tFrom = i + 1;
            }
        }
        if (tFrom < largeString.length()) inLines.add(largeString.substring(tFrom));
        tFinish = System.nanoTime();
        tLaps = (tFinish -tStart) / tDiv;
        System.out.println("Manual-----------> " + tLaps + " seconds");
        System.out.println(inLines.size()+"  lines");
        System.out.println(" ");
        // -------------------------------------------------------------
    }

    private static void TestEncryption(String largeString) {
        long tStart, tFinish;
        double tLaps, tDiv = 1000000000.00;

        String order = uCipher.keyBoard25, chr="", encSeed="";
        int ri = 0;
        Random rdm = new Random();
        int lx = order.length();
        while (order.length() > 0) {
            ri = rdm.nextInt(lx);
            chr = order.substring(ri, (ri + 1));
            encSeed += chr;
            order = order.substring(0, ri) + order.substring(ri + 1, lx);
            lx = order.length();
        }

        String encString="";
        boolean isLic = License.IsValid();

        uCipher.SetAES(true, NamedCommon.secret, NamedCommon.salt);

        tStart = System.nanoTime();
        encString = AESEncryption.encrypt(largeString, NamedCommon.secret, NamedCommon.salt);
        tFinish = System.nanoTime();
        tLaps = (tFinish -tStart) / tDiv;
        System.out.println("AES256 : " + tLaps);
        String checkr = AESEncryption.decrypt(encString, NamedCommon.secret, NamedCommon.salt);
        if (!checkr.equals(largeString)) {
            System.out.println("AESEncryption failure");
            System.exit(1);
        }
        encString = "";

        tStart = System.nanoTime();
        encString = uCipher.Encrypt(largeString, encSeed);
        tFinish = System.nanoTime();
        tLaps = (tFinish -tStart) / tDiv;
        System.out.println("Encrypt : " + tLaps);
        encString = "";

        tStart = System.nanoTime();
        encString = uCipher.v2Scramble(uCipher.keyBoard25, largeString, encSeed);
        tFinish = System.nanoTime();
        tLaps = (tFinish -tStart) / tDiv;
        System.out.println("v2Scramble : " + tLaps);
    }

    public static void ChangeMessage(String tag, String value) {
        tag = tag.replaceAll("\\=", "");
        APImsg.APIset(tag, value);
    }

    private static void RenameDATfiles() {
        System.out.println("Renaming csv back to dat files");
        boolean isRaw=false;
        NamedCommon.BaseCamp = NamedCommon.DevCentre;
        String fPath = NamedCommon.BaseCamp + "/data/ins/";
        String matchStr = "csv";
        File dir = new File(fPath);
        File[] matchFiles = null;
        String tmp = matchStr, ren;
        uCommons.uSendMessage("looking for " + tmp + "  in " + fPath);
        matchFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(matchStr);
            }
        });
        int nbrmatches;
        if (matchFiles == null) {
            nbrmatches = 0;
        } else {
            nbrmatches = matchFiles.length;
        }
        // make sure the older ones get inserted first.
        Arrays.sort(matchFiles, Collections.reverseOrder());
        String[] matchingFiles = new String[nbrmatches];
        for (int ff = 0; ff < nbrmatches; ff++) { matchingFiles[ff] = ""; }
        tmp = "";
        int fpos = 0;
        for (int ff = 0; ff < nbrmatches; ff++) {
            tmp = String.valueOf(matchFiles[ff]);
            if (isRaw) {
                if (!tmp.contains("raw")) continue;
            } else {
                if (tmp.contains("raw")) continue;
            }
            matchingFiles[fpos] = tmp;
            fpos++;
        }
        uCommons.uSendMessage("   ... found " + (fpos-1));
        int eoi = matchingFiles.length;
        for (int i=0; i<eoi; i++) {
            tmp = matchingFiles[i];
            ren = tmp.replace(".csv", ".dat");
            if (!uCommons.RenameFile(tmp, ren)) continue;
        }
        System.out.println("done.");
    }

    private static void TestXML() {
        String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><payload><Inbound><file>CUSTOMER</file><data><StreetName>Kinross Central</StreetName><Suburb>Kinross Central</Suburb><GivenNames>VIANAH SOKHOM</GivenNames><DoB>21 MAY 1985</DoB><StreetNumber>19</StreetNumber><ignore>true</ignore><ID>100001</ID><LastName>BARTZEE</LastName><Gender>F</Gender><Initials>VS</Initials><Postcode>4567</Postcode><Salutation>MS</Salutation></data></Inbound><Inbound><file>CUSTOMER</file><data><StreetName>Kinross Central</StreetName><Suburb>Kinross Central</Suburb><GivenNames>VIANAH SOKHOM</GivenNames><DoB>21 MAY 1985</DoB><StreetNumber>19</StreetNumber><ignore>true</ignore><ID>100001</ID><LastName>BARTZOO</LastName><Gender>F</Gender><Initials>VS</Initials><Postcode>4567</Postcode><Salutation>MS</Salutation></data></Inbound></payload>";
        Document doc = null;
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            //
            docFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl",true);
            docFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd",false);
            //
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            InputSource xis = new InputSource(new StringReader(xml));
            doc = docBuilder.parse(xis);
        } catch (Exception e) {
            NamedCommon.Zmessage = "***  Malformed XML " + e.getMessage();
            System.exit(1);
        }
        parent = doc.getDocumentElement().getNodeName();
        containers = new ArrayList<>();
        valuesV2   = new ArrayList<ArrayList<String>>();
        DoXMLstuff(doc.getDocumentElement().getChildNodes());
        for (int c=0 ; c < containers.size(); c++) {
            if (valuesV2.get(c).size() == 0) {
                containers.remove(c);
                valuesV2.remove(c);
                c--;
            }
        }
        for (int c=0 ; c < containers.size(); c++) {
            System.out.println(" Node: " + containers.get(c));
            for (int v=0 ; v < valuesV2.get(c).size(); v++) {
                System.out.println("Value: [" + v + "] " + valuesV2.get(c).get(v));
            }
        }
        System.exit(1);
    }

    private static void DoXMLstuff(NodeList nodeList) {
        String context="", value="";
        for (int i = 0; i < nodeList.getLength(); ++i) {
            Node node = nodeList.item(i);
            context = node.getNodeName();
            if (context.equals("#text")) {
                value = node.getNodeValue();
                int pos = containers.indexOf(parent);
                int instance = valuesV2.get(pos).size();
                if (valuesV2.get(pos).size() == 0) valuesV2.set(pos, new ArrayList<>());
                valuesV2.get(pos).add(value);
                continue;
            }
            if (node.hasChildNodes()) {
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    parent = parent + "." + node.getNodeName();
                    if (containers.indexOf(parent) < 0) {
                        containers.add(parent);
                        int pos = containers.indexOf(parent);
                        valuesV2.add(pos, new ArrayList<>());
                    }
                    DoXMLstuff(node.getChildNodes());
                    parent = parent.replace("."+node.getNodeName(), "");
                    continue;
                }
            }
        }
    }

    private static String stringifyMessage(String theMessage) {
        StringBuilder answer = new StringBuilder();
        JSONObject obj = null;
        try {
            obj = new JSONObject(theMessage);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        if (obj == null) return "";
        if (obj.length() < 1) return "";
        String jHeader = "request";
        Iterator<String> jKeys = null;
        try {
            jKeys = obj.getJSONObject(jHeader).keys();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = "";
            try {
                zval = obj.getJSONObject(jHeader).get(zkey).toString();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            answer.append(zkey.toUpperCase() + "<is>" + zval + "<tm>");
        }
        return answer.toString();
    }

    private static void DecodeInstruction(String jHeader, String request) {

        if (request.equals("")) return;

        String zkey;
        String zval;

        JSONObject obj = null;
        try {
            obj = new JSONObject(request);
        } catch (JSONException je) {
            System.out.println(je.getMessage());
            return;
        }
        obj = obj.getJSONObject("request").getJSONObject("payload");

        Iterator<String> jKeys = null;
        jKeys = obj.keys();

//        if (request.contains(jHeader)) {
//            jKeys = obj.getJSONObject(jHeader).keys();
//        } else {
//            jKeys = obj.keys();
//        }

        while (jKeys.hasNext()) {
            zkey = jKeys.next();
//            if (request.contains(jHeader)) {
//                zval = obj.getJSONObject(jHeader).get(zkey).toString();
//            } else {
                zval = obj.get(zkey).toString();
//            }
            if (isJson(zkey, zval)) {
                System.out.println("decode " + zkey);
                parent += zkey + ".";
                DecodeInstruction(zkey, zval);
                parent = parent.replace(zkey + ".", "");
            }
            reqKeys.add(parent + zkey.toLowerCase());
            reqVals.add(zval);
        }
        obj = null;
    }

    private static boolean isJson(String inKey, String inval) {
        try {
            Object oo = new JSONObject(inval);
            if (oo instanceof JSONObject) return true;
        } catch (JSONException e) {
            //
        }
//        try {
//            Object oa = new JSONArray(inval);
//            if (oa instanceof JSONArray) return true;
//        } catch (JSONException e) {
//            //
//        }
        return false;
    }

    public static void BuildMessagePool() {
        messages = new ArrayList<>();
        // 0
        messages.add("task<is>055<tm>Correlationid<is>Clear-Test-Files-SRTN<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/PrepareTestFiles.msv<tm>payload<is><tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        // 1 - 10
        messages.add("task<is>055<tm>format<is>json<tm>Correlationid<is>Create-uCATALOG-uCLRTEST<tm>replyto<is>TestResults<tm>use.$file$<is>uCATALOG<tm>mscat<is>UPLQA/AllPurposeUpdater.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"uCLRTEST\",\"RECORD\":\"exec-x-CLR.TESTFILES\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>format<is>json<tm>Correlationid<is>Create-VOC-CLR.TESTFILES<tm>replyto<is>TestResults<tm>use.$file$<is>VOC<tm>mscat<is>UPLQA/AllPurposeUpdater.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"CLR.TESTFILES\",\"RECORD\":\"PA@FM@HUSH ON@FM@CLEAR.FILE TEST.DATA@FM@CLEAR.FILE TEST.DATA2@FM@HUSH OFF\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>TRUE<tm>\n");
        messages.add("task<is>055<tm>Correlationid<is>Clear-Test-Files-EXEC<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/ClearTestFiles.msv<tm>payload<is><tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\": {\"BaseData\": {\"ID\": \"1\",\"Firstname\": \"MyFirstName\",\"Email\": \"create.me@unilibre.com.au\",\"DoB\": \"12-11-1971\",\"Phone\": \"0400112233\",\"Surname\": \"MyFamilyName\",\"CardNumber\": \"1111222233334444\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>TRUE<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA2<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-JOIN-Record<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\": {\"BaseData\": {\"ID\": \"1\",\"Surname\": \"Join has worked\",\"Email\": \"\",\"DoB\": \"\",\"Phone\": \"\",\"Firstname\": \"100@VM@200@VM@300@VM@400\",\"CardNumber\": \"\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record-2<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord-2.msv<tm>payload<is>{\"InBound\": {\"BaseData\": {\"ID\": \"2\",\"Alias\": \"0412123456@VM@my.email@payid.com.au\",\"Status\": \"ACTIVE@VM@INACTIVE\",\"Type\": \"TYPE1@VM@TYPE2\",\"Method\": \"sms@VM@email\",\"StatusReason1\": \"InUse@VM@Closed\",\"StatusReason2\": \"@VM@\",\"Account\": \"S33@VM@S11\",\"BSB\": \"880-601@VM@880-601\",\"ExtAccount\": \"123456@VM@654321\",\"cDate\": \"2018-01-20@VM@2018-01-21\",\"cTime\": \"10:01@VM@09:10\",\"lastUsedDate\": \"2018-05-12@VM@2018-05-13\",\"lastUsedTime\": \"15:25@VM@11:20\",\"xferDate\": \"2018-05-12@VM@2018-05-13\",\"xferTime\": \"15:20@VM@11:15\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record-1-M-M<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord-3.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"3\",\"Company\":\"Unilbre\",\"StaffID\":\"1@VM@2\",\"Name\":\"Consultant1@VM@Consultant2\",\"Contact\":\"0400112233@SM@consultant1@unilibre.com.au@VM@0433221100\",\"ContactType\":\"mobile@SM@email@VM@mobile\",\"Location\":\"Brisbane@VM@Perth\",\"State\":\"QLD@VM@WA\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record-5<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord-2.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"5\",\"Alias\":\"0412123456\",\"Status\":\"ACTIVE\",\"Type\":\"TYPE1\",\"Method\":\"sms\",\"StatusReason1\":\"InUse\",\"StatusReason2\":\"\",\"Account\":\"S33\",\"BSB\":\"880-601\",\"ExtAccount\":\"123456\",\"cDate\":\"2018-01-20\",\"cTime\":\"10:01\",\"lastUsedDate\":\"2018-05-12\",\"lastUsedTime\":\"15:25\",\"xferDate\":\"2018-05-12\",\"xferTime\":\"15:20\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-Test-Record-11<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"11\",\"Firstname\":\"MyFirstName\",\"Email\":\"create.me@unilibre.com.au\",\"DoB\":\"12-11-1971\",\"Phone\":\"0400112233\",\"Surname\":\"MyFamilyName\",\"CardNumber\":\"1111222233334444\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>format<is>json<tm>Correlationid<is>Create-PUID-Test-Record<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"50\",\"Firstname\":\"Given1@VM@Given2@VM@Given3@VM@Given4\",\"DoB\":\"PUID1@VM@PUID2\",\"Surname\":\"Surname1@VM@Surname2\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        // 11 - 20
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/ReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>correlationid<is>1stReadTest<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>-1<tm>format<is>json<tm>Correlationid<is>Minus-1-Append<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"1\",\"Firstname\":\"MyBasicAppend\",\"Email\":\"append@unilibre.com.au\",\"DoB\":\"12-11-1972\",\"Phone\":\"0400445566\",\"Surname\":\"AnotherFamily\",\"CardNumber\":\"11221133113441155\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/ReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>use.$sv$<is>N<tm>use.$av$<is>N<tm>correlationid<is>ReadAfterAppend<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$mv$<is>1<tm>u2p.subs.vm<is>~VM~<tm>format<is>json<tm>Correlationid<is>Insert-Values-Test<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/CreateTestRecord.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"1\",\"Firstname\":\"~VM~MyInsertedName\",\"Email\":\"~VM~inserted@unilibre.com.au\",\"DoB\":\"~VM~12-11-1972\",\"Phone\":\"~VM~0400778899\",\"Surname\":\"~VM~\",\"CardNumber\":\"~VM~11229933993449955\"}}}<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/ReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>correlationid<is>ReadAfterInsert-NOassoc-Dense<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/ReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>correlationid<is>ReadAfterInsert-NOassoc-Sparse<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>sparse<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/assocReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>correlationid<is>ReadAfterInsert-Assoc-Sparse<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>sparse<is>true<tm>\n");
        messages.add("task<is>050<tm>format<is>xml<tm>map<is>UPLQA/uRest/assocReadTestRecord.map<tm>item<is>1<tm>use.$mv$<is>N<tm>correlationid<is>ReadAfterInsert-Assoc-Dense<tm>showlineage<is>true<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>sparse<is>false<tm>\n");
        messages.add("task<is>050<tm>use.$MV$<is>1<tm>replyto<is>TestResults<tm>correlationID<is>BasicRead-Text<tm>map<is>UPLQA/uRest/BasicText.map<tm>item<is>11<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>use.$MV$<is>1<tm>replyto<is>TestResults<tm>correlationID<is>BasicRead-XML<tm>map<is>UPLQA/uRest/BasicRead-XML.map<tm>item<is>11<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        // 21 - 30
        messages.add("task<is>050<tm>use.$MV$<is>1<tm>replyto<is>TestResults<tm>format<is>json<tm>correlationID<is>BasicRead-JSON<tm>map<is>UPLQA/uRest/BasicRead-JSON.map<tm>item<is>11<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Nested-1-1<tm>map<is>UPLQA/uRest/Nested.map<tm>item<is>5<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Nested-1-m<tm>map<is>UPLQA/uRest/Nested.map<tm>item<is>2<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Nested-1-m-m-Dense<tm>map<is>UPLQA/uRest/Nested1MM.map<tm>item<is>3<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Nested-1-m-m-Sparse<tm>map<is>UPLQA/uRest/Nested1MM.map<tm>item<is>3<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>sparse<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>055<tm>sHost<is>src/aws2-rfuel<tm>correlationID<is>BasicExecute<tm>replyto<is>TestResults<tm>format<is>xml<tm>mscat<is>UPLQA/SubrTest.msv<tm>payload<is><InBound><BaseData><ID>11</ID><Surname>Unilibre</Surname></BaseData></InBound><tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>formt<is>xml<tm>map<is>UPLQA/uRest/ReadFunctionTest.map<tm>item<is>1<tm>use.$mv$<is>1<tm>sparse<is>true<tm>showlineage<is>false<tm>replyto<is>TestResults<tm>wraptask<is>true<tm>correlationid<is>ReadFunctionTest<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>\n");
        messages.add("task<is>050<tm>sHost<is>src/aws2-rfuel<tm>sparse<is>true<tm>replyto<is>TestResults<tm>correlationID<is>SparseValues<tm>map<is>UPLQA/uRest/SparseValues.map<tm>item<is>50<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>050<tm>wraptask<is>false<tm>sHost<is>src/aws2-data<tm>replyto<is>TestResults<tm>correlationID<is>Cross-Account-Reader<tm>map<is>UPLQA/uRest/UF0001-Reader.map<tm>item<is>1299<tm>u2p.dirty.updates<is>1<tm>u2p.allow.append<is>true<tm>showlineage<is>true<tm>sparse<is>true<tm>\n");
        messages.add("task<is>055<tm>format<is>xml<tm>mscat<is>UPLQA/DirtyUpdate.msv<tm>correlationID<is>Dirty-Updates-off-xml<tm>replyTo<is>TestResults<tm>debug<is>false<tm>payload<is><?xml version=\"1.0\" encoding=\"UTF-8\"?><Heartbeat><ID>pulse-6ef51311-c763-4df6-bc7d-a6af942ccad4</ID><date>2018-10-17</date><time>15:08:53</time></Heartbeat><tm>u2p.dirty.updates<is>0<tm>u2p.allow.append<is>false<tm>\n");
        // 31 - 40
        messages.add("task<is>055<tm>format<is>xml<tm>mscat<is>UPLQA/DirtyUpdate.msv<tm>correlationID<is>Dirty-Updates-off-json<tm>replyTo<is>TestResults<tm>debug<is>false<tm>payload<is>{\"Heartbeat\":{\"ID\":\"pulse-6ef51311-c763-4df6-bc7d-a6af942ccad4\",\"date\":\"2018-10-17\",\"time\":\"15:08:53\"}}<tm>u2p.dirty.updates<is>0<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>055<tm><tm>format<is>json<tm>Correlationid<is>Subroutine-Q-Pointer<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/Q-Pointer-SRTN.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"Account\":\"DATA\",\"File\":\"CLIENT\",\"Qpointer\":\"A.CRAZY.QPOINTER\",\"ID\":\"51\"}}}<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$op$<is>+<tm>format<is>json<tm>Correlationid<is>Valid-Math(+)-Write<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/MathTester.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"51\",\"Value\":\"323\"}}}<tm>u2p.dirty.updates<is>true<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Valid-Math(+)-Read<tm>map<is>UPLQA/uRest/MathReader.map<tm>item<is>51<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$op$<is>-<tm>format<is>json<tm>Correlationid<is>alid-Math(-)<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/MathTester.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"51\",\"Value\":\"111\"}}}<tm>u2p.dirty.updates<is>true<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Valid-Math(-)<tm>map<is>UPLQA/uRest/MathReader.map<tm>item<is>51<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$op$<is>*<tm>format<is>json<tm>Correlationid<is>MathTester<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/MathTester.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"51\",\"Value\":\"14\"}}}<tm>u2p.dirty.updates<is>true<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Valid-Math(*)<tm>map<is>UPLQA/uRest/MathReader.map<tm>item<is>51<tm>wraptask<is>false<tm>\n");
        messages.add("task<is>055<tm>use.$file$<is>TEST.DATA<tm>use.$op$<is>/<tm>format<is>json<tm>Correlationid<is>MathTester<tm>replyto<is>TestResults<tm>mscat<is>UPLQA/MathTester.msv<tm>payload<is>{\"InBound\":{\"BaseData\":{\"ID\":\"51\",\"Value\":\"14\"}}}<tm>u2p.dirty.updates<is>true<tm>u2p.allow.append<is>false<tm>\n");
        messages.add("task<is>050<tm>replyto<is>TestResults<tm>correlationID<is>Valid-Math(*)<tm>map<is>UPLQA/uRest/MathReader.map<tm>item<is>51<tm>wraptask<is>false<tm>");
    }

    private static void Initalise() {
    }

    private static void ResetMemory() {
        // ---------------------------------------------------------------- //
        // Any change here needs to also go into responder.ResetMemory()    //
        // ---------------------------------------------------------------- //
        NamedCommon.Reset();
    }

    private static void Prepare(String task) {
        if (!task.startsWith("05")) return;
        System.out.println("Preparing http-return-codes");
        if (NamedCommon.ReturnCodes.isEmpty()) { uCommons.SetupReturnCodes(); }
    }

    public static String[] ReadFileNames(String indir, String inext) {
        String lookin = NamedCommon.BaseCamp + indir;
        final String matchStr = inext;
        if (NamedCommon.debugging) uCommons.uSendMessage("Look in " + lookin + "  for " + matchStr);
        File dir = new File(lookin);
        File[] matchFiles = dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(matchStr);
            }
        });
        if (matchFiles == null) return null;
        int nbrmatches = matchFiles.length;
        String[] matchingFiles = new String[nbrmatches];
        for (int ff = 0; ff < nbrmatches; ff++) {
            matchingFiles[ff] = String.valueOf(matchFiles[ff]);
        }
        return matchingFiles;
    }

    private static void Cryptics() {
        //
        String badchars = "/\\,@#$%^&";
        String host = "";
        String expyr= "20270215";       // "20270215";
        String vers = "3";              // "DEV";
        String prds = "***";            // 3  can use all 3 products
        String raw  = host + "*" + expyr + "*" + vers + "*" + prds;
        String lic  = "";
        String dsd  = "";
        String rfEncMe = "";
        String rfDecMe = "";
        String encrytMe = "";
        String decyptMe = "";
        String secret = "";
        String salt   = "";
        String scrambleString = "";
        // ***********************************************************************
        // keyBoard18 is for Heritage Licences ( anything before v2.6.x )       //
        // uCipher.keyBoard = uCipher.keyBoard18;                               //
        // keyBoard25 is for non-Heritage licences ( anything > 2.5.24  )       //
        // ***********************************************************************
        uCipher.keyBoard    = uCipher.keyBoard25;
        String[] encPgms    = {"CLEAR.TAKE.LOADED.bas"};
        boolean encBP       = true;
        boolean encDir      = false;
        boolean decBP       = false;
        boolean doChkSum    = false;
        boolean tempQueTest = false;
        boolean generateSS  = false;

        if (!scrambleString.equals("")) {
            long stMS = System.nanoTime();
            String encSeed      = uCipher.GetCipherKey();
            String secureString = uCipher.v2Scramble(uCipher.keyBoard, scrambleString, encSeed);
            String clearText    = uCipher.v2UnScramble(uCipher.keyBoard, secureString, encSeed);
            long fiMS = System.nanoTime();

            System.out.println("In String   : " + scrambleString);
            System.out.println(" Passport   : " + encSeed);
            System.out.println("  Scrambl   : " + secureString);
            System.out.println("UnScrambl   : " + clearText);
            System.exit(1);
        }

        if (tempQueTest) {
            NamedCommon.artemis = false;
            if (NamedCommon.artemis) {
                NamedCommon.messageBrokerUrl = "tcp://192.168.48.51:61616";
            }
            String queue = "000-HealthCheck"; // "rFuel/queue/010";
            boolean okay = false;
            String reply = "", message = "MQ-Broker-RU-There";

            uCommons.uSendMessage(" Sending: " + message);
            try {
                inputQueue = NamedCommon.inputQueue;
                if (!NamedCommon.artemis) {
                    activeMQ.SetDeliveryMode(2);
                    activeMQ.SetExpiry(NamedCommon.Expiry);
                    okay = activeMQ.produce(NamedCommon.messageBrokerUrl, "admin", "admin", "NAME", queue, message);
                } else {
                    okay = artemisMQ.produce(NamedCommon.messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, "NAME", queue, message);
                }
                inputQueue = "";
            } catch (JMSException e) {
                e.printStackTrace();
            }

            try {
                if (!NamedCommon.artemis) {
                    reply = activeMQ.consume(NamedCommon.messageBrokerUrl, "admin", "admin", "ACK", queue);
                } else {
                    reply = artemisMQ.consume(NamedCommon.messageBrokerUrl, NamedCommon.bkr_user, NamedCommon.bkr_pword, "ACK", queue);
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
            uCommons.uSendMessage("Received: " + reply);
            if (!reply.equals(message)) {
                uCommons.uSendMessage("FAIL");
            } else {
                uCommons.uSendMessage("PASS");
            }
            System.exit(0);
        }

        if (!raw.equals("") && !host.equals("")) {
            System.out.println(" ");
            System.out.println(" ");
            System.out.println(" ");
            System.out.println(" ");
            String env, rlic, rchk;
            if(raw.contains(",")) {
                String[] arrRaw = raw.split(",");
                for (int i=0; i< arrRaw.length; i++) {
                    raw = arrRaw[i];
                    env = "*DEV*";
                    if (raw.startsWith("ocp-dc")) env = "*PROD*";
                    raw += "*20230331" + env + "2";
                    System.out.println("raw=" + raw);
                    rlic = uCipher.Encrypt(raw);
                    rchk = uCipher.Decrypt(rlic);
                    System.out.println("lic=" + rlic);
//                    System.out.println("chk=" + rchk);
                    System.out.println(" ");
                    System.out.println("---------------------------------------------------------------------------------------------------------------------");
                    System.out.println(" ");
                }
            } else {
                System.out.println("raw=" + raw);
                while (true) {
//                    uCipher.SetAES(true, secret, salt);
                    rlic = uCipher.Encrypt(raw);
                    rchk = uCipher.Decrypt(rlic);
//                    if (uCipher.GetAES()) {
//                        int lx = badchars.length();
//                        String chr;
//                        boolean redo = false;
//                        for (int i = 0; i < lx; i++) {
//                            chr = (badchars + " ").substring(i, i + 1);
//                            if (rlic.indexOf(chr) >= 0) {
//                                System.out.println("Bad character [" + chr + "]  doing it again.");
//                                redo = true;
//                                break;
//                            }
//                        }
//                        if (!redo) break;
//                    } else {
//                        break;
//                    }
                    break;
                }
                System.out.println("lic=" + rlic);
                System.out.println("chk=" + rchk);
                System.out.println();
                System.out.println("Secret: " + NamedCommon.secret);
                System.out.println("  Salt: " + NamedCommon.salt);
            }
            System.exit(0);
        }

        if (!lic.equals("")) {
            System.out.println(" ");
            System.out.println(" ");
            System.out.println(" ");
            System.out.println(" ");
            String enc = lic.substring(lic.indexOf("=") + 1, lic.length());
            String chk = uCipher.Decrypt(enc);
            System.out.println("lic=" + enc);
            System.out.println("chk=" + chk);
            System.out.println(" ");
            System.out.println(" ");
            System.out.println(" ");
            System.out.println(" ");
            System.exit(0);
        }

        if (!encrytMe.equals("")) {
            String enc = uCipher.Encrypt(encrytMe);
            String dec = uCipher.Decrypt(enc);
            System.out.println(" input=" + encrytMe);
            System.out.println("output=" + enc);
            System.out.println(" check=" + dec);
            System.exit(0);
        }

        if (!decyptMe.equals("")) {
            String dec = uCipher.Decrypt(decyptMe);
            String enc = uCipher.Encrypt(dec);
            System.out.println(" input=" + decyptMe);
            System.out.println("output=" + dec);
            System.out.println(" check=" + enc);
            System.exit(0);
        }

        if (!rfEncMe.equals("")) {
            System.out.println("Encrypting ------------------------------------------------");
            uCipher.SetAES(false,"","");
            String enc = uCipher.Encrypt(rfEncMe);
            String dec = uCipher.Decrypt(enc);
            System.out.println(" input= " + rfEncMe);
            System.out.println("output= ENC(" + enc + ")");
            System.out.println(" check= " + dec);
            rfDecMe = "ENC("+enc+")";
        }

        if (!rfDecMe.equals("")) {
            System.out.println("Decrypting ------------------------------------------------");
            uCipher.SetAES(false,"","");
            String dec = uCipher.Decrypt(rfDecMe);
            String enc = uCipher.Encrypt(dec);
            System.out.println(" input=" + rfDecMe);
            System.out.println("output=" + dec);
        }

        if (!rfEncMe.isEmpty() || !rfDecMe.isEmpty()) System.exit(1);

        if (!(secret+salt).equals("")) {
            uCipher.SetAES(false, "", "");
            String encSecret = uCipher.Encrypt(secret);
            String encSalt   = uCipher.Encrypt(salt);
            uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
            System.out.println("Secret  raw: " + secret);
            System.out.println("        enc: " + encSecret);
            System.out.println("   Salt raw: " + salt);
            System.out.println("        enc: " + encSalt);
            System.exit(1);
        }

        if (!dsd.equals("")) {
            uCommons.GetMap(dsd);
            dsd = OBMethods.DebugDSD();
            System.out.println("DSD: " + dsd);
            System.exit(0);
        }

        String junk = "";
        if (!junk.equals("")) {
            String rfVersion = "3.0.0";
            String obfusVer = "300";
            String bpraw = "D:\\WorkSpace\\Upl_Dev\\" + rfVersion + "\\BP-UPL-Code";
            String bpenc = "D:\\RunnableJars\\Deploy_Obfuscated_" + obfusVer + "\\BP.UPL";
            String enc = uCommons.ReadDiskRecord(bpenc + "\\" + junk);
//            enc = uCipher.Encrypt(enc);
            String chk = uCipher.Decrypt(enc);
            System.out.println(chk);
            System.exit(0);
        }

        if (encDir) {
            String srcDir = "D:\\RunnableJars\\encSource";
            String tgtDir = "D:\\RunnableJars\\encTarget";
            System.out.println(" ");
            System.out.println(" ");
            System.out.println("Everything in " + srcDir + " will be encrypted and written to " + tgtDir);
            System.out.println("Read from " + srcDir);
            System.out.println("Encryt to " + tgtDir);

            String[] items;
            items = uCommons.ReadDiskFiles(srcDir, "");
            int nbrPgms = items.length, maxLen=0;
            if (nbrPgms == 0) System.exit(1);

            String itemid="", strRecord, encRecord;
            for (int p=0 ; p < nbrPgms ; p++) {
                itemid = items[p];
                if (itemid.length() > maxLen) maxLen = itemid.length();
            }
            maxLen += 3;

            for (int p=0 ; p < nbrPgms ; p++) {
                itemid = items[p];
                strRecord = uCommons.ReadDiskRecord(srcDir+"\\"+itemid);
                encRecord = uCipher.Encrypt(strRecord);
                if (!encRecord.startsWith("ENC(")) encRecord = "ENC(" + encRecord + ")";
                if (uCommons.WriteDiskRecord(tgtDir+"\\"+itemid, encRecord)) {
                    System.out.println("      .) " + uCommons.LeftHash(itemid, maxLen) + " PASS");
                } else {
                    System.out.println("      .) " + uCommons.LeftHash(itemid, maxLen) + " FAIL");
                }
            }
            System.exit(0);
        }

        if (encBP) {
            BufferedWriter bWriter = null;
            String extn;
            String rfVersion = "3.1.1";
            String obfusVer = rfVersion.replaceAll("\\.", "");
            String bpraw, bpFile;
            String[] pFiles = new String[3];
            pFiles[0] = "BP-UPL";
            pFiles[1] = "";
            pFiles[2] = "";
            System.out.println(" ");
            for (int bp=0 ; bp < 3 ; bp++) {
                if (pFiles[bp].equals("")) continue;
                System.out.println("Obfuscating "+pFiles[bp]);
            }
            System.out.println(" ");
            uCipher.SetAES(false, "", "");

            for (int bp=0 ; bp < 3 ; bp++) {
                bpFile = pFiles[bp];
                if (bpFile.equals("")) continue;
                System.out.println(" ");
                System.out.println(" ");
                System.out.println("-----------------------------------------------------------------------");
                String bpPath = "/home/andy/rFuel/DEV/";
                String deploy = "/home/andy/rFuel/Deploy/";
                bpraw = bpPath +  rfVersion + "/dbProgs/" + bpFile;
                String bpenc = "";
                bpenc = deploy + rfVersion + "/lib";
                String[] pgms;

                System.out.println("Read from " + bpraw);
                System.out.println("Encryt to " + bpenc);

                List<String> changes = new ArrayList<>();

                // ----------- Get last update date and make LONG -----------------------------------------

                long lastEncryptionRun;
                String rec = uCommons.ReadDiskRecord("./dbProgs/lastEncrypt.txt"); // load from current directory
                if (rec.isEmpty()) rec = "01-01-2025";
                rec = rec.replaceAll("\\r?\\n", "");
                System.out.println("Last encryption: "+rec+"  *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-");
                DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd-MM-yyyy");
                LocalDate date = LocalDate.parse(rec, fmt);
                lastEncryptionRun = date.atStartOfDay(ZoneId.systemDefault()) .toInstant() .toEpochMilli();
                NamedCommon.ZERROR = false; NamedCommon.Zmessage = "";

                // --------------------- Globally catalogued items ----------------------------------------

                extn = ".gct";
                System.out.println("1  .) Find *" + extn + " to encrypt");
                pgms = uCommons.GetChangedItems(bpraw, extn, lastEncryptionRun);
                EncryptProgs(bWriter, pgms, bpraw, bpenc);
                for (String item : pgms) {
                    changes.add(item);
                }

                // --------------------- BASIC programs ---------------------------------------------------

                extn = ".bas";
                System.out.println("2  .) Find *" + extn + " to encrypt");
                if (encPgms.length==0) {
                    pgms = uCommons.GetChangedItems(bpraw, extn, lastEncryptionRun);
                } else {
                    pgms = encPgms;
                }
                EncryptProgs(bWriter, pgms, bpraw, bpenc);
                for (String item : pgms) {
                    changes.add(item);
                }

                // --------------------- INSERT items -----------------------------------------------------

                extn = ".ins";
                System.out.println("3  .) Find *" + extn + " to encrypt");
                if (encPgms.length==0) {
                    String[] insrt = uCommons.GetChangedItems(bpraw, extn, lastEncryptionRun);
                    EncryptProgs(bWriter, insrt, bpraw, bpenc);
                    for (String item : pgms) {
                        changes.add(item);
                    }
                }
                System.out.println(" ");
                System.out.println("Loaded: " + bpenc);
                System.out.println(" ");

                Scanner scanner = new Scanner(System.in);
                System.out.print("Copy to s3://core/rfuel/3.1.1/staging (Y/N) ");
                String myans =  scanner.nextLine().trim().toUpperCase();
                if ("Y".equals(myans)) {
                    String cmd = "touch ./dbProgs/last_encrypt_marker";
                    uCommons.nixExecute(cmd, false);
                    System.out.println("last_encrypt_marker  has been set");
                    String s3Path = "/home/andy/s3rfuel/core/"+rfVersion+"/staging";
                    System.out.println(" ");
                    System.out.println("Copy from: "+bpenc);
                    System.out.println("       to: "+s3Path);

                    System.out.println("  NOTE   : This may take time to copy !! ");

                    for (String item : changes) {
                        try {
                            Files.copy(
                                    Paths.get(bpenc, item),
                                    Paths.get(s3Path, item),
                                    StandardCopyOption.REPLACE_EXISTING
                            );
                            System.out.println("Copyied    "+item+"    to " + s3Path);
                        } catch (IOException e) {
                            System.out.println(e.getMessage());
                        }
                        System.out.println("");
                        System.out.println("find " + s3Path + " \\");
                        System.out.println("    -type f -newer ./dbProgs/last_encrypt_marker \\");
                        System.out.println("    -exec cp -v {} ~/s3rfuel/customers/kiwibank/dlv/ \\;");

                    }
                } else if ("N".equals(myans)) {
                    System.out.println("Exiting...");
                    System.exit(0);
                } else {
                    System.out.println("Invalid input. Please enter Y or N.");
                }
            }
            System.out.println("Done ");
        }

        if (decBP) {
            BufferedWriter bWriter = null;
            String rfVersion = "3.1.1";

            String bpreco = "/home/andy/rFuel/Deploy/" + rfVersion + "/lib/";
            String bphold = "/home/andy/rFuel/Deploy/" + rfVersion+ "/hold";

            String[] pgms = new String[]{"uDO.bas"};
            String item, prog;
            uCipher.SetAES(false, "", "");
            int eop = pgms.length;

            for (int p = 0; p < eop; p++) {
                item = pgms[p];
                System.out.println(item);
                prog = uCommons.ReadDiskRecord(bpreco + item);
                if (prog.startsWith("ENC(")) {
                    while (prog.endsWith("\n")) { prog = prog.substring(0, prog.length() - 1); }
                    prog = prog.substring(4, prog.length());
                    prog = prog.substring(0, (prog.length() - 1));
                    prog = uCipher.Decrypt(prog);
                }
                bWriter = uCommons.GetOSFileHandle(bphold + "/" + item);
                try {
                    bWriter.write(prog);
                    bWriter.flush();
                    bWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            }
            System.out.println("Done " + eop);
            System.exit(0);
        }

        if (encBP || decBP) System.exit(0);

        if (doChkSum) {
            // sha256sum
        }
    }

    private static void OldLicence(String inVal) {
        ArrayList<Integer> range = new ArrayList<>();
        String original = "", order = "";
        String encString = "", chr = "",  tilde = "~";
        String license = inVal;
        if (license.equals(""))  System.exit(0);

        encString = uCommons.FieldOf(license, tilde, 2);
        license   = uCommons.FieldOf(license, tilde, 1);

        int lx = license.length();
        for (int rr = 65; rr < 91; rr++) { range.add(rr); }
        for (int rr = 97; rr < 123; rr++) { range.add(rr); }
        for (int rr = 48; rr < 58; rr++) { range.add(rr); }
        range.add(46);
        range.add(95);
        range.add(42);
        range.add(45);
        range.add(32);
        int eor = range.size(), chx = 0;
        for (int rr = 0; rr < eor; rr++) {
            chx = range.get(rr);
            order += Character.toString((char) (int) chx);
        }
        original = order;

        int a1 = encString.length();
        int a2 = original.length();
        int a3 = license.length();

        int chk = 0, pos = 0;
        String licChk = "";
        for (int x = lx; x > 0; x--) {
            chr = license.substring(x - 1, x);
            if (!chr.equals(",") && chk != 4) {
                pos = encString.indexOf(chr);
                licChk = original.substring(pos, pos + 1) + licChk;
                chk++;
            } else {
                chk = 0;
            }
        }
        String LicDom = uCommons.FieldOf(licChk, "\\*", 1);
        if (licChk.contains(NamedCommon.noLic)) {
            System.out.println("Yup - that works!");
        } else {
            System.out.println("HBL=" + licChk);
        }
    }

    private static void EncryptProgs(BufferedWriter bWriter, String[] pgms, String bpraw, String bpenc) {
        String item, prog;
        int eop = pgms.length;
        for (int p = 0; p < eop; p++) {
            item = pgms[p];

            System.out.println(String.format("%4d", p+1) + " .) " + item);

            prog = uCommons.ReadDiskRecord(bpraw + "/" + item);
            prog = "ENC(" + uCipher.Encrypt(prog) + ")";
            bWriter = uCommons.GetOSFileHandle(bpenc + "/" + item);
            try {
                bWriter.write(prog);
                bWriter.flush();
                bWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
        System.out.println(eop + " items encrypted");
    }

    private static ArrayList<String> debugReader(String type, String infile) {
        String answer = "";
        ArrayList<String> arr = new ArrayList<>();
        BufferedReader BRin = null;
        FileReader fr = null;
        try {
            fr = new FileReader(infile);
            BRin = new BufferedReader(fr);
            String line = null;
            try {
                line = BRin.readLine();
            } catch (IOException e) {
                uCommons.uSendMessage("read FAIL on " + infile);
                uCommons.uSendMessage(e.getMessage());
            }
            while ((line) != null) {
                if (type.equals("A")) arr.add(line);
                if (type.equals("S")) answer += line + "[[fm]]";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    uCommons.uSendMessage("read FAIL on " + infile);
                    uCommons.uSendMessage(e.getMessage());
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
            } catch (IOException e) {
                uCommons.uSendMessage("File Close FAIL on " + infile);
                uCommons.uSendMessage(e.getMessage());
            }
            fr = null;
        } catch (IOException e) {
            if (!NamedCommon.isNRT) {
                uCommons.uSendMessage("File Access FAIL :: " + infile);
                uCommons.uSendMessage(e.getMessage());
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = e.getMessage();
            }
        }
        if (type.equals("S")) arr.add(answer);
        return arr;
    }

}
