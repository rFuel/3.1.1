package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniSelectList;
import asjava.uniobjects.UniSession;
import com.northgateis.reality.rsc.RSCConnection;

import javax.jms.Session;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;

public class NamedCommon {

    public static Runtime MQgarbo = Runtime.getRuntime();
    public static int ackMode;
    public static String messageBrokerUrl;
    public static String inputQueue;

    static {
        messageBrokerUrl = "tcp://localhost:61616";
        inputQueue = "unknown_queue";
        ackMode = Session.AUTO_ACKNOWLEDGE;
    }

    /* --------------- rFuel Commons --------------- */
    public static boolean noLock=false;
    public static boolean Scrub_uID= false;
    public static boolean AES = false;
    public static boolean VPN = false;
    public static boolean ADMP= false;
    public static boolean datOnly = false;
    public static boolean mapLooping = false;
    public static boolean useProxy=false;
    public static boolean jdbcLoader = false;
    public static boolean CPL = false;
    public static boolean uSecure = false;
    public static boolean debugging = false;
    public static boolean artemis = true;
    public static boolean sendACK = false;
    public static boolean IsAvailableU2 = false;
    public static boolean BulkLoad = false;
    public static boolean isValid = false;
    public static boolean krbDefault = false;
    public static boolean SqlUseStmt = false;
    public static boolean replyReqd = false;
    public static boolean licBulk = false;
    public static boolean licRest = false;
    public static boolean licNRT = false;
    public static boolean isWhse = false;
    public static boolean isRest = false;
    public static boolean isKafka= false;
    public static boolean isNRT  = false;
    public static boolean isPrt = false;
    public static boolean isGUI = false;
    public static boolean isRDS = false;
    public static boolean DropIt = true;
    public static boolean KeepData = true;
    public static boolean EncBaseSql = false;
    public static boolean TruncIt = false;
    public static boolean catchErrs = false;
    public static boolean uniBase = false;
    public static boolean allowDups = true;
    public static boolean Proceed = false;
    public static boolean fmvArrayIsSet = false;
    public static boolean tConnected = false;
    public static boolean sConnected = false;
    public static boolean showNulls = true;
    public static boolean threader = false;
    public static boolean ZERROR = false;
    public static boolean cERROR = false;
    public static boolean Sparse = false;
    public static boolean isAssoc = false;
    public static boolean uGetMaster = false;
    public static boolean showLineage = false;
    public static boolean isWebs = false;
    public static boolean AutoTests = false;
    public static boolean showDNSresoltion = false;
    public static boolean masterStop = false;
    public static boolean showPID = false;
    public static boolean PoolDebug = false;
    public static boolean StopOnSlowDB = false;
    public static boolean ShowDateTime = true;
    public static boolean AutoVault = false;
    public static boolean encRaw=false;
    public static boolean mkEnc=false;
    public static boolean jLogs = false;
    public static boolean escXML= false;
    public static boolean rFuelLogs = true;     // future use: if false, only log ZERRORs
    public static boolean isDocker = false;
    public static boolean sentU2 = false;
    public static boolean uvReset = false;
    public static boolean ConnectionError = false;
    public static boolean ErrorStop = false;
    public static boolean KeepSecrets = false;
    public static boolean CleanseData = false;
    public static boolean multihost = false;
    public static boolean emptyrows = false;
    public static boolean H2Server = false;
    public static boolean runSilent= false;
    public static boolean preLoadAES = false;
    public static boolean MultiMovers= false;
    public static boolean TranIsolation = false;
    public static boolean sqlLite = false;
    public static boolean SmartMovers = false;
    public static boolean BurstRestarted = false;
    public static boolean trace = false;
    public static boolean PartitionTables = false;
    public static String PartScheme="ps_rfuel";
    public static String jolokiaURL="";
    public static String jolokiaORIGIN="";
    public static String queueWATCH="";
    public static String bkrName = "rfuel";
    public static String Digest = "SHA-512";
    public static String esbfmt = "JSON";
    public static String AlertQ = "uplALERTS";
    public static String DevHost = "/home/andy/rfuel14";
    public static String DevCentre=DevHost;
    public static String AMS="";
    public static String encSeed="";
    public static String logLevel = "info";
    public static String hostname = ""; //ManagementFactory.getOperatingSystemMXBean().getName(); //"";
    public static String bkr_url  = "";
    public static String bkr_user = "";
    public static String bkr_pword = "";
    public static String protocol = "";
    public static String mqHost = "";
    public static String InMount = "";
    public static String OutMount = "";
    public static String licChecked = "";
    public static String Zmessage = "";
    public static String message = "";
    public static String vwPrefix = "";
    public static String BaseCamp = System.getProperty("user.dir");
    public static String KeystorePassword = "";
    public static String keystorePath = "conf/UniLibreKeys.p12";
    public static String webservices = "";
    public static String tName = "";
    public static String uplSite = "";
    public static String UniLibre = "UniLibre";
    public static String gmods = "/upl/lib/";
    public static String Broker = "";
    public static String que = "";
    public static String BatchID = "";
    public static String task = "";
    public static String realhost = "";
    public static String realuser = "";
    public static String realdb = "";
    public static String realac = "";
    public static String dbhost = "";
    public static String dbPort = "31438";
    public static String dbpath = "";
    public static String dbuser = "";
    public static String passwd = "";
    public static String noLp1 = "A*N*D";
    public static String xMap = "";
    public static String item = "";
    public static String datAct = "";
    public static String fLocn = "";
    public static String datPath = "";
    public static String databaseType = "UNIVERSE";
    public static String noLp2 = " **A*N*A*K**";
    public static String noLic = (noLp1.trim() + noLp2.trim()).replaceAll("\\*", "");
    public static String RunType = "";
    public static String SqlSchema = "";
    public static String SqlDBJar = "";
    public static String jdbcSep = ";";
    public static String jdbcAuth = "";
    public static String jdbcRealm= "";
    public static String jdbcUser= "";
    public static String SqlDatabase = "";
    public static String maxType = "";
    public static String intType = "";
    public static String keyType = "";
    public static String datType = "";
    public static String smlType = "";
    public static String rawDB = "$$$";
    public static String rawSC = "raw";
    public static String Komma = "\t";
    public static String Quote = "";
    public static String upl = "";
    public static String CorrelationID = "~~~";
    public static String MessageID = "~~~";
    public static String StopNow = "   ";
    public static String sqlTarget = "   ";
    public static String SqlRole = "";
    public static String SqlWarehouse = "";
    public static String SystemSchema = "";
    public static String u2Source = "   ";
    public static String ServiceType = "   ";
    public static String u2FileRef = "   ";
    public static String mapSelect = "";
    public static String mapNselect = "";
    public static String presql = "";
    public static String preuni  = "";
    public static String postuni = "";
    public static String mTrigger = "";
    public static String mTrigQue = "014";
    public static String thisProc = "";
    public static String[] csvList = {};
    public static String[][] escChars = new String[10][2];
    public static String[][] jsonChars = new String[10][2];
    public static String colpfx = "   ";
    public static String Template = "";
    public static String ProcSuccess = "   ";
    public static String SqlReply = "";
    public static String tblCols = "";
    public static String sqlStmt = "   ";
    public static String reply2Q = "";
    public static String slash = "/";
    public static String StructuredResponse = "";
    public static String xmlProlog = "";
    public static String minPoolSize = "1";
    public static String maxPoolSize = "5";
    public static String zMarker = "";
    public static String block = "*************************************************************";
    public static String pid = ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0];
    public static String IAM = pid;
    public static String upAlpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    public static String loAplha = "abcdefghijklmnopqrstuvwxyz";
    public static String dbgPfx = "";
    public static String uStatus = "200";
    public static String kafkaBase= "kafka.properties";
    public static String topicExtn = "";
    public static String kafkaAction="";
    public static String jdbcCon = "";
    public static String jdbcDvr = "";
    public static String jdbcAdd = "";
    public static String pKeys = "   ";
    public static String action = "GET";
    public static String zID = "";
    public static String zFile = "";
    public static String zData = "";
    public static String rawCols = "-MD5,-uID,-LoadDte,.ProcNum,*RawData";
    public static String burstCols = ".Seqn,-uID,-LoadDte,-MD5,.ProcNum,Source,-NestPosition";
    public static String sep = "<tm>";
    public static String IMark = "<im>";
    public static String KMark = "<km>";
    public static String FMark = "<fm>";
    public static String VMark = "<vm>";
    public static String SMark = "<sm>";
    public static String uniqId = "";
    public static String ListID = "uWhse";
    public static String secret = "";
    public static String salt = "";
    public static String serial = "";
    public static String Presel = "";
    public static String SourceDBIP = "";
    public static String cSeed;
    public static String Mandatory = "";
    public static String RDSdir = "";
    public static String AZdir;
    public static String Token = "";
    public static String pxHost = "";
    public static String testQ = "999_Test";
    public static String TimeZone = "";
    public static ArrayList<String> restMaps = new ArrayList<>();
    public static ArrayList<String> restItems = new ArrayList<>();
    public static ArrayList<String> Digests = new ArrayList<>();
    public static ArrayList<String> mapOrder = new ArrayList<>();
    public static ArrayList<String> ThisRunKey = new ArrayList<>();
    public static ArrayList<String> ThisRunVal = new ArrayList<>();
    public static ArrayList<String> PointerFiles = new ArrayList<>();
    public static ArrayList<String> threadslots = new ArrayList<>();
    public static ArrayList<String> tStatus = new ArrayList<>();
    public static ArrayList<String> tHostList = new ArrayList<>();
    public static ArrayList<String> sHostList = new ArrayList<>();
    public static ArrayList<String> DataLineage = new ArrayList<>();
    public static ArrayList<String> DataList = new ArrayList<>();
    public static ArrayList<Boolean> AsocList = new ArrayList<>();
    public static ArrayList<String> SubsList = new ArrayList<>();
    public static ArrayList<String> TmplList = new ArrayList<>();
    public static ArrayList<String> Templates = new ArrayList<>();
    public static ArrayList<String> SelectCmd = new ArrayList<>();
    public static ArrayList<String> SelectList = new ArrayList<>();
    public static ArrayList<String> ReturnCodes = new ArrayList<>();
    public static ArrayList<UniFile> u2Handles = new ArrayList<>();
    public static ArrayList<String>  OpenFiles = new ArrayList<>();
    public static ArrayList<String>  streamedFiles = new ArrayList<>();
    public static UniJava uJava = new UniJava();
    public static UniSession uSession = null;
    public static UniFile U2File = null;
    public static UniFile VOC = null;
    public static UniFile uRequests = null;
    public static UniFile uLoaded = null, uTake = null;
    public static UniString uNewRec = null;
    public static UniString uID = null;
    public static UniDynArray dynRec = null;
    public static UniDynArray fmvArray;
    public static UniSelectList uSelect = null;
    public static Connection uCon = null;
    public static RSCConnection rcon = null;
    public static double ConnectAcceptable = 9.99;
    public static int DatSize = 2500000;
    public static int MQ_AUTO_ACKNOWLEDGE = 1;
    public static int MQ_CLIENT_ACKNOWLEDGE = 2;
    public static int MQackMode = MQ_AUTO_ACKNOWLEDGE;
    public static int heavyUseMax = 5000;
    public static int FetchSize = 1000;
    public static int licWarning = 30;
    public static int dbWait = 120;
    public static int mqWait = 120;
    public static int vtPing = 120;
    public static int mqHeartBeat = 60;
    public static int restartWait = 0;
    public static int MaxProc = 10;
    public static int mqPort = 58555;
    public static int DatRows = 999;
    public static int fDatRows = 999;
    public static int bDatRows = 999;
    public static int MaxDat = 3;
    public static int aStep = 999;
    public static int iStep = 99;       // INCRemental steps
    public static int rStep = 9999;     // REFRESH steps
    public static int showAT = 39999;
    public static int bshowAT = 0;
    public static int RQM = 3000;
    public static int datCt = 0;
    public static int msgCt = 0;
    public static int rowID = 1;
    public static int burstCnt = 0;
    public static int mqCounter=1;
    public static int BurstWait = 99;
    public static int numthreads = 4;
    public static int pxPort = 31448;
    public static int NextFdir = 0;
    public static int maxFdir = 0;
    public static int maxSmartCons = 1;
    public static long Expiry = 0;
    public static long dbActive;
    public static long startM=0;
    public static long lastM=0;
    public static LocalDate Day0  = LocalDate.of(1967, Month.DECEMBER, 31);
    public static ArrayList<UniFile> fHandles = new ArrayList<>();
    public static ArrayList<String> fTagNames = new ArrayList<>();

    public static void Reset() {
        AES = false;
        VPN = false;
        BulkLoad = false;
        sqlLite = false;
        debugging = false;
        masterStop = false;
        sendACK = false;
        showLineage = false;
        ZERROR = false;
        cERROR = false;
        sentU2 = false;
        isRDS = false;
        ConnectionError = false;
        BurstRestarted = false;
        CorrelationID = "~~~";
        datAct = "";
        dynRec = null;
        MessageID = "~~~";
        dbWait = 120;
        mqWait = 120;
        vtPing = 120;
        mqHeartBeat = 60;
        uNewRec = null;
        uID = null;
        reply2Q = "";
        secret = "";
        salt = "";
        serial = "";
        SqlSchema = "";
        SqlDatabase = "";
        Zmessage = "";
        tblCols = "";
        dbPort = "31438";
        RDSdir= "";
        AZdir = "";
        APImap.instantiate();
        ResetEscChars();

        try {
            InetAddress me = InetAddress.getLocalHost();
            String myIP = me.getHostAddress();
            IAM  = myIP;
            me   = null;
            myIP = null;
        } catch (UnknownHostException e) {
            IAM = pid;
        }

    }

    public static void ResetEscChars() {
        for (int clr=0 ; clr < NamedCommon.escChars.length ; clr++) {
            NamedCommon.escChars[clr][0] = "";
            NamedCommon.escChars[clr][1] = "";
            NamedCommon.jsonChars[clr][0] = "";
            NamedCommon.jsonChars[clr][1] = "";
        }

        NamedCommon.jsonChars[0][0] = "\\";
        NamedCommon.jsonChars[0][1] = "\\\\";

        NamedCommon.jsonChars[1][0] = "\"";
        NamedCommon.jsonChars[1][1] = "\\\"";

        NamedCommon.jsonChars[2][0] = "\n";
        NamedCommon.jsonChars[2][1] = "\\n";

        NamedCommon.jsonChars[3][0] = "\t";
        NamedCommon.jsonChars[3][1] = "\\t";

        NamedCommon.jsonChars[4][0] = "\b";
        NamedCommon.jsonChars[4][1] = "\\b";

        NamedCommon.jsonChars[5][0] = "\f";
        NamedCommon.jsonChars[5][1] = "\\f";

        NamedCommon.jsonChars[6][0] = "\r";
        NamedCommon.jsonChars[6][1] = "\\r";

        NamedCommon.escChars[0][0] = "&";
        NamedCommon.escChars[0][1] = "&amp";

        NamedCommon.escChars[1][0] = "\"";
        NamedCommon.escChars[1][1] = "&quot";

        NamedCommon.escChars[2][0] = "'";
        NamedCommon.escChars[2][1] = "&apos";

        NamedCommon.escChars[3][0] = ">";
        NamedCommon.escChars[3][1] = "&gt";

        NamedCommon.escChars[4][0] = "<";
        NamedCommon.escChars[4][1] = "&lt";

        NamedCommon.escChars[5][0] = "~";
        NamedCommon.escChars[5][1] = "comma";

        NamedCommon.escChars[6][0] = "!";
        NamedCommon.escChars[6][1] = "exclaimed";

        NamedCommon.escChars[7][0] = "@";
        NamedCommon.escChars[7][1] = "at";

        NamedCommon.escChars[8][0] = "\\";
        NamedCommon.escChars[8][1] = "slash";

        NamedCommon.escChars[9][0] = "^";
        NamedCommon.escChars[9][1] = "caret";

        NamedCommon.Digests.clear();
        NamedCommon.Digests.add("SHA3-512");
        NamedCommon.Digests.add("SHA3-384");
        NamedCommon.Digests.add("SHA3-256");
        NamedCommon.Digests.add("SHA3-224");
        NamedCommon.Digests.add("SHA-512/256");
        NamedCommon.Digests.add("SHA-512/224");
        NamedCommon.Digests.add("SHA-512");
        NamedCommon.Digests.add("SHA-384");
        NamedCommon.Digests.add("SHA-256");
        NamedCommon.Digests.add("SHA-224");
        NamedCommon.Digests.add("SHA");
        NamedCommon.Digests.add("MD5");
        NamedCommon.Digests.add("MD2");
    }

}
