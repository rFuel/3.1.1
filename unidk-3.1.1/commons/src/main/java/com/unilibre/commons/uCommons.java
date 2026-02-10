package com.unilibre.commons;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import com.unilibre.cipher.AESEncryption;
import com.unilibre.cipher.uCipher;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.jms.*;
import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class uCommons {

    private static final String newLine = System.getProperty("line.separator");
    public static int eStatus = 0;
    public static String eMessage;
    public static String HOME = NamedCommon.BaseCamp;
    public static String uMQDir = "/data";
    public static String insDir = "/ins";
    public static String outDir = "/out";
    public static String rstDir = "/rest";
    public static String extn = ".dat";
    public static String rstn = ".rst";
    public static String theFile = "";
    public static Properties props = new Properties();
    public static boolean isDel = false;
    public static boolean showErrors = true;
    private static long MAGIC = 86400000L;
    private static Runtime rt = Runtime.getRuntime();
    private static String oCmd;

    /* ************************************************************************************************ */

    public static boolean OkayToProcess(String task) {
        boolean okay = true;
        if (task.startsWith("9")) return okay;
        if (!NamedCommon.isWhse) return okay;
        String[] mustHave = NamedCommon.Mandatory.split(",");
        if (mustHave[0].equals("")) mustHave[0] = "TASK";
        int eom = mustHave.length;
        for (int m=0; m<eom; m++) {
            if (APIGetter(mustHave[m]).equals("")) {
                okay = false;
                uCommons.uSendMessage("No value for " + mustHave[m] + " in the message, cannot process !");
            }
        }
        return  okay;
    }

    public static String GenerateString(int targetStringLength) {
        //  Generate a random Alphanumeric string, 10 chars long
        int lowerLimit   = 48;     // numeral '0'
        int upperLimit  = 122;      // letter 'z'
        Random random = new Random();
        String generatedString = random.ints(lowerLimit, upperLimit + 1)
                .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        return generatedString;
    }

    public static String GetToday() {
//        LocalDate dNow = LocalDate.now();
        LocalDate dNow;
        if (NamedCommon.TimeZone != null && !NamedCommon.TimeZone.isEmpty()) {
            dNow = ZonedDateTime.now(ZoneId.of(NamedCommon.TimeZone)).toLocalDate();
        } else {
            dNow = ZonedDateTime.now(ZoneId.systemDefault()).toLocalDate();
        }
        String sNow = dNow.toString();
        int today = iconvD(sNow, "yyyy-MM-dd");
        return String.valueOf(today);
    }

    public static String GetLocaltimeStamp() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone(NamedCommon.TimeZone));
        return sdf.format(new Date());
    }

    public static String MakeBatchID() {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of(NamedCommon.TimeZone));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        String base = now.format(formatter);
        String micros = String.format("%06d", now.getNano() / 1000); // microseconds
        return base + micros;
    }

    public static String GetTime() {
        ZoneId zoneId = null;
        if (NamedCommon.TimeZone != null && !NamedCommon.TimeZone.isEmpty()) {
            zoneId = ZoneId.of(NamedCommon.TimeZone);
        } else {
            zoneId = ZoneId.of("UTF");
        }
        ZonedDateTime nowZoned = ZonedDateTime.now(zoneId);
        Instant midnight = nowZoned.toLocalDate().atStartOfDay(nowZoned.getZone()).toInstant();
        Duration duration = Duration.between(midnight, Instant.now());
        long seconds = duration.getSeconds();
        return String.valueOf(seconds);
    }

    public static void uSendMessage(String inMsg) {
        if (!NamedCommon.rFuelLogs && !NamedCommon.ZERROR) return;
        // find out who called uSendMessage
        String caller="";
        if (NamedCommon.jLogs) caller = NamedCommon.IAM;
        //
        String chk, mTime, mDate, MSec;
        chk = inMsg.trim();
        if (chk.length() > 0) {
            mTime = "";
            mDate = "";
            MSec = "";
            if (NamedCommon.ShowDateTime) {
                String rightNow = GetLocaltimeStamp();
                mTime = FieldOf(rightNow, " ", 2);
                mTime = FieldOf(mTime, "\\.", 1);
                MSec = "." + FieldOf(rightNow, "\\.", 2);
                if (NamedCommon.ZERROR || NamedCommon.cERROR) {
                    mDate = "**** ERROR";
                } else {
                    if (!NamedCommon.zMarker.equals("")) {
                        mDate = RightHash(("**********"+NamedCommon.zMarker), 10);
                    } else {
                        mDate = FieldOf(rightNow, " ", 1);
                    }
                }
                if (NamedCommon.showPID) mDate = uCommons.LeftHash(NamedCommon.pid, 10);
            }
            if (NamedCommon.jLogs) {
                String jMsg = "{";
                if (NamedCommon.CorrelationID.equals("~~~")) {
                    jMsg += "\"CorrelationID\": \"rFuel\",";
                } else {
                    jMsg += "\"CorrelationID\": \"" + NamedCommon.CorrelationID + "\",";
                }
                jMsg += "\"DateString\": \"" + mDate + "\",";
                jMsg += "\"TimeString\": \"" + mTime + MSec + "\",";
                jMsg += "\"Caller\": \"" + caller + "\",";
                jMsg += "\"Event\": \"" + inMsg + "\"";
                jMsg += "}";
                System.out.println(jMsg);
                jMsg = "";
            } else {
                System.out.println(mDate + " " + mTime + MSec + " " + inMsg);
            }
            eMessage = "";
            mTime = "";
            mDate = "";
            MSec = "";
            inMsg = "";
        }
    }

    public static void SetMemory(String key, String val) {
        if (!key.equals("")) {
            int fnd = NamedCommon.ThisRunKey.indexOf(key);
            if (fnd < 0) {
                NamedCommon.ThisRunKey.add(key);
                NamedCommon.ThisRunVal.add(val);
            } else {
                NamedCommon.ThisRunVal.set(fnd, val);
            }
        }
    }

    public static String GetMemory(String key) {
        String val = "";
        if (!key.equals("")) {
            int fnd = NamedCommon.ThisRunKey.indexOf(key);
            if (fnd > -1) {
                val = NamedCommon.ThisRunVal.get(fnd);
            }
        }
        return val;
    }

    public static void setvars() {
        HOME = NamedCommon.BaseCamp;
        uMQDir = "/data";
        insDir = "/ins";
        outDir = "/out";
        rstDir = "/rest";
        extn = ".dat";
        rstn = ".rst";
    }

    public static Properties LoadProperties(String fname) {
        NamedCommon.StopNow = "";
        if (!NamedCommon.ZERROR) NamedCommon.Zmessage = "";
        Properties lProps = new Properties();

        if (!fname.contains(NamedCommon.slash)) {
            fname = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash + fname;
        }

        StringBuilder sb = new StringBuilder();
        List<String> lines;
        try {
            lines = Files.readAllLines(Paths.get(fname), StandardCharsets.ISO_8859_1); // safer encoding
            // Handle backslash line continuations manually
            StringBuilder logicalLine = new StringBuilder();
            boolean skipLine=false;
            String trimmed;
            for (String line : lines) {
                skipLine = false;
                if (line.startsWith("*")) skipLine = true;
                if (line.startsWith("#")) skipLine = true;
                trimmed = line.trim();
                if (line.length() == 0) skipLine = true;
                // --------------- This works but needs testing ------------------
//                if (trimmed.isEmpty()) continue;
//                if (skipLine) continue;
                // ---------------------------------------------------------------
                if (skipLine) {
                    if (logicalLine.length() > 0) {
                        sb.append(logicalLine.toString()).append("\n");
                        logicalLine.setLength(0); // reset for next logical line
                    }
                    continue;
                }
                if (trimmed.endsWith("\\")) {
                    logicalLine.append(trimmed, 0, trimmed.length() - 1);
                    continue; // wait for more lines
                } else {
                    logicalLine.append(trimmed);
                    sb.append(logicalLine.toString()).append("\n");
                    logicalLine.setLength(0); // reset for next logical line
                }
            }
            if (logicalLine.toString().length() > 0) {
                sb.append(logicalLine.toString()).append("\n");
                logicalLine.setLength(0); // reset for next logical line
            }
            try (Reader reader = new StringReader(sb.toString())) {
                lProps.load(reader);
            } catch (IOException | IllegalArgumentException e) {
                NamedCommon.StopNow = "<<FAIL>>";
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "Error loading '" + fname + "' – " + e.getMessage();
                uCommons.uSendMessage(NamedCommon.Zmessage);
            }
        } catch (IOException e) {
            NamedCommon.StopNow = "<<FAIL>>";
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Please load '" + fname + "' – " + e.getMessage();
            uCommons.uSendMessage(NamedCommon.Zmessage);
        }

        String value, cleaned;
        for (String key : lProps.stringPropertyNames()) {
            value = lProps.getProperty(key);
            if (value != null) {
                cleaned = value.replaceAll("[\\r\\n]", "");
                lProps.setProperty(key, cleaned);
            }
        }

        return lProps;
    }

    public static void Message2Properties(String arg) {
        // arg must be in rfuel format, not json
        MessageToAPI(arg);
        if (NamedCommon.ZERROR) return;
//        Properties mProps = DecodeMessage(arg);
        Properties mProps = APImsg.MessageAsProperties();

        NamedCommon.task = mProps.getProperty("TASK", NamedCommon.task);
        NamedCommon.xMap = mProps.getProperty("MAP", "NOT-PROVIDED");
        NamedCommon.item = mProps.getProperty("ITEM", "");
        if (NamedCommon.item.equals("*")) NamedCommon.item = "";
        NamedCommon.RunType = mProps.getProperty("RUNTYPE", "NOT-PROVIDED").toUpperCase();
        NamedCommon.Proceed = (mProps.getProperty("PROCEED", String.valueOf(NamedCommon.Proceed)).equals("true"));
        NamedCommon.SqlSchema = mProps.getProperty("SCHEMA", "");
        NamedCommon.SqlDatabase = mProps.getProperty("SQLDB", "SQLDB-MSG-ERROR"); // NamedCommon.SqlDatabase);
        NamedCommon.Presel = mProps.getProperty("PRESEL", "Unknown");
        NamedCommon.action = mProps.getProperty("ACTION", "Unknown");
        NamedCommon.zID = mProps.getProperty("ZID", "Unknown");
        NamedCommon.zFile = mProps.getProperty("ZFILE", "Unknown");
        NamedCommon.zData = mProps.getProperty("ZDATA", "Unknown");
        NamedCommon.StopNow = mProps.getProperty("STOP", "Unknown");
        NamedCommon.uniqId = mProps.getProperty("UUID", "Unknown");
        NamedCommon.datAct = mProps.getProperty("dacct", NamedCommon.datAct);
        String dbgFlag = mProps.getProperty("DEBUG", "Unknown");
        if (!NamedCommon.debugging) NamedCommon.debugging = dbgFlag.toLowerCase().equals("true");
        if (!APImsg.APIget("TTL").equals("")) {
            try {
                NamedCommon.Expiry = Integer.valueOf(APImsg.APIget("TTL"));
            } catch (NumberFormatException nfe) {
                uCommons.uSendMessage("   .) Message contained in an invalid Expiry setting [" + APImsg.APIget("TTL")+"]");
            }
        }
    }

    public static void MessageToAPI(String message) {
        // ----------------------------------------------------------------
        // ORDER: of priority
        //      rFuel.properties        (SetCommons - the defaults)
        //      then: map / grp / gog
        //      then: message           (highest priority)
        //
        // 1. Break message into key - value pairs and store in APImsg
        // 2. If the message has sHost and / or tHost properties;
        //    a. Get details from properties file(s)
        //    b. break into key-value pairs and store in APImsg
        // 3. Revert back to properties held in message
        //    --> this give over-ride priority to the message
        // ----------------------------------------------------------------
        if (APImsg.GetMsgSize() == 0) APImsg.instantiate();
        int nbrValues;
        String zkey, zval;
        NamedCommon.restMaps.clear();
        NamedCommon.restItems.clear();

        if (message.startsWith("{")) {
            //
            // MessageProtocol executes stringifyMessage() so this code is likely never going to get executed.
            //
            ArrayList<String> reqKeys = new ArrayList<>();
            ArrayList<String> reqVals = new ArrayList<>();
            String jHeader = "request";
            Iterator<String> jKeys;
            try {
                JSONObject tObj = new JSONObject(message);
                jKeys = tObj.getJSONObject(jHeader).keys();
                while (jKeys.hasNext()) {
                    zkey = jKeys.next();
                    zval = tObj.getJSONObject(jHeader).get(zkey).toString();
                    reqKeys.add(zkey.toUpperCase());
                    reqVals.add(zval);
                    APImsg.APIset(zkey, zval);
                }
            } catch (JSONException e) {
                NamedCommon.Zmessage = "MessageToAPI(): " + e.getMessage();
                NamedCommon.ZERROR = true;
                uCommons.uSendMessage(NamedCommon.Zmessage);
                return;
            }
            nbrValues = reqKeys.size();
            if (reqKeys.indexOf("GRP") >= 0) {
                String grp = reqVals.get(reqKeys.indexOf("GRP"));
                JSONArray jMsgArr = new JSONArray(grp);
                int arrLen = jMsgArr.length();
                JSONObject element;
                for (int j=0 ; j < arrLen ; j++) {
                    element = jMsgArr.getJSONObject(j);
                    zkey = element.getString("map").toString();
                    zval = element.getString("item").toString();
                    NamedCommon.restMaps.add(zkey);
                    NamedCommon.restItems.add(zval);
                    element = null;
                }
            }
            zkey = "map";
            zval = "";
            if (reqKeys.indexOf("ACTION") >= 0) {
                zval = reqVals.get(reqKeys.indexOf("ACTION"));
                APImsg.APIset(zkey, zval);
            }
            reqKeys.clear();
            reqVals.clear();
        } else {
            message = message.replaceAll("\\r?\\n", " ");

            while (message.toLowerCase().contains("<is>")) {
                message = message.replace("<is>", "=");
                message = message.replace("<IS>", "=");
                message = message.replace("<Is>", "=");
                message = message.replace("<iS>", "=");
            }
            message = message.replace("<TM>", NamedCommon.sep);
            message = message.replace("<Tm>", NamedCommon.sep);
            message = message.replace("<tM>", NamedCommon.sep);

            String[] values = message.split(NamedCommon.sep);
            nbrValues = values.length;
            int posx;
            String[] tmp = new String[2];
            for (int i = 0; i < nbrValues; i += 1) {
                posx = values[i].indexOf("=");
                if (posx > 0) {
                    tmp[0] = values[i].substring(0, posx).replaceAll("\\ ", "");
                    if (posx == values[i].length()) {
                        APImsg.APIset(tmp[0].toUpperCase(), "");
                    } else {
                        tmp[1] = values[i].substring(posx + 1, values[i].length());
                        if (!tmp[0].toUpperCase().startsWith("GRP")) {
                            APImsg.APIset(tmp[0].toUpperCase(), tmp[1]);
                        } else {
                            if (tmp[0].toUpperCase().equals("GRPS")) {
                                if (tmp.length == 2) {
                                    APImsg.APIset(tmp[0].toUpperCase(), tmp[1]);
                                } else {
                                    APImsg.APIset(tmp[0].toUpperCase(), "");
                                }
                            } else {
                                String grpKeys = tmp[0].substring(3, tmp[0].length());     // "map" or "item"
                                String grpVals = tmp[1];                                   // "123,456"
                                String[] grpArr = grpVals.split(",");
                                int eoa = grpArr.length;
                                for (int j = 0; j < eoa; j++) {
                                    if (grpKeys.toUpperCase().equals("MAP")) {
                                        NamedCommon.restMaps.add(grpArr[j]);
                                    } else {
                                        NamedCommon.restItems.add(grpArr[j]);
                                    }
                                }
                                APImsg.APIset("GRP", "true");
                            }
                        }
                    }
                }
            }
        }

        if (!NamedCommon.AutoTests) {
            if (!APImsg.APIget("SHOST").equals("")) {
                String fqn = NamedCommon.BaseCamp + "/conf/" + APImsg.APIget("SHOST");
                uCommons.uSendMessage(">>> Using SourceHost: " + APImsg.APIget("SHOST"));
                uCommons.showErrors = false;
                String hostDets = ReadDiskRecord(fqn);
                if (NamedCommon.ZERROR) {
                    NamedCommon.ZERROR=false;
                    NamedCommon.Zmessage="";
                    fqn = NamedCommon.BaseCamp + "/conf/hosts/" + APImsg.APIget("SHOST");
                    hostDets = ReadDiskRecord(fqn);
                }
                if (NamedCommon.ZERROR) {
                    NamedCommon.ZERROR=false;
                    NamedCommon.Zmessage="";
                    fqn = NamedCommon.BaseCamp + "/conf/src/" + APImsg.APIget("SHOST");
                    hostDets = ReadDiskRecord(fqn);
                }
                uCommons.showErrors = true;
                if (hostDets.startsWith("ENC(")) {
                    hostDets = hostDets.replaceAll("\\r?\\n", "");
                    hostDets = hostDets.substring(4);
                    if (hostDets.endsWith(")")) hostDets = hostDets.substring(0, (hostDets.length()-1));
                    String[] inLines = hostDets.split("<nl>");
                    hostDets = "";
                    for (int l=0 ; l < inLines.length ; l++) { hostDets += uCipher.Decrypt(inLines[l]) + "\n"; }
                    inLines = null;
//                } else {
//                    NamedCommon.ZERROR=true;
//                    NamedCommon.Zmessage = "Licence violation at Source host for "+APImsg.APIget("SHOST") + ". File must be ENCrypted in conf/ (hosts or src).";
                }
                if (!NamedCommon.ZERROR) {
                    String[] lines = hostDets.split("\\r?\\n");
                    String line,stmp;
                    int nbrLines = lines.length, lx;
                    for (int ll = 0; ll < nbrLines; ll++) {
                        line = lines[ll].replaceAll("\\ +", " ");
                        if (!line.equals("")) {
                            if (line.contains("=ENC(")) {
                                lx = line.indexOf("=ENC(");
                                stmp = line.substring(lx + 5, (line.length() - 1));
                                line = line.substring(0, lx + 1) + uCipher.Decrypt(stmp);
                            }
                            String[] tmparr = line.split("=");
                            if (tmparr.length > 1) {
                                APImsg.APIset(tmparr[0].toUpperCase(), tmparr[1]);
                            } else {
                                APImsg.APIset(tmparr[0].toUpperCase(), "");
                            }
                            tmparr = null;
                        }
                    }
                    lines = null;
                    // update the NC variables here
                    boolean rfFormat = (!APImsg.APIget("u2host").equals(""));
                    if (!rfFormat) {
                        NamedCommon.dbhost = APImsg.APIget("HOST");
                        if (!APImsg.APIget("PORT").equals("")) NamedCommon.dbPort = APImsg.APIget("PORT");
                        NamedCommon.dbpath = APImsg.APIget("PATH");
                        NamedCommon.dbuser = APImsg.APIget("USER");
                        NamedCommon.passwd = APImsg.APIget("PWORD");
                        NamedCommon.datAct = APImsg.APIget("DACCT");
                    } else {
                        NamedCommon.dbhost = APImsg.APIget("u2host");
                        if (!APImsg.APIget("PORT").equals("")) NamedCommon.dbPort = APImsg.APIget("u2port");
                        NamedCommon.dbpath = APImsg.APIget("u2path");
                        NamedCommon.dbuser = APImsg.APIget("u2user");
                        NamedCommon.passwd = APImsg.APIget("u2pass");
                        NamedCommon.datAct = APImsg.APIget("DACCT");
                        if (NamedCommon.datAct.equals("")) NamedCommon.datAct = APImsg.APIget("u2acct");
                        NamedCommon.databaseType = APImsg.APIget("dbtype");
                    }
                }
            } else {
                if (NamedCommon.debugging) uCommons.uSendMessage(">>> Using SourceHost: rFuel.properties");
            }

            if (NamedCommon.isWhse && !NamedCommon.ZERROR) {
                // gitlab # 19
                // ## Gitlab 921
                if (!APImsg.APIget("THOST").equals("") && !NamedCommon.datOnly) {
                    ArrayList<String> hostlist = new ArrayList<>(Arrays.asList(APImsg.APIget("THOST").split("\\,")));
                    String thisHost;
                    for (int h = 0; h < hostlist.size(); h++) {
                        if (NamedCommon.ZERROR) return;
                        thisHost = hostlist.get(h);
                        if (!NamedCommon.tConnected) {
                            if (!NamedCommon.isNRT) uCommons.uSendMessage((h + 1) + " > Using TargetHost: " + thisHost);
                            SqlCommands.ConnectSQL();
                        }
                    }
                } else {
                    SetBaseSql();
                }
                // -----------
                if (NamedCommon.ZERROR) return;
            }
        }

        if (nbrValues < 1) {
            uCommons.eMessage = "MessageToAPI Decoding Error: [" + message + "] no values found";
            uCommons.uSendMessage(uCommons.eMessage);
        }

        if (!APImsg.APIget("rfTag").equals("")) return;

        if (NamedCommon.ZERROR) return;

        // Revert back to Message properties
        NamedCommon.datAct = MessageChecker(NamedCommon.datAct, "dacct");

        String chkProt = MessageChecker(NamedCommon.protocol, "protocol");

        if (chkProt.toUpperCase().startsWith("R")) {
            NamedCommon.realhost = MessageChecker(NamedCommon.realhost, "realhost");
            NamedCommon.realuser = MessageChecker(NamedCommon.realuser, "realuser");
            NamedCommon.realdb = MessageChecker(NamedCommon.realdb, "realdb");
            NamedCommon.realac = MessageChecker(NamedCommon.realac, "realac");
        }
        NamedCommon.xMap = APImsg.APIget("MAP");
        NamedCommon.item = APImsg.APIget("ITEM");
        if (NamedCommon.item.equals("*")) NamedCommon.item = "";
        NamedCommon.RunType = APImsg.APIget("RUNTYPE").toUpperCase();
        String proceed = APImsg.APIget("PROCEED").toLowerCase();
        if (proceed.equals("")) proceed = "true";
        NamedCommon.Proceed = proceed.equals("true");
        NamedCommon.SqlSchema = MessageChecker(NamedCommon.SqlSchema, "SCHEMA");
        NamedCommon.SqlDatabase = MessageChecker(NamedCommon.SqlDatabase, "SQLDB");
        NamedCommon.CorrelationID = CheckField(APImsg.APIget("CORRELATIONID"));
        NamedCommon.reply2Q = CheckField(APImsg.APIget("REPLYTO"));
        NamedCommon.Presel = APImsg.APIget("PRESEL");
        String bwait = APImsg.APIget("BURSTWAIT");
        if (!bwait.equals("")) {
            try {
                NamedCommon.BurstWait = Integer.valueOf(bwait);
            } catch (NumberFormatException nfe) {
                uCommons.uSendMessage("Message contains and inon-integer burstwait period ! ***********");
                // leave it as it is in rFuel.properties
            }
        }
        NamedCommon.action = APImsg.APIget("ACTION");
        if (!NamedCommon.debugging) NamedCommon.debugging = APImsg.APIget("DEBUG").toLowerCase().equals("true");
        APImap.APIset("MESSAGE", NamedCommon.MessageID);
    }

    private static void SetBaseSql() {
        if (NamedCommon.datOnly) {
            uCommons.uSendMessage("*********************************************************************");
            uCommons.uSendMessage(">>> Using TargetHost: rFuel.properties                               ");
            uCommons.uSendMessage(">>> Will produce data files ONLY - no Target DB actions.             ");
            uCommons.uSendMessage("*********************************************************************");
            return;
        } else {
            if (NamedCommon.debugging) uCommons.uSendMessage(">>> Using TargetHost: rFuel.properties");
        }
        //
        // this section stopped the ability to add extra partameters for integratedSecurity (Kiwibank).
        // why this block was needed - some clients were putting everything in the jdbcCon property.
        // this has been deprecated by using jdbcUsr and jdbcPwd
        //
        String[] jdbcParts = NamedCommon.jdbcCon.split(NamedCommon.jdbcSep);
        // PATCH ---------------------------------------------------
        SqlCommands.SetJdbcParts(jdbcParts, NamedCommon.EncBaseSql);
        // ---------------------------------------------------------

        String url, usr, pwd, dbi;
        String dbname = APImsg.APIget("sqldb");
        if (dbname.equals("")) dbname = NamedCommon.SqlDatabase;
        String scname = APImsg.APIget("schema");
        if (scname.equals("")) scname = NamedCommon.SqlSchema;
        APImsg.APIset("sqldb:base-sql", dbname);
        APImsg.APIset("schema:base-sql", scname);
        url = APImsg.APIget("jdbccon:base-sql");
        usr = APImsg.APIget("jdbcUsr:base-sql");
        pwd = APImsg.APIget("jdbcPwd:base-sql");
        dbi = APImsg.APIget("jdbcDbi:base-sql");
        String jdbcDBI = APImsg.APIget("sqldb:base-sql");
        String jdbcSCH = APImsg.APIget("schema:base-sql");

        if (!jdbcDBI.equals("")) {
            if (!url.endsWith(";")) url += ";";
            url += "databaseName=" + jdbcDBI;
            if (!url.endsWith(";")) url += ";";     // in case Dbi is just a name - common.
        }

        if (!NamedCommon.jdbcAdd.equals("")) {
            if (!url.endsWith(";")) url += ";";
            url += NamedCommon.jdbcAdd;
            if (!url.endsWith(";")) url += ";";     // just in case
        }

        if (NamedCommon.tHostList.indexOf("base-sql") < 0) {
            try {
                ConnectionPool.AddToPool(url, usr, pwd);
                if (!NamedCommon.ZERROR) {
                    if (NamedCommon.tHostList.indexOf("base-sql") < 0) NamedCommon.tHostList.add("base-sql");
                    String chk = url + "+" + jdbcSCH;
                    if (ConnectionPool.objPool.indexOf(chk) < 0) {
                        ConnectionPool.objPool.add(chk);
                    }
                } else {
                    return;
                }
            } catch (SQLException e) {
                uCommons.uSendMessage(e.getMessage());
            }
        }

        String fDir = NamedCommon.BaseCamp + "/conf/";
        // BS:  base sql
//        uCipher.SetAES(false, "", "");
        String bsCheck = ReadDiskRecord(fDir + "base-sql", true);
        String inpLine = "jdbcCon=" + url +
                "\njdbcDbi="+dbi +
                "\njdbcUsr=" + usr +
                "\njdbcPwd=ENC(" + uCipher.Encrypt(pwd) + ")" +
                "\nsqldb=" + APImsg.APIget("sqldb:base-sql") +
                "\nschema=" + APImsg.APIget("schema:base-sql");
        boolean bsChanged = false;
        uCommons.uSendMessage(">>> Checking base-sql for changes");
        //
        // sometimes the encryption string has chars which cause problems with split functions.
        // At some point, it will be safer / better to load them into a Properties obj and
        // trip through each, rather than picking them out of strings !!
        //
        String[] oldBS = bsCheck.split("\\n");
        String[] newBS = (inpLine+"\n").split("\\n");
        String[] tmpBS = null;
        String oldV="", newV="", tmp;
        for (int l=0 ; l < newBS.length ; l++) {
            if (oldBS[l].contains("ENC(")) {
                tmp = oldBS[l];
                tmp = tmp.substring(tmp.indexOf("=")+1, tmp.length());
                oldBS[l] = oldBS[l].substring(0, oldBS[l].indexOf("=")+1) + uCipher.Decrypt(tmp);
            }
            if (newBS[l].contains("ENC(")) {
                tmp = newBS[l];
                tmp = tmp.substring(tmp.indexOf("="), tmp.length());
                newBS[l] = newBS[l].substring(0, newBS[l].indexOf("=")+1) + uCipher.Decrypt(tmp);
            }
            oldV += oldBS[l];
            newV += newBS[l];
            if (!oldV.equals(newV)) break;
        }
        if(!oldV.equals(newV)) bsChanged = true;
        if (bsChanged) {
            uCommons.uSendMessage("Updating: " + fDir + "base-sql");
            BufferedWriter bWriter = GetOSFileHandle(fDir + "base-sql");
            NamedCommon.ZERROR = false;
            NamedCommon.Zmessage="";
            try {
                bWriter.write(inpLine);
                bWriter.newLine();
                bWriter.flush();
                bWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
//        uCommons.uSendMessage("Done.");
        uCommons.uSendMessage(">>> Done.");
        NamedCommon.ZERROR = false;
        NamedCommon.Zmessage = "";
        APImsg.APIset("THOST", "base-sql");
    }

    private static String MessageChecker(String inVal, String mKey) {
        String ans = inVal;
        String chk = APImsg.APIget(mKey);
        if (!chk.equals("")) ans = chk;
        return ans;
    }

    public static String CheckField(String property) {
        if (property.contains("~")) {
            property = property.replaceAll("\\~", " ");
        }
        return property;
    }

    public static void Execute(String cmd) {
        Runtime rt = Runtime.getRuntime();
        try {
            System.out.println("command [" + cmd + "]");
            Process p = rt.exec(cmd);

            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(p.getErrorStream()));

            boolean first = true;
            String s;
            while ((s = stdInput.readLine()) != null) {
                if (first) {
                    System.out.println("Output from the command:");
                    first = false;
                }
                System.out.println("  > " + s);
            }

            first = true;
            while ((s = stdError.readLine()) != null) {
                if (first) {
                    System.out.println("Errors from the command:");
                    first = false;
                }
                System.out.println("  > " + s);
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
            System.out.println("exec() failed for :: " + cmd);
        }
    }

    public static String GetMD5(String inStr) {
        MessageDigest m;
        // Get the hashing algorythm from rFuel.properties -> NamedCommon.Digest
        String digest = "";
//        digest = "MD5";
//        digest = "SHA3-512";
//        digest = "SHA-384";
//        digest = "SHA";
//        digest = "SHA3-384";
//        digest = "SHA-224";
//        digest = "SHA-512/256";
//        digest = "SHA-256";
//        digest = "MD2";
//        digest = "SHA-512/224";
//        digest = "SHA3-256";
//        digest = "SHA-512";
        digest = "MD5";
//        digest = "SHA3-224";
        try {
            m = MessageDigest.getInstance(digest);
        } catch (NoSuchAlgorithmException e) {
            m = null;
            uCommons.eMessage = "MD5 Library Error";
            uCommons.eMessage += "\n" + e.getMessage();
            String ZERROR = uCommons.eMessage;
            uCommons.uSendMessage(uCommons.eMessage);
            //coreCommons.ReportRunError(ZERROR);
            NamedCommon.ZERROR = true;
            return "";
        }
        m.update(inStr.getBytes(), 0, inStr.length());
        String ans = new BigInteger(1, m.digest()).toString(16);
        return ans;
    }

    public static String GetHash(String inStr) {
        MessageDigest m;
        // Get the hashing algorythm from rFuel.properties -> NamedCommon.Digest
        try {
            m = MessageDigest.getInstance(NamedCommon.Digest);
        } catch (NoSuchAlgorithmException e) {
            m = null;
            uCommons.eMessage = "Digest library error";
            uCommons.eMessage += "\n" + e.getMessage();
            NamedCommon.ZERROR = true;
            return "";
        }
        m.update(inStr.getBytes(), 0, inStr.length());
        String ans = new BigInteger(1, m.digest()).toString(16);
        return ans;
    }

    public static void GetMap(String map) {
        NamedCommon.mapOrder.clear();
        GetAPImap(map);
        if (!NamedCommon.ZERROR) {
            NamedCommon.sqlStmt = NamedCommon.sqlStmt + "INSERT INTO " + NamedCommon.sqlTarget + "(";
        }
    }

    public static void SetAPIMap(String map) {
        String maplines = ReadDiskRecord(map);
        if (maplines.indexOf("\\") > 0) {
            StringBuilder msb = new StringBuilder();
            AtomicReference<String> line = new AtomicReference<>("");
            Properties mProps = uCommons.LoadProperties(map);
            mProps.forEach((key, value) -> {
                msb.append(key + "=" + value + "\n");
            });
            maplines = msb.toString();
        }
        if (maplines.contains("ENC(")) {
            System.out.println(" ");
            System.out.println(" decrypting map [1]");
            System.out.println(" ");
            maplines = uCipher.Decrypt(maplines);
        } else {
            if (NamedCommon.KeepSecrets) {
                WriteDiskRecord(map + "_text", maplines);
                uCipher.isLic = false;
                String encMap = uCipher.Encrypt(maplines);
                WriteDiskRecord(map, "ENC(" + encMap + ")");
                encMap = "";
                uCipher.isLic = true;
            }
        }

        APImap.instantiate();
        if (maplines.startsWith("ENC(")) {
            System.out.println(" ");
            System.out.println(" decrypting map [2]");
            System.out.println(" ");
            maplines = uCipher.Decrypt(maplines);
        }
        maplines = maplines.trim().replaceAll("\\ +", " ");
        String[] values = maplines.split("\\r?\\n");
        int nbrValues = values.length, px;
        if (!maplines.isEmpty()) {
            String key, val, line, chr;
            for (int i = 0; i < nbrValues; i += 1) {
                values[i] = values[i].replace(" = ", "=");
                line = values[i];
                if (line.isEmpty()) continue;
                chr = line.substring(0, 1);
                if ("*#;".contains(chr)) continue;
                if (line.indexOf("=") > 0) {
                    px = line.indexOf("=");
                    if (px > -1) {
                        key = line.substring(0, px);
                        if (key.toLowerCase().equals("u2file")) NamedCommon.mapOrder.add("PICK");
                        if (key.toLowerCase().equals("jdbccon")) NamedCommon.mapOrder.add("SQL");
                        val = line.substring(px + 1, line.length());
                        if (!val.isEmpty()) {
                            if (val.startsWith("ENC(")) val = uCipher.Decrypt(val);
                            APImap.APIset(key.toUpperCase(), val);
                        } else {
                            APImap.APIset(key.toUpperCase(), "");
                        }
                    }
                }
            }
        } else {
            uCommons.eMessage = "Map Decoding Error: [" + map + "] no details found";
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = uCommons.eMessage;
            uCommons.uSendMessage(uCommons.eMessage);
            return;
        }
    }

    public static void GetAPImap(String map) {
        SetAPIMap(map);

        if (NamedCommon.ZERROR) { uCommons.uSendMessage(uCommons.eMessage); return; }
        if (NamedCommon.mapOrder.indexOf("SQL") >= 0) CheckJDBCcmd();
        if (NamedCommon.ZERROR) { uCommons.uSendMessage(uCommons.eMessage); return; }

//        NamedCommon.u2Source = APImap.APIget("U2FILE");
//        NamedCommon.sqlTarget = APImap.APIget("SQLTABLE");
//        NamedCommon.csvList = APImap.APIget("LIST").split(",");
        NamedCommon.u2Source = APIGetter("u2file");
        NamedCommon.sqlTarget =APIGetter("SQLTABLE");
        NamedCommon.csvList = APIGetter("LIST").split(",");
        NamedCommon.colpfx = APIGetter("COLPFX");
        NamedCommon.Template = APIGetter("TEMPLATE");
        NamedCommon.mapSelect = APIGetter("SELECT");
        NamedCommon.mapNselect = APIGetter("NSELECT");
        NamedCommon.presql = APIGetter("PRESQL");
        NamedCommon.preuni = APIGetter("PREUNI");
        NamedCommon.postuni= APIGetter("POSTUNI");
        NamedCommon.thisProc = APIGetter("RAWSEL");
        NamedCommon.mTrigger = APIGetter("TRIGGER");
        NamedCommon.mTrigQue = APIGetter("TRIGQUE");
        NamedCommon.fLocn = APIGetter("FLOCN");

        NamedCommon.u2FileRef = NamedCommon.u2Source.replace(".", "_") + "_" + NamedCommon.datAct;
        if (NamedCommon.mapSelect.equals("")) NamedCommon.mapSelect = "NO";
        if (NamedCommon.Template.equals("")) NamedCommon.Template = NamedCommon.u2Source + ".xml";
        if (NamedCommon.sqlTarget.equals("")) NamedCommon.sqlTarget = NamedCommon.u2Source;
        if (NamedCommon.mTrigQue.equals("")) NamedCommon.mTrigQue = "014";
        if (NamedCommon.colpfx.equals("")) NamedCommon.colpfx = "F";
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\.", "_");
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\,", "_");
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\ ", "_");

        // Proceed is take from the map and over-ride from message
        if (NamedCommon.Proceed) {
            String mapProceed = APImsg.APIget("proceed").toLowerCase();
            if (mapProceed.equals("")) mapProceed = "true";
            if (mapProceed.equals("false")) {
                uCommons.uSendMessage("      -----------------------------------");
                uCommons.uSendMessage("   .) this map turns the proceed flag OFF");
                uCommons.uSendMessage("      -----------------------------------");
                NamedCommon.Proceed = false;
            }
        }

    }

    public static String APIGetter(String key, String dflt) {
        String ans = APIGetter(key);
        if (ans.equals("")) ans = dflt;
        return ans;
    }

    public static String APIGetter(String key) {
        String value = "";
        // message over-rides map definitions !
        value = APImsg.APIget(key);
        if (value.equals("")) value = APImap.APIget(key);
        return value;
    }

    private static void CheckJDBCcmd() {
        String jdbcCmd = uCommons.APIGetter("jdbcCmd");
        if (jdbcCmd.startsWith("ENC")) {
            String tmp = jdbcCmd.substring(4, (jdbcCmd.length() - 1));
            jdbcCmd = uCipher.Decrypt(tmp);
        }
        jdbcCmd = jdbcCmd.toUpperCase();
        boolean hasUpdate = false;
        boolean hasDelete = false;
        if (jdbcCmd.startsWith("UPDATE")) hasUpdate = true;
        if (jdbcCmd.contains(" UPDATE ")) hasUpdate = true;
        if (jdbcCmd.contains(";UPDATE ")) hasUpdate = true;
        if (jdbcCmd.contains("; UPDATE")) hasUpdate = true;

        if (jdbcCmd.startsWith("DELETE")) hasDelete = true;
        if (jdbcCmd.contains(" DELETE ")) hasDelete = true;
        if (jdbcCmd.contains(";DELETE ")) hasDelete = true;
        if (jdbcCmd.contains("; DELETE")) hasDelete = true;

        NamedCommon.Zmessage = "";
        if (hasUpdate) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage += "jdbcCommand has an UPDATE statement - NOT PERMITTED.";
        }
        if (hasDelete) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage += " jdbcCommand has an DELETE statement - NOT PERMITTED.";
        }
    }

    public static ArrayList<String> GetFilesByExt(String indir, String inext) {
        File dir = new File(indir);
        ArrayList<String> fileNames = new ArrayList<>();
        FindFilesByExt(inext, dir, fileNames);
        Collections.sort(fileNames, null);
        return fileNames;
    }

    private static void FindFilesByExt(final String pattern, final File folder, List<String> fileNamesList) {
        String fname;
        for (final File f : folder.listFiles()) {
            if (f.isDirectory()) {
                FindFilesByExt(pattern, f, fileNamesList);
            }
            if (f.isFile()) {
                fname = f.getName().toString();
                if (fname.endsWith(pattern)) { fileNamesList.add(f.getAbsolutePath()); }
            }
        }
    }

    public static String[] GetChangedItems(String indir, String inext, long lastRun) {
        String[] items = ReadDiskFiles(indir, inext);

        List<String> list = new ArrayList<>(Arrays.asList(items));

        int lx = items.length;
        String fPath;
        for (int fl=0 ; fl < lx ; fl++) {
            fPath = indir + "/"+ list.get(fl);
            File chkFile = new File(fPath);
            if (chkFile.lastModified() < lastRun) list.set(fl, "");
        }
        lx = list.size();
        for (int fl=0 ; fl < lx ; fl++) {
            if (list.get(fl).isEmpty()) {
                list.remove(fl);
                lx = list.size();
                fl = -1;
            }
        }
        items = list.toArray(new String[0]);
        return items;
    }

    public static String[] ReadDiskFiles(String indir, String inext) {
        String lookin = indir;
        final String matchStr = inext;
        File dir = new File(lookin);
        List<File> list = new ArrayList<>();
        String error = "";
        boolean proceed = true;
        if (!dir.exists()) {
            error = "<<FAIL>> " + lookin + " is not a directory";
            uCommons.uSendMessage(error);
            proceed = false;
        }
        if (proceed) {
            list = Arrays.asList(dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(matchStr); // or something else
                }
            }));
            Collections.sort(list, Comparator.comparing(File::getName));
        }
        int nbrmatches = list.size();
        String[] matchingFiles = new String[nbrmatches];
        if (nbrmatches > 0) {
            String[] pathdets;
            int nbrDirs;
            String fname, tmp;
            for (int ff = 0; ff < nbrmatches; ff++) {
                tmp = String.valueOf(list.get(ff));
                if (tmp.contains(NamedCommon.slash) && !NamedCommon.slash.equals("\\")) {
                    pathdets = tmp.split(NamedCommon.slash);
                } else {
                    pathdets = tmp.split("\\\\");
                }
                nbrDirs = pathdets.length - 1;
                fname = pathdets[nbrDirs];
                matchingFiles[ff] = fname;
            }
        } else {
            if (!error.equals("")) matchingFiles = new String[]{error};
        }
        return matchingFiles;
    }

    public static boolean WriteArrayToDisk(String file, ArrayList array) {
        StringBuilder data = new StringBuilder();
        String record="";
        int eoi = array.size();
        for (int i=0 ; i<eoi ; i++){  data.append(array.get(i) + newLine);  }
        record = data.toString();
        data = null;
        if (NamedCommon.AES) {
            record = AESEncryption.encrypt(record, NamedCommon.secret, NamedCommon.salt);
        }
        boolean ok = WriteDiskRecord(file, record);
        record = null;
        return ok;
    }

    public static boolean WriteDiskRecord(String file, String data) {
        boolean okay = true;
        BufferedWriter bWriter = GetOSFileHandle(file);
        if (bWriter != null) {
            try {
                bWriter.write(data);
                bWriter.newLine();
                bWriter.flush();
                bWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            bWriter = null;
        } else {
            okay = false;
        }
        return okay;
    }

    public static String ReadDiskRecord(String infile, boolean quiet) {
        if (quiet) showErrors = false;
        String ans = ReadDiskRecord(infile);
        showErrors = true;
        return ans;
    }

    public static String ReadDiskRecord(String infile) {

        String rec = "";
        BufferedReader BRin = null;
        FileReader fr = null;
        try {
            fr = new FileReader(infile);
            BRin = new BufferedReader(fr);
            String line = null;
            try {
                line = BRin.readLine();
            } catch (IOException e) {
                if (showErrors) {
                    uCommons.uSendMessage("read FAIL on " + infile);
                    uCommons.uSendMessage(e.getMessage());
                }
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    if (showErrors) {
                        uCommons.uSendMessage("read FAIL on " + infile);
                        uCommons.uSendMessage(e.getMessage());
                    }
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                if (showErrors) {
                    uCommons.uSendMessage("File Close FAIL on " + infile);
                    uCommons.uSendMessage(e.getMessage());
                }
            }
        } catch (IOException e) {
            if (!NamedCommon.isNRT) {
                if (showErrors) {
                    uCommons.uSendMessage("-------------------------------------------------------------------");
                    uCommons.uSendMessage("File Access FAIL :: " + infile);
                    uCommons.uSendMessage(e.getMessage());
                    uCommons.uSendMessage("-------------------------------------------------------------------");
                }
            }
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = e.getMessage();
        }
        return rec;
    }

    public static String FirstPartOf(String inStr, String tmp) {
        int pos = inStr.indexOf(tmp);
        if (pos > 0) {
            inStr = inStr.substring(0, pos);
        }
        return inStr;
    }

    public static String FieldOf(String str, String findme, int occ) {
        String ans, chkr;
        int iChk;
        String[] tmpStr;
        try {
            chkr = str;
        } catch (NullPointerException npe) {
            uCommons.uSendMessage("ERROR: FieldOf() received a null string value");
            return "";
        }
        try {
            iChk = occ;
        } catch (NullPointerException npe) {
            uCommons.uSendMessage("ERROR: FieldOf() received a null occurance value");
            return "";
        }
        tmpStr = str.split(findme);
        if (occ <= tmpStr.length) {
            ans = tmpStr[occ - 1];
        } else {
            ans = "";
        }
        return ans;
    }

    public static Properties DecodeMessage(String message) {
        String Tmark = "<tm>";
        Properties props = new Properties();
        message = message.trim().replaceAll("\\ +", " "); // strip double+ spaces

        message = message.replaceAll("\\r?\\n", " ");
        while (message.toLowerCase().contains("<is>")) {
            message = message.replace("<is>", "=");
            message = message.replace("<IS>", "=");
            message = message.replace("<Is>", "=");
            message = message.replace("<iS>", "=");
        }
        message = message.replace("<TM>", Tmark);
        message = message.replace("<Tm>", Tmark);
        message = message.replace("<tM>", Tmark);
        String[] values = message.split(Tmark);
        int nbrValues = values.length;

//        String[] values = message.split(" ");
//        int nbrValues = values.length;
        if (nbrValues > 1) {
            String[] tmp;
            for (int i = 0; i < nbrValues; i += 1) {
                if (values[i].indexOf("=") > 0 && values[i].indexOf(" = ") < 0) {
                    tmp = values[i].split("=");
                    if (tmp.length > 1) {
                        props.setProperty(tmp[0].toUpperCase(), tmp[1]);
                    } else {
                        props.setProperty(tmp[0].toUpperCase(), "");
                    }
                }
            }
        } else {
            uCommons.eMessage = "DecodeMessage Error: [" + message + "] no values found";
            uCommons.uSendMessage(uCommons.eMessage);
        }
        return props;
    }

    public static UniDynArray PrepareCsvDetails(List<String> csvLine) {
        String sTag, sAtr, sMv, sSv, sCnv, sCol, sName, sRepl, tmpCnv, sTmpl, sExt;
        UniDynArray LineArray = new UniDynArray();
        int av = 1;
        int mv = 1;
        int sv = 1, idx = 1;
        if (NamedCommon.tblCols.replaceAll("\\ ", "").equals("")) NamedCommon.tblCols = NamedCommon.burstCols;
        String tempNames = "", newCols = NamedCommon.tblCols, line;
        newCols = " " + newCols.replaceAll("\\,", " ") + " ";
        boolean junkLine = false;
        int csvLines = csvLine.size();
        for (int j = 0; j < csvLines; j++) {
            line = csvLine.get(j);
            junkLine = line.replaceAll("\\ ", "").equals("");
            junkLine = (junkLine || line.startsWith("#"));
            if (junkLine) continue;

            while (line.contains(",,")) {
                line = line.replaceAll("\\,\\,", ", ,");
            }
            line+= ", , , , , , , , ";

            // -------------------------------- sTag ---------------------------------------//
            //  "s"  String, Tag - user based tag for processing                            //
            // allow customers to "tag" columns for various data and columns types          //
            //  "*" is for maxtype  rFuel.properties maxcol or "varchar(max) null"          //
            //  "-" is for keytype  rFuel.properties keycol or "varchar(150) not null"      //
            //  "." is for smltype  rFuel.properties smlcol or "varchar(50) null"           //
            //  "@" is for inttype  rFuel.properties intcol or "int not null"               //
            //  ""  is for dattype  rFuel.properties datcol or "varchar(250) null           //

            sTag = uCommons.FieldOf(line, ",", 1).trim();
            sAtr = uCommons.FieldOf(line, ",", 2).replace("-", "").trim();
            sMv = uCommons.FieldOf(line, ",", 3).replace("-", "").trim();
            sSv = uCommons.FieldOf(line, ",", 4).replace("-", "").trim();
            tmpCnv = uCommons.FieldOf(line, ",", 5); // raw conversion code
            if (tmpCnv.trim().length() > 0) {
                sCnv = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")[4];
            } else {
                sCnv = "";
            }
            sName = uCommons.FieldOf(line, ",", 6).trim();
            sRepl = uCommons.FieldOf(line, ",", 7).trim();
            sTmpl = uCommons.FieldOf(line, ",", 8).trim();

            if (NamedCommon.isRest && sTmpl.trim().equals("")) {
                sTmpl = NamedCommon.Template;
            }
            if (sCnv.contains("~")) sCnv = sCnv.replace("~", ",");
            if (sAtr.equals("0")) {
                // sCol = "" + NamedCommon.sqlTarget + "_ID" + idx + "";
                //
                // column does not need _ID because it then becomes part of the table index
                // too many indicies and SQL chokes on data ingestion, unsurprisingly.
                // designate as _CK (Composite Key) instead --------------------------------
                //
                sCol = "" + NamedCommon.sqlTarget + "_ID" + idx + "";
                idx++;
            } else {
                if (sMv.equals("")) sMv = "1";
                if (sSv.equals("")) sSv = "1";
                if (!sTag.equals("")) {
                    sCol = sTag + NamedCommon.colpfx;
                } else {
                    sCol = NamedCommon.colpfx;
                }
                sCol = sCol + EmbeddedTableName(sAtr) + "_";
                sCol = sCol + EmbeddedTableName(sMv) + "_";
                sCol = sCol + EmbeddedTableName(sSv);
                if (!NamedCommon.uniBase && !sCol.contains(sName)) sCol = sTag + sName;
            }
            if (NamedCommon.isWhse) {
                if (newCols.contains(" " + sCol + " ") && !sCol.equals("")) {
                    boolean recur = false;
                    uCommons.eMessage = "   .) (stdCol) " + sCol + " occurs more than once ";
                    uCommons.uSendMessage(uCommons.eMessage);
                    newCols = newCols.trim().replaceAll("\\ \\ ", " ");
                    sCol = RecurringColumn(sCol);
                    if (sCol.equals("")) return null;
                }
                if (tempNames.contains(" " + sName + " ") && !sName.trim().equals("")) {
                    boolean recur = false;
                    uCommons.eMessage = "   .) (asName) " + sName + " occurs more than once ";
                    uCommons.uSendMessage(uCommons.eMessage);
                    tempNames = tempNames.trim().replaceAll("\\ \\ ", " ");
                    sName = RecurringColumn(sName);
                    if (sName.equals("")) return null;
                }
            }
            newCols += " " + sCol + " ";
            tempNames += " " + sName + " ";
            LineArray.insert(av, 1, 1, sAtr);
            LineArray.insert(av, 2, 1, sMv);
            LineArray.insert(av, 3, 1, sSv);
            LineArray.insert(av, 4, 1, sCnv);
            LineArray.insert(av, 5, 1, sCol);
            LineArray.insert(av, 6, 1, sName);
            LineArray.insert(av, 7, 1, sTag);
            LineArray.insert(av, 8, 1, sRepl);
            LineArray.insert(av, 9, 1, sTmpl);
            if (!sRepl.trim().equals("") && NamedCommon.isRest) {
                NamedCommon.SubsList.add(sRepl);
                NamedCommon.DataList.add("");
                NamedCommon.AsocList.add(false);
                NamedCommon.DataLineage.add("");
                NamedCommon.TmplList.add("");
            }
            av++;
        }
        newCols = newCols.replaceAll("\\ \\ ", " ");
        NamedCommon.tblCols = newCols.trim().replaceAll("\\ ", ",");
        NamedCommon.tblCols = NamedCommon.tblCols.replaceAll("\\,\\,", ",");
        tempNames = "";
        // ---------------------------------------------------------------- //
        return LineArray;
    }

    private static String RecurringColumn(String sCol) {
        String sExt = sCol;
        boolean recur = false;
        for (int oo = 1; oo < 200; oo++) {
            sExt = sCol + "_r" + oo;
            if (!NamedCommon.tblCols.contains(sExt)) {
                sCol = sExt;
                recur = true;
                break;
            }
        }
        if (!recur) {
            uCommons.eMessage = "   .) " + sCol + " occurs more than 200 times!";
            uCommons.uSendMessage(uCommons.eMessage);
            NamedCommon.ZERROR = true;
            sExt = "";
        } else {
            uCommons.eMessage = "      >> Changed to " + sCol;
            uCommons.uSendMessage(uCommons.eMessage);
        }
        return sExt;
    }

    public static String EmbeddedTableName(String amv) {
        String ans = amv;
        if (amv.toLowerCase().contains("n")) {
            ans = "ET";
            String cc = amv.substring(0, 1).toLowerCase();
            if (cc.equals("n")) {
                ans = ans + "1n";
            } else {
                ans = ans + amv;
            }
        }
        return ans;
    }

    public static void SQLDump(List<String> indata) {
        if (NamedCommon.isRest) return;
        if (HOME != NamedCommon.BaseCamp) setvars();

        if (indata.size() > 0) {
            if (!NamedCommon.BulkLoad) {
                SqlCommands.SetBatching(true);
                SqlCommands.ExecuteSQL(indata);
                SqlCommands.SetBatching(false);
                return;
            }
            BufferedWriter bWriter = null;
            String sfile = NamedCommon.sqlTarget;
            if (sfile.contains("_" + NamedCommon.MessageID)) {
                sfile = u2Commons.GetExtractFile(sfile);
            }
            sfile = sfile.replaceAll("\\.", "_");
            sfile = sfile.replaceAll("\\,", "_");
            sfile = sfile.replaceAll("\\ ", "_");
            String fname = "", fDir = "", pSCH = NamedCommon.SqlSchema;
            String datFile = "", sqlFile = "", rstFile = "", dctFile = "";
            isDel = false;
            String eXtn = ".upl";
            String dbf = NamedCommon.SqlDatabase;
            if (dbf.equals("$DB$")) {
                if (APImsg.APIget("sqldb").equals("")) {
                    dbf = "~DB~";
                } else {
                    dbf = APImsg.APIget("sqldb");
                }
            }
            String sch = NamedCommon.SqlSchema;
            if (sch.equals("$SC$")) {
                if (APImsg.APIget("schema").equals("")) {
                    pSCH = "~SC~";
                } else {
                    pSCH = APImsg.APIget("schema");
                }
            }
            if (NamedCommon.isRest) {
                fDir = HOME + uMQDir + rstDir;
                rstFile = NamedCommon.CorrelationID.replaceAll("\\.", "_");
            } else {
                if (NamedCommon.uniBase && NamedCommon.task.equals("014")) pSCH = "uni";
                fDir = HOME + uMQDir + insDir;
                if (NamedCommon.MultiMovers) {
                    NamedCommon.NextFdir++;
                    if (NamedCommon.NextFdir > NamedCommon.maxFdir) NamedCommon.NextFdir = 1;
                    String dirPart = RightHash("000"+NamedCommon.NextFdir, 3);
                    fDir += NamedCommon.slash + dirPart;
                }
                //
//                sqlFile = new Date().getTime() + "." + GetMSEC() + ".";
                datFile = "[" + dbf + "]_";
                datFile += "[" + pSCH + "]_[" + sfile + "]";
                // use pid so fetch can wait for ALL its files to be loaded before going to burst
                datFile += "_(" + NamedCommon.pid + ")_" + GetMSEC();
                sqlFile = datFile;
            }

            if (NamedCommon.isRest) {
                // ---------------------------------------------
                // this code will NEVER be executed.
                // ---------------------------------------------
                fname = rstFile;
                eXtn = rstn;
                bWriter = CreateFile(fDir, fname, eXtn);
                if (NamedCommon.ZERROR) return;
            } else {
                // ----------------------------------------------------------------------
                //      Create the dat file to dump the data into
                // ----------------------------------------------------------------------
                if (NamedCommon.BulkLoad && (!isDel)) {
                    boolean created = false;
                    while (!created) {
                        eXtn = ".upl";
                        bWriter = CreateFile(fDir, datFile, eXtn);      // e.g. datFile = [demo]_[uni]_[file]_(pid)_
                        if (NamedCommon.ZERROR) return;
                        if (bWriter != null) {
                            created = true;
                            continue;
                        }
                        datFile = "[" + dbf + "]_";
                        datFile += "[" + pSCH + "]_[" + sfile + "]";
                        if (NamedCommon.SqlSchema.equals(NamedCommon.rawSC)) {
                            datFile += "_" + GetMSEC();
                        } else {
                            datFile += "_(" + NamedCommon.serial + ")_"+GetMSEC();      // NOT Sure of this !!!
                        }
                    }
                } else {
                    eXtn = ".upl";
                    bWriter = CreateFile(fDir, sqlFile, eXtn);
                    if (NamedCommon.ZERROR) return;
                }
                fname = theFile.substring(0, (theFile.length() - eXtn.length()));
            }
            boolean doData = false;
            if (!NamedCommon.isRest && NamedCommon.BulkLoad) doData = true;
            int ww;
            boolean first = true, last = false, isOracle = NamedCommon.SqlDBJar.toUpperCase().contains("ORACLE");
            String inpLine = "", sFrom = "0", sTo = "0", cols="", aes;
            int nbrLoops = indata.size();
            for (ww = 0; ww < nbrLoops; ww++) {
                cols="";
                try {
                    // can't write an empty line at end of file
                    if (ww > 0 && !NamedCommon.isRest) bWriter.newLine();
                    inpLine = indata.get(ww);
                    inpLine = inpLine.replaceAll("\\r?\\n", "");
                    if (doData) {
                        if (first && isOracle) cols = GetColsOnly(inpLine);
                        inpLine = DataPartOnly(inpLine);
                        if (first) {
                            sFrom = inpLine.split("\\"+NamedCommon.Komma)[0];
                            first = false;
                        }
                    } else {
                        if (!NamedCommon.BulkLoad) {
                            if (NamedCommon.task.equals("012")) {
                                inpLine = inpLine.replace("$DB$", NamedCommon.rawDB);
                                inpLine = inpLine.replace("$SC$", "raw");
                            } else {
                                inpLine = inpLine.replace("$DB$", APImsg.APIget("sqldb"));
                                inpLine = inpLine.replace("$SC$", APImsg.APIget("schema"));
                            }
                        }
                    }
                    if (!cols.equals("")) {
                        // Oracle only -------
                        bWriter.write(cols);
                        bWriter.newLine();
                    }
                    bWriter.write(inpLine);
                    NamedCommon.datCt++;
                } catch (IOException e) {
                    uCommons.eMessage = "[FATAL]   Cannot write to " + fname;
                    uCommons.eMessage += "\n" + e.getMessage();
                    String ZERROR = uCommons.eMessage;
                    uCommons.uSendMessage(uCommons.eMessage);
                    ReportRunError(ZERROR);
                    NamedCommon.ZERROR = true;
                    return;
                }
            }
            if (!NamedCommon.ZERROR) {
                String newFile = "";
                if (NamedCommon.BulkLoad) {
                    sTo = inpLine.split("\\"+NamedCommon.Komma)[0];
                    // ---------------------------------------------------
                    // may want to catch error in sFrom or sTo here
                    // if not integers
                    // ---------------------------------------------------
                    newFile = datFile + sFrom + "_" + sTo + ".dat";
                } else {
                    eXtn = ".sql";
                    newFile = sqlFile + ".sql";
                }
                try {
                    bWriter.newLine();
                    bWriter.flush();
                    bWriter.close();
                } catch (IOException e) {
                    uCommons.eMessage = "[FATAL]   Cannot close " + fname;
                    uCommons.eMessage += "\n" + e.getMessage();
                    NamedCommon.Zmessage = uCommons.eMessage;
                    uCommons.uSendMessage(uCommons.eMessage);
                    NamedCommon.ZERROR = true;
                }
                if (!NamedCommon.ZERROR) {
                    String oldfile = fname + ".upl";
                    String newfile = "";
                    String hostfle = "";
                    if (NamedCommon.SqlSchema.equals(NamedCommon.rawSC)) {
                        if (NamedCommon.BulkLoad) {
                            newfile = fname + ".dat";
                        } else {
                            newfile = fname + eXtn;
                        }
                        hostfle = datFile;
                    } else {
                        newfile = fDir + NamedCommon.slash + newFile;
                        hostfle = newFile.replace(eXtn, "");
                    }

                    if (NamedCommon.isWhse) {
                        boolean doHost = (NamedCommon.tHostList.size() > 1);
                        if (doHost) {
                            bWriter = CreateFile(fDir, hostfle, ".htx");
                            String oldHost = fDir + NamedCommon.slash + hostfle + ".htx";
                            String newHost = oldHost.replace(".htx", ".hosts");
                            for (int th = 0; th < NamedCommon.tHostList.size(); th++) {
                                try {
                                    bWriter.write(NamedCommon.tHostList.get(th));
                                    bWriter.newLine();
                                } catch (IOException e) {
                                    uCommons.eMessage = "[FATAL]   Cannot write to " + fname + eXtn;
                                    uCommons.eMessage += "\n" + e.getMessage();
                                    uCommons.uSendMessage(uCommons.eMessage);
                                    NamedCommon.ZERROR = true;
                                    NamedCommon.Zmessage = uCommons.eMessage;
                                    return;
                                }
                            }
                            try {
                                bWriter.newLine();
                                bWriter.flush();
                                bWriter.close();
                            } catch (IOException e) {
                                uCommons.eMessage = "[FATAL]   Cannot close " + fname + eXtn;
                                uCommons.eMessage += "\n" + e.getMessage();
                                uCommons.uSendMessage(uCommons.eMessage);
                                NamedCommon.ZERROR = true;
                                NamedCommon.Zmessage = uCommons.eMessage;
                            }
                            if (NamedCommon.debugging) {
                                uCommons.uSendMessage("Rename: " + oldHost);
                                uCommons.uSendMessage("    To: " + newHost);
                            }
                            if (NamedCommon.BulkLoad) RenameFile(oldHost, newHost);
                        }
                        RenameFile(oldfile, newfile);
                    }
                }
            }
        } else {
            uCommons.uSendMessage("SQLDump() - no action!");
        }
    }

    public static String GetMSEC() {
        String rightnow = uCommons.GetLocaltimeStamp();
        String MSec = uCommons.FieldOf(rightnow, " ", 2);
        MSec = MSec.replace(":", "");
        MSec = MSec.replace(".", "");
//        Date date = new Date();
//        long time = date.getTime();
//        String MSec = "";
//        long millis = Calendar.getInstance().get(Calendar.MILLISECOND);
//        int minutes = Calendar.getInstance().get(Calendar.MINUTE);
//        int seconds = Calendar.getInstance().get(Calendar.SECOND);
//        MSec = String.valueOf(RightHash("000" + minutes, 2));
//        MSec += String.valueOf(RightHash("000" + seconds, 2));
//        MSec += String.valueOf(RightHash("000" + millis, 3));
        return MSec;
    }

    public static int ADMPdump(int counter, ArrayList array) {
        String dirLoc = NamedCommon.BaseCamp + "/data/external/";
        String vSch = NamedCommon.SqlSchema;
        if (NamedCommon.uniBase) vSch = "uni";
        String fileBase = "["+NamedCommon.SqlDatabase+"].["+vSch+"].["+NamedCommon.sqlTarget+"]_"+NamedCommon.pid+"_";
        String admpFile = fileBase+counter+".dat";
        while (FileExists(dirLoc + admpFile)) {
            counter++;
            admpFile = fileBase+counter+".dat";
        }
        boolean ok = uCommons.WriteArrayToDisk(dirLoc + admpFile, array);
        if (!ok) {
            uCommons.uSendMessage(NamedCommon.block);
            uCommons.uSendMessage("ADMP Data not dumped");
            uCommons.uSendMessage(NamedCommon.block);
        }
        return counter;
    }

    public static boolean RenameFile(String oldName, String newName) {
        // both files must be in fqn convention
        boolean okay = false;
        File hFile = new File(oldName);
        File dFile = new File(newName);
        boolean junk;
        junk = hFile.setExecutable(true, false);
        junk = hFile.setReadable(true, false);
        junk = hFile.setWritable(true, false);

        if (dFile.exists()) {
            if (NamedCommon.KeepData) {
                // This is for mdkeep=true   --------------------------------------------
                // The file has been saved for collection by another system (e.g. ADMP)
                // but that system has yet to come collect the file. Must not overwrite !
                // ----------------------------------------------------------------------
                int ctr = 0;
                String extn = newName.substring(newName.indexOf("."), newName.length());        // e.g. .dat
                String thisName = newName.substring(0, newName.indexOf("."));
                String useName = "";
                while (dFile.exists()) {
                    dFile = null;
                    ctr++;
                    useName = thisName + "_" + String.valueOf(ctr) + extn;
                    dFile = new File(useName);
                    if (ctr > 1000) {
                        uCommons.uSendMessage("<<FAIL>> " + newName + " ... has not been collected !!");
                        return false;
                    }
                }
                extn = "";
                thisName = "";
                useName = "";
            } else {
                uCommons.uSendMessage("<<FAIL>> " + newName + "  ... already exists.");
                return false;
            }
        }

        int trycnt = 0;
        while (trycnt < 3 && !okay) {
            trycnt++;
            if (hFile.renameTo(dFile)) {
                okay = true;
            } else {
                File chkF = new File(newName);
                // make sure it hasn't been renamed by another process.
                if (!chkF.exists()) {
                    uCommons.uSendMessage("<<FAIL>> Cannot rename " + oldName + " to " + newName);
                    okay = false;
                }
                chkF = null;
            }
        }
        hFile = null;
        dFile = null;
        return okay;
    }

    public static boolean FileExists(String filename) {
        boolean okay = true;
        File chkF = new File(filename);
        if (!chkF.exists()) okay = false;
        chkF = null;
        return okay;
    }

    public static boolean DeleteFile(String filename) {
        boolean okay = true;
        int tries=0, max=10;

        File chkF = new File(filename);
        if (!chkF.exists()) return okay;

        while (!chkF.delete()) {
            uCommons.Sleep(0);
            tries++;
            if (tries > max) {
                okay = false;
                break;
            }
        }
        chkF = null;
        return okay;
    }

    public static String nixExecCmd(String cmd, int limit) {
        StringBuilder ans = new StringBuilder();
        String line = "";
        int cnt=0;
        try {
            Process p = rt.exec(cmd);
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));
            while ((line = stdInput.readLine()) != null) {
                if (line == null) continue;
//                if (line.equals("") || line.contains("null")) continue;
                ans.append(line + "\n");
                cnt++;
                if (cnt >= limit) break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ans.toString();
    }

    public static void nixExecute(String cmd, boolean showOutput) {
        try {
            if (showOutput) uCommons.uSendMessage("Execute cmd:  " + cmd);
            Process p = rt.exec(cmd);

            if (showOutput) {
                BufferedReader stdInput = new BufferedReader(new
                        InputStreamReader(p.getInputStream()));

                BufferedReader stdError = new BufferedReader(new
                        InputStreamReader(p.getErrorStream()));

                boolean first = true;
                while ((oCmd = stdInput.readLine()) != null) {
                    if (first) {
                        uCommons.uSendMessage("Output from the command:");
                        first = false;
                    }
                    uCommons.uSendMessage("  > " + oCmd);
                }

                first = true;
                while ((oCmd = stdError.readLine()) != null) {
                    if (first) {
                        uCommons.uSendMessage("Errors from the command:");
                        first = false;
                    }
                    uCommons.uSendMessage("  > " + oCmd);
                }
            }
        } catch (IOException e) {
            uCommons.uSendMessage(e.getMessage());
            uCommons.uSendMessage("exec() failed for :: " + cmd);
        }
    }

    public static String DataPartOnly(String strInpLine) {
        String datLine = "", values = "\\) VALUES \\(";
        datLine = FieldOf(strInpLine, values, 2).trim();
        if (datLine.length() > 0) {
            datLine = datLine.substring(0, datLine.length() - 1); // strip the trailing ")"
        } else {
            datLine = strInpLine;
        }
        return datLine;
    }

    public static String GetColsOnly(String strInpLine) {
        //
        // This is for Oracle Only. Oracle wants row 1 to be the column names
        //
        if (!NamedCommon.SqlDBJar.toUpperCase().contains("ORACLE")) return "";
        String colnames="";
        colnames = FieldOf(strInpLine, "\\) VALUES \\(", 1).trim();
        colnames = FieldOf(colnames, "\\(", 2);
        colnames = "#" + colnames;
        return colnames;
    }

    public static BufferedWriter GetOSFileHandle(String fqName) {
        BufferedWriter bWriter = null;
        File outFile = new File(fqName);
        try {
            bWriter = new BufferedWriter(new FileWriter(outFile));
        } catch (IOException e) {
            if (outFile.exists()) {
                uCommons.uSendMessage("Cannot access " + fqName);
                uCommons.uSendMessage(e.getMessage());
            } else {
                uCommons.uSendMessage("Cannot find " + fqName + ". This process will create it.");
                int lastPos = NumberOf(fqName, "/") + 1;
                String fileName = FieldOf(fqName, "/", lastPos);
                String filePath = fqName.replace(fileName, "");
//                bWriter = CreateFile(fqName, "", "");
                bWriter = CreateFile(filePath, fileName, "");
            }
        }
        return bWriter;
    }

    public static BufferedWriter CreateFile(String fDir, String fName, String extn) {
        boolean ok = false;
        String fIdentifier = "", nxt = "";
        File outFile = null;
        BufferedWriter bWriter = null;
        int tryCnt = 0;
        int fCnt = 0;
        ok = false;
        fIdentifier = "";
        boolean stpFlg = false;
        while (!stpFlg) {
            if (fCnt > 50) {
                uCommons.eMessage = "Cannot find a dump-file number for " + fIdentifier;
                NamedCommon.Zmessage = uCommons.eMessage;
                uCommons.uSendMessage(uCommons.eMessage);
                NamedCommon.ZERROR = true;
                return null;
            }
            try {
               if (fDir.endsWith(NamedCommon.slash)) {
                  fIdentifier = fDir + fName + extn;
               } else {
                   if (fCnt > 0) nxt = "-" + fCnt;
                   if (!fName.equals("")) {
                       fIdentifier = fDir + NamedCommon.slash + fName + nxt + extn;
                   } else {
                       fIdentifier = fDir + fName + extn;
                   }
               }
               outFile = new File(fIdentifier);
               if (!outFile.exists()) {
                   ok = outFile.createNewFile();
                   if (!ok) {
                       if (NamedCommon.debugging) uCommons.uSendMessage(">> issue creating " + fIdentifier + "  ... trying again.");
                       return null;
                   }
                   if (!outFile.canWrite()) {
                       uCommons.uSendMessage(">> Write permission denied " + fIdentifier + " !!");
                       fCnt++;
                       ok = false;
                   }
               } else {
                   fCnt++;
                   try {
                        Thread.sleep(250);
                   } catch (InterruptedException e) {
                       uSendMessage(e.getMessage());
                   }
                   if (extn.equals(".sql") && fCnt > NamedCommon.MaxDat) fCnt = 1;
               }
               if (ok) stpFlg = true;
            } catch (IOException e) {
                uCommons.uSendMessage("Error accessing " + fIdentifier);
                uCommons.uSendMessage("Try # " + tryCnt + "  " + e.getMessage());
                File makeFile = new File(fDir);
                ok = makeFile.mkdirs();   // create directories if possible
                tryCnt++;
                if (tryCnt > 2) stpFlg = true;
            }
        }
        if (!ok) {
            uCommons.eMessage = "[FATAL]   Cannot create " + fIdentifier;
            NamedCommon.Zmessage = uCommons.eMessage;
            uCommons.uSendMessage(uCommons.eMessage);
            NamedCommon.ZERROR = true;
            uSendMessage("Data Path: " + fDir);
            uSendMessage("Data File: " + fName + extn);
            return null;
        }
        try {
            if (NamedCommon.BulkLoad && (!isDel)) {
                bWriter = new BufferedWriter(new FileWriter(outFile, true));  // true = append mode
            } else {
                bWriter = new BufferedWriter(new FileWriter(outFile));
            }
        } catch (IOException e) {
            uCommons.eMessage = "[FATAL]   Cannot create Writer to " + fIdentifier;
            NamedCommon.Zmessage = uCommons.eMessage;
            uCommons.uSendMessage(uCommons.eMessage);
            NamedCommon.ZERROR = true;
            return null;
        }
        theFile = fIdentifier;
        return bWriter;
    }

    public static UniDynArray SQL2UVRec(String data) {
        UniDynArray rec = new UniDynArray();
        String tmp;
        int av, mv, sv;
        String[] AtrFields = new String[]{};
        String[] ItemFields = new String[]{};
//        ItemFields = uStrings.gSplit2Array(data, NamedCommon.IMark);
        ItemFields = uStrings.jSplit2Array(data, NamedCommon.IMark);
        if (ItemFields.length > 1) {
            // ignore ItemFields[0] which is the uID
            data = ItemFields[1];
        } else {
            // there is no uID - just data
            data = ItemFields[0];
        }
        boolean isEmpty = true;
//        AtrFields = uStrings.gSplit2Array(data, NamedCommon.FMark);
        AtrFields = uStrings.jSplit2Array(data, NamedCommon.FMark);
        int nbrAtr = AtrFields.length;
        for (int a = 0; a < nbrAtr; a++) {
            tmp = AtrFields[a];
            String[] MvFields = new String[]{};
//            MvFields = uStrings.gSplit2Array(tmp, NamedCommon.VMark);
            MvFields = uStrings.jSplit2Array(tmp, NamedCommon.VMark);
            int nbrMvs = MvFields.length;
            for (int m = 0; m < nbrMvs; m++) {
                tmp = MvFields[m];
                String[] SvFields = new String[]{};
//                SvFields = uStrings.gSplit2Array(tmp, NamedCommon.SMark);
                SvFields = uStrings.jSplit2Array(tmp, NamedCommon.SMark);
                int nbrSvs = SvFields.length;
                for (int s = 0; s < nbrSvs; s++) {
                    tmp = SvFields[s];
                    av = a + 1;
                    mv = m + 1;
                    sv = s + 1;
                    rec.insert(av, mv, sv, tmp);
                    isEmpty = false;
                }
            }
        }
        if (isEmpty) rec.insert(1, 0, 0, "EmptyRecord");
        return rec;
    }

    public static String UV2SQLRec(UniString ID, UniString record) {
        UniDynArray inrec = new UniDynArray(record);
        String datum = "";
        StringBuilder outVal= new StringBuilder();
        if (ID != null) outVal.append(String.valueOf(ID) + "<im>");
        int nbrAtr, nbrMVs, nbrSVs;
        if (record == null) {
            nbrAtr = 0;
        } else {
            nbrAtr = inrec.dcount(0);
        }

        for (int a = 1; a <= nbrAtr; a++) {
            if (a > 1) outVal.append(NamedCommon.FMark);
            nbrMVs = inrec.dcount(a);
            for (int m = 1; m <= nbrMVs; m++) {
                if (m > 1) outVal.append(NamedCommon.VMark);
                nbrSVs = inrec.dcount(a, m);
                for (int s = 1; s <= nbrSVs; s++) {
                    if (s > 1) outVal.append(NamedCommon.SMark);
                    datum = inrec.extract(a, m, s).toString();
                    outVal.append(datum);
                }
            }
        }
        datum = outVal.toString();
        inrec = null;
        outVal= null;
        outVal= null;
        return datum;
    }

    public static void SetCommons(Properties runProps) {
        String opsys = System.getProperty("os.name");
        if (opsys.toLowerCase().contains("window")) NamedCommon.slash = "\\";

        // never put the AES check first or encryption will fail
        NamedCommon.AES              = false;
        NamedCommon.secret           = "";
        NamedCommon.salt             = "";
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);

        NamedCommon.Scrub_uID        = (GetValue(runProps, "scrub.ids", "false").equals("true"));
        NamedCommon.ADMP             = (GetValue(runProps, "admp.interface", "false").equals("true"));
        NamedCommon.VPN              = (GetValue(runProps, "using.vpn", "false").equals("true"));
        NamedCommon.datOnly          = (GetValue(runProps, "dat.only", "false").equals("true"));
        NamedCommon.mapLooping       = (GetValue(runProps, "map.looping", "false").equals("true"));
        NamedCommon.jdbcLoader       = (GetValue(runProps, "jdbcloader", "false").equals("true"));
        NamedCommon.jdbcAdd          = (GetValue(runProps, "jdbcAdd", ""));
        NamedCommon.jLogs            = (GetValue(runProps, "jlogs", "false").equals("true"));
        NamedCommon.escXML           = (GetValue(runProps, "escxml", "false").equals("true"));
        NamedCommon.artemis          = (GetValue(runProps, "bkrtype", "activemq").equals("artemis"));
        NamedCommon.AutoVault        = (GetValue(runProps, "autovault", "false").equals("true"));
        NamedCommon.AMS              =  GetValue(runProps, "ams", "");
        NamedCommon.uvReset          = (GetValue(runProps, "uvreset", "false").equals("true"));
        NamedCommon.encRaw           = (GetValue(runProps, "encraw", "false").equals("true"));
        NamedCommon.StopOnSlowDB     = (GetValue(runProps, "stoponslow", "false").equals("true"));
        NamedCommon.ShowDateTime     = (GetValue(runProps, "showdt", "true").equals("true"));
        NamedCommon.minPoolSize      =  GetValue(runProps, "minpoolsize", "1");
        NamedCommon.maxPoolSize      =  GetValue(runProps, "maxpoolsize", "5");
        NamedCommon.PoolDebug        = (GetValue(runProps, "pooldebug", "false").equals("true"));
        NamedCommon.showDNSresoltion = (GetValue(runProps, "showDNS", "false").equals("true"));
        NamedCommon.uSecure          = (GetValue(runProps, "secure", "false").equals("true"));
        NamedCommon.CPL              = (GetValue(runProps, "cpl", "false").equals("true"));
        NamedCommon.uplSite          =  GetValue(runProps, "uplsite", "");
        NamedCommon.ServiceType      =  GetValue(runProps, "service", "");
        NamedCommon.KeepData         = (GetValue(runProps, "mdkeep", "false").equals("true"));
        NamedCommon.EncBaseSql        = (GetValue(runProps, "enc.base.sql", "false").equals("true"));
        NamedCommon.sendACK          = (GetValue(runProps, "reply-required", "false").equals("true"));
        NamedCommon.KeepSecrets      = (GetValue(runProps, "vnda", "false").equals("true"));
        NamedCommon.CleanseData      = (GetValue(runProps, "cleanser", "false").equals("true"));
        boolean hardcodeDocker       = (GetValue(runProps, "docker", "false").equals("true"));
        NamedCommon.isRDS            = (GetValue(runProps, "rds", "false").equals("true"));
        NamedCommon.multihost        = (GetValue(runProps, "multihost", "false").equals("true"));
        NamedCommon.emptyrows        = (GetValue(runProps, "emptyrows", "true").equals("true")); // default = true
        NamedCommon.useProxy         = (GetValue(runProps, "proxy", "false").equals("true"));
        NamedCommon.SmartMovers      = (GetValue(runProps, "smart.movers", "false").equals("true"));
        NamedCommon.Token            = (GetValue(runProps, "pxtoken", ""));
        NamedCommon.pxHost           = (GetValue(runProps, "pxhost", ""));
        NamedCommon.RDSdir           =  GetValue(runProps, "rdsdir", "");
        NamedCommon.AZdir            =  GetValue(runProps, "azure.source", "");
        NamedCommon.kafkaBase        =  GetValue(runProps, "kafkabase", "");
        NamedCommon.topicExtn        =  GetValue(runProps, "topicextn", "");
        NamedCommon.bkr_user         =  GetValue(runProps, "bkruser", NamedCommon.bkr_user);
        NamedCommon.bkr_pword        =  GetValue(runProps, "bkrpword", NamedCommon.bkr_pword);
        NamedCommon.esbfmt           =  GetValue(runProps, "esbfmt", "XML");
        NamedCommon.cSeed            =  GetValue(runProps, "cseed", "");
        NamedCommon.Mandatory        =  GetValue(runProps, "mandatory.items", "");
        NamedCommon.KeystorePassword =  GetValue(runProps, "uplKeystore", "");
        NamedCommon.webservices      =  GetValue(runProps, "webservices", "");
        NamedCommon.PartScheme       =  GetValue(runProps, "partition.scheme", NamedCommon.PartScheme);
        NamedCommon.TimeZone         =  GetValue(runProps, "timezone", "");
        if (NamedCommon.TimeZone.equals("")) NamedCommon.TimeZone = String.valueOf(ZoneId.systemDefault());

        // Warning: TransactionIsolation WILL cause severe perfmormance issues when multi.movers is true
        String.valueOf(NamedCommon.TranIsolation    = (GetValue(runProps, "transaction.isolation", "false").equals("true")));

        if (!NamedCommon.debugging) {
            NamedCommon.debugging    = (GetValue(runProps, "debugmaster", "false").equals("true"));
        }

        NamedCommon.noLock           = (GetValue(runProps, "nolock", "false").equals("true"));
        NamedCommon.krbDefault       = (GetValue(runProps, "kerberos", "false").equals("true"));
        NamedCommon.SqlUseStmt       = (GetValue(runProps, "sql.use.statement", "false").equals("true"));

        // To have more than 2 or 3 Fetch and Burst queues, we need more MoveRaw and MoveData processes
        // if multi.movers=true then create data/ins/001, 002, ... directories.
        // max.insdirs is the highest directory number that is monitored. (runcontrol StartUp)

        NamedCommon.MultiMovers      = (GetValue(runProps, "multi.movers", "false").toLowerCase().equals("true"));
        // -------------------------------------------------------------------------------------------------------------
        if (!NamedCommon.isDocker) NamedCommon.isDocker = hardcodeDocker;
        if (NamedCommon.ADMP) NamedCommon.KeepData = false;
        String ackMode = GetValue(runProps, "mqackmode", "auto");
        if (ackMode.toLowerCase().equals("client")) NamedCommon.MQackMode = NamedCommon.MQ_CLIENT_ACKNOWLEDGE;

        Properties srcProps = runProps;

        NamedCommon.databaseType = GetValue(srcProps, "dbtype", NamedCommon.databaseType);
        NamedCommon.protocol = GetValue(srcProps, "protocol", NamedCommon.protocol);
        switch (NamedCommon.protocol) {
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
                // DO NOT break - allow it to fall through the u2cs block.
            case "u2cs":
                NamedCommon.dbhost = GetValue(srcProps, NamedCommon.dbgPfx + "u2host", NamedCommon.dbhost);
                NamedCommon.dbPort = GetValue(srcProps, NamedCommon.dbgPfx + "u2port", NamedCommon.dbPort);
                NamedCommon.dbpath = GetValue(srcProps, "u2path", NamedCommon.dbpath);
                NamedCommon.dbuser = GetValue(srcProps, "u2user", NamedCommon.dbuser);
                NamedCommon.passwd = GetValue(srcProps, "u2pass", NamedCommon.passwd);
                NamedCommon.datAct = GetValue(srcProps, "u2acct", NamedCommon.datAct);
                NamedCommon.realhost = "";
                NamedCommon.realuser = "";
                NamedCommon.realdb = "";
                NamedCommon.realac = "";
                break;
            case "real":
                NamedCommon.realhost = GetValue(runProps, "realhost", NamedCommon.realhost);
                NamedCommon.realuser = GetValue(runProps, "realuser", NamedCommon.realuser);
                NamedCommon.realdb   = GetValue(runProps, "realdb", NamedCommon.realdb);
                NamedCommon.realac   = GetValue(runProps, "realac", NamedCommon.realac);
                NamedCommon.datAct   = NamedCommon.realac;
                if (!NamedCommon.realac.replaceAll("\\ ", "").equals("")) {
                    NamedCommon.datAct = NamedCommon.realac.split("\\,")[0];
                }
                NamedCommon.dbhost   = "";
                NamedCommon.dbpath   = "";
                NamedCommon.dbuser   = "";
                NamedCommon.passwd   = "";
                NamedCommon.datAct   = "";
                break;
            default:
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage="";
                uCommons.uSendMessage("No source DB host defination available to this process.");
        }

        srcProps = null;
        // -------------------------------------------------------------------------------------------------------------
        NamedCommon.InMount = GetValue(runProps, "mntin", NamedCommon.InMount);
        NamedCommon.OutMount = GetValue(runProps, "mntout", NamedCommon.OutMount);
        NamedCommon.gmods = GetValue(runProps, "gmods", NamedCommon.gmods);
        // ------------ [JDBC setup] -----------------------------------------------------------------------------------
        NamedCommon.SqlRole = "";
        NamedCommon.SqlWarehouse = "";
        NamedCommon.jdbcSep  = ";";
        NamedCommon.jdbcAuth = GetValue(runProps, "jdbcAuth", "");
        // next line save a shit tonne of escaping ad line-continuation headaches !!
        NamedCommon.jdbcAuth = NamedCommon.jdbcAuth.replace("~", "\\");
        NamedCommon.jdbcRealm= GetValue(runProps, "jdbcRealm", "");
        // jdbcUser must ONLY ne used for Sql Server AUTHORIZATION !!!
        NamedCommon.jdbcUser = GetValue(runProps, "jdbcUsr", "");
        NamedCommon.SystemSchema = "";
        if (!NamedCommon.datOnly) {
            NamedCommon.SqlDBJar = GetValue(runProps, "sqljar", NamedCommon.SqlDBJar);
            NamedCommon.jdbcCon = GetValue(runProps, NamedCommon.dbgPfx + "jdbcCon", NamedCommon.jdbcCon);
            NamedCommon.jdbcDvr = GetValue(runProps, NamedCommon.dbgPfx + "jdbcDriver", "");
            String jdbcDbi = GetValue(runProps, "jdbcDbi", "");
            String jdbcUsr = GetValue(runProps, "jdbcUsr", "");
            String jdbcPwd = GetValue(runProps, "jdbcPwd", "");
            String jdbcDwh = GetValue(runProps, "jdbcDwh", "");
            String jdbcSch = GetValue(runProps, "jdbcSch", "");
            String jdbcRol = GetValue(runProps, "jdbcRol", "");
            if (NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE")) {
                NamedCommon.SqlRole = jdbcRol;
                NamedCommon.SqlWarehouse = jdbcDwh;
                NamedCommon.jdbcSep = "&";
                NamedCommon.SystemSchema = jdbcSch;
            }
            NamedCommon.rawDB = GetValue(runProps, "rawDB", NamedCommon.rawDB);
            NamedCommon.rawSC = GetValue(runProps, "rawSC", NamedCommon.rawSC);
            NamedCommon.datPath = GetValue(runProps, "datPath", NamedCommon.datPath);
            while (NamedCommon.datPath.endsWith(";")) { NamedCommon.datPath = NamedCommon.datPath.substring(0, NamedCommon.datPath.length()-1); }
            NamedCommon.catchErrs = (GetValue(runProps, "sqlerrors", String.valueOf(NamedCommon.catchErrs)).equals("true"));
            NamedCommon.allowDups = (GetValue(runProps, "allowdups", String.valueOf(NamedCommon.allowDups)).equals("true"));
            NamedCommon.uniBase = (GetValue(runProps, "uniBase", String.valueOf(NamedCommon.uniBase)).equals("true"));
            NamedCommon.vwPrefix = GetValue(runProps, "vwpfx", NamedCommon.vwPrefix);
            NamedCommon.DropIt = (GetValue(runProps, "drop", String.valueOf(NamedCommon.DropIt)).equals("true"));
            NamedCommon.TruncIt = (GetValue(runProps, "trunc", String.valueOf(NamedCommon.TruncIt)).equals("true"));
            NamedCommon.maxType = " " + (GetValue(runProps, "maxcol", "varchar(max) null"));
            NamedCommon.intType = " " + (GetValue(runProps, "intcol", "int not null"));
            NamedCommon.smlType = " " + (GetValue(runProps, "smlcol", "varchar(50) null"));
            NamedCommon.keyType = " " + (GetValue(runProps, "keycol", "varchar(150) null"));
            NamedCommon.datType = " " + (GetValue(runProps, "datcol", "varchar(250) null"));
            if (NamedCommon.datOnly) {
                NamedCommon.jdbcCon = "";
                NamedCommon.SqlDBJar= "";
            } else {
                if (NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE")) {
                    NamedCommon.jdbcCon +=
                            "?user=" + jdbcUsr +
                                    "&password=" + jdbcPwd +
                                    "&role=" + jdbcRol +
                                    "&db=" + jdbcDbi +
                                    "&schema=" + jdbcSch +
                                    "&warehouse=" + jdbcDwh;
                } else {
                    if (NamedCommon.SqlDBJar.toUpperCase().equals("MSSQL")) {
                        MakeJdbcCon(runProps);
                    }
                    if (!NamedCommon.jdbcCon.equals("") && !NamedCommon.jdbcCon.endsWith(NamedCommon.jdbcSep)) NamedCommon.jdbcCon += NamedCommon.jdbcSep;

                    if (!jdbcUsr.equals("")) {
                        if (NamedCommon.jdbcCon.indexOf("user=") < 0) {
                            NamedCommon.jdbcCon += "user=" + jdbcUsr + NamedCommon.jdbcSep;
                        }
                    }
                    if (!jdbcPwd.equals("")) {
                        if (NamedCommon.jdbcCon.indexOf("password=") < 0) {
                            NamedCommon.jdbcCon += "password=" + jdbcPwd + NamedCommon.jdbcSep;
                        }
                    }
                }
            }
            NamedCommon.jdbcCon = NamedCommon.jdbcCon.replace(NamedCommon.jdbcSep+NamedCommon.jdbcSep, NamedCommon.jdbcSep);
        }
        // -------------------------------------------------------------------------------------------------------------

        NamedCommon.rawCols = GetValue(runProps, "rawcols", NamedCommon.rawCols);
        NamedCommon.burstCols = GetValue(runProps, "tblcols", NamedCommon.tblCols);
        NamedCommon.showNulls = (GetValue(runProps, "showNulls", String.valueOf(NamedCommon.showNulls)).equals("true"));
        NamedCommon.threader = (GetValue(runProps, "threader", String.valueOf(NamedCommon.threader)).equals("true"));
        NamedCommon.mqHost = GetValue(runProps, "mqhost", NamedCommon.mqHost);
        NamedCommon.StructuredResponse = GetValue(runProps, "wraptask", "");
        NamedCommon.xmlProlog = GetValue(runProps, "xmlProlog", "<xmlProlog not in rFuel.properties>");
        NamedCommon.hostname = GetValue(runProps, "hostname", NamedCommon.hostname);
        NamedCommon.Digest = GetValue(runProps, "digest", NamedCommon.Digest);

        try {
            NamedCommon.licWarning = Integer.valueOf((GetValue(runProps, "licWarn", "30")));
        } catch (NumberFormatException e) {
            NamedCommon.licWarning = 30;
        }
        try {
            NamedCommon.BurstWait = Integer.valueOf((GetValue(runProps, "burstwait", "99")));
        } catch (NumberFormatException e) {
            NamedCommon.BurstWait = 99;
        }
        try {
            NamedCommon.ConnectAcceptable = Double.valueOf((GetValue(runProps, "slowconnect", "4.99")));
        } catch (NumberFormatException e) {
            NamedCommon.ConnectAcceptable = 4.99;
        }
        try {
            NamedCommon.mqHeartBeat = Integer.valueOf((GetValue(runProps, "heartbeat", "aaa")));
        } catch (NumberFormatException nfe) {
            try {
                NamedCommon.mqHeartBeat = Integer.valueOf((GetValue(runProps, "mqheartbeat", "120")));
            } catch (NumberFormatException nfe1) {
                NamedCommon.mqHeartBeat = 120;
            }
        }
        try {
            NamedCommon.MaxProc = Integer.valueOf((GetValue(runProps, "maxproc", "10")));
        } catch (NumberFormatException nfe) {
            NamedCommon.MaxProc = 10;
        }
        try {
            NamedCommon.dbWait = Integer.valueOf((GetValue(runProps, "dbtimeout", "300")));
        } catch (NumberFormatException nfe) {
            NamedCommon.dbWait = 300;
        }
        try {
            NamedCommon.mqWait = Integer.valueOf((GetValue(runProps, "mqtimeout", "1200")));
        } catch (NumberFormatException nfe) {
            NamedCommon.mqWait = 1200;
        }
        try {
            NamedCommon.vtPing = Integer.valueOf((GetValue(runProps, "vtping", "0")));
        } catch (NumberFormatException nfe) {
            NamedCommon.vtPing = 0;
        }
        try {
            NamedCommon.restartWait = Integer.valueOf((GetValue(runProps, "restartwait", "0")));
        } catch (NumberFormatException nfe) {
            NamedCommon.restartWait = 0;
        }
        try {
            NamedCommon.aStep = 0;
            NamedCommon.rStep = Integer.valueOf(GetValue(runProps, "rStep", "0"));
            NamedCommon.iStep = Integer.valueOf(GetValue(runProps, "iStep", "0"));
        } catch (NumberFormatException nfe) {
            NamedCommon.aStep = 299;
        }
        try {
            NamedCommon.showAT = Integer.valueOf(GetValue(runProps, "show", String.valueOf(NamedCommon.showAT)));
            NamedCommon.bshowAT= Integer.valueOf(GetValue(runProps, "bshow", "0"));
        } catch (NumberFormatException nfe) {
            NamedCommon.showAT = 2999;
        }
        try {
            NamedCommon.RQM = Integer.valueOf(GetValue(runProps, "rqm", String.valueOf(NamedCommon.RQM)));
        } catch (NumberFormatException nfe) {
            NamedCommon.RQM = 99999;
        }
        // ----------------------------------------------------------------------------------------------------------
        // DatSize is used for Bulk Inserting where each site can perform differently
        // DatRows is used when not Bulk Inserting - Fetch & Burst are different because some raw records are huge.
        // ----------------------------------------------------------------------------------------------------------
        try {
            NamedCommon.DatSize = Integer.valueOf(GetValue(runProps, "DatSize", "0"));
        } catch (NumberFormatException nfe) {
            NamedCommon.DatSize = 0;
        }
        try {
            switch (NamedCommon.task) {
                case "012":
                    NamedCommon.DatRows = Integer.valueOf(GetValue(runProps, "fDatRows", String.valueOf(NamedCommon.DatRows)));
                    break;
                case "014":
                    NamedCommon.DatRows = Integer.valueOf(GetValue(runProps, "bDatRows", String.valueOf(NamedCommon.DatRows)));
                    break;
                default:
                    NamedCommon.DatRows = Integer.valueOf(GetValue(runProps, "DatRows", String.valueOf(NamedCommon.DatRows)));
            }
        } catch (NumberFormatException nfe) {
            uCommons.uSendMessage("DatRows is non-numeric,  defaulting to 1000 *********************");
            NamedCommon.DatRows = 1000;
        }
        try {
            NamedCommon.mqPort = Integer.valueOf(GetValue(runProps, "mqport", String.valueOf(NamedCommon.mqPort)));
        } catch (NumberFormatException nfe) {
            NamedCommon.mqPort = 8555;
        }
        try {
            NamedCommon.FetchSize = Integer.valueOf(GetValue(runProps, "fetchsize", String.valueOf(NamedCommon.FetchSize)));
        } catch (NumberFormatException nfe) {
            NamedCommon.FetchSize = 1000;
        }
        try {
            NamedCommon.heavyUseMax = Integer.valueOf(GetValue(runProps, "heavyuse", ""));
        } catch (NumberFormatException nfe) {
            NamedCommon.heavyUseMax = 100000;
        }
        try {
            NamedCommon.numthreads = Integer.valueOf(GetValue(runProps, "threads", "2"));
        } catch (NumberFormatException nfe) {
            NamedCommon.numthreads = 2;
        }
        try {
            NamedCommon.pxPort = Integer.valueOf(GetValue(runProps, "pxport", String.valueOf(NamedCommon.pxPort)));
        } catch (NumberFormatException nfe) {
            NamedCommon.pxPort = 31448;
        }

        try {
            NamedCommon.maxFdir = Integer.valueOf((GetValue(runProps, "max.insdir", "1")));
        } catch (NumberFormatException e) {
            NamedCommon.maxFdir = 1;
        }

        if (NamedCommon.Expiry == 0) {
            // other processes (e.g. Http2Server) can set Expiry to be shorter than usual.
            try {
                NamedCommon.Expiry = Integer.valueOf(GetValue(runProps, "responsettl", String.valueOf(NamedCommon.Expiry)));
            } catch (NumberFormatException nfe) {
                NamedCommon.Expiry = 10000;
            }
        }

        try {
            NamedCommon.maxSmartCons = Integer.valueOf((GetValue(runProps, "max.smart.consumers", "1")));
        } catch (NumberFormatException e) {
            NamedCommon.maxSmartCons = 1;
        }

        String sparse = GetValue(runProps, "sparse", String.valueOf(NamedCommon.Sparse));
        NamedCommon.Sparse = (sparse.equals("true"));
        String bLoad = runProps.getProperty("bulk", "1");
        String comma = GetValue(runProps, "comma", NamedCommon.Komma);
        String quote = GetValue(runProps, "quote", NamedCommon.Quote);
        NamedCommon.Quote = quote;
        NamedCommon.Komma = comma;

        if (bLoad.equals("1")) NamedCommon.BulkLoad = true;
        if (bLoad.toLowerCase().equals("true")) NamedCommon.BulkLoad = true;

        NamedCommon.ErrorStop = GetValue(runProps, "stopError", "false").toLowerCase().equals("true");
        //
        // AES must be done last so that all config encryption is rFuel style.
        //
        NamedCommon.AES              = (GetValue(runProps, "aes.encryption", "false").equals("true"));
        NamedCommon.secret           = (GetValue(runProps, "aes.secret", ""));
        NamedCommon.salt             = (GetValue(runProps, "aes.salt", ""));
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
    }

    public static void MakeJdbcCon(Properties runProps) {
        NamedCommon.jdbcCon = GetValue(runProps, NamedCommon.dbgPfx + "jdbcCon", NamedCommon.jdbcCon);
        String jdbcDbi = GetValue(runProps, "jdbcDbi", "");
        if (!NamedCommon.jdbcCon.endsWith(";")) NamedCommon.jdbcCon += ";";
        if (!jdbcDbi.equals("")) NamedCommon.jdbcCon += "databaseName="+jdbcDbi;
        if (!NamedCommon.jdbcCon.endsWith(";")) NamedCommon.jdbcCon += ";";
        if (!NamedCommon.jdbcAdd.equals("")) NamedCommon.jdbcCon += NamedCommon.jdbcAdd;
        if (!NamedCommon.jdbcCon.endsWith(";")) NamedCommon.jdbcCon += ";";
    }

    public static void BkrCommons(Properties runProps) {
        NamedCommon.bkrName   = GetValue(runProps, "name", "ERROR");
        NamedCommon.bkr_user  = GetValue(runProps, "bkruser", "admin");
        NamedCommon.bkr_pword = GetValue(runProps, "bkrpword", "admin");
        String ip  = GetValue(runProps, "ip.address", "");
        String url = GetValue(runProps, "url", "");
        if (url.contains("$ip")) url = url.replace("$ip", ip);
        NamedCommon.messageBrokerUrl = url;
        url = GetValue(runProps, "jolokia.url", "");
        if (url.contains("$ip")) url = url.replace("$ip", ip);
        NamedCommon.jolokiaURL = url;
        url = GetValue(runProps, "jolokia.origin", "");
        if (url.contains("$ip")) url = url.replace("$ip", ip);
        NamedCommon.jolokiaORIGIN = url;
        NamedCommon.queueWATCH = GetValue(runProps, "queue.watch", "901.SQL");
//        String junk;
//        junk = GetValue(runProps, "bkruser", NamedCommon.bkr_user);
//        if (!junk.equals("")) NamedCommon.bkr_user = junk;
//        junk = GetValue(runProps, "bkrpword", NamedCommon.bkr_pword);
//        if (!junk.equals("")) NamedCommon.bkr_pword = junk;
//        junk = GetValue(runProps, "name", NamedCommon.bkrName);
//        if (!junk.equals("")) NamedCommon.bkrName = junk;
    }

    public static String GetValue(Properties runProps, String key, String def) {
        String value = runProps.getProperty(key, def);
        if (value.equals(null) || value.equals("")) {
            value = def;
        } else {
            if (value.startsWith("ENC(")) {
                String tmp = value.substring(4, (value.length() - 1));
                value = uCipher.Decrypt(tmp);
            }
        }
        if (value.contains("~")) value = value.replace("~", "\\");
        return value;
    }

    public static String RightHash(String value, int spaces) {
        if (value.length() > spaces) value = value.substring(value.length() - spaces, value.length());
        String fmt = "%" + spaces + "s";
        String ans = String.format(fmt, value);
        return ans;
    }

    public static String LeftHash(String value, int spaces) {
        for (int i=0 ; i <= spaces ; i++) { value += " "; }
        return value.substring(0, spaces);
    }

    public static void StartUp() {
        boolean shutdown = false;
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);      // these will be empty
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);      // these will be setup

        if (!NamedCommon.sConnected) {
            SourceDB.ConnectSourceDB();
            if (NamedCommon.sConnected) {
                uCommons.uSendMessage("SourceDB: Connected");
            } else {
                uCommons.uSendMessage("SourceDB connection FAILURE");
                System.exit(0);
            }
        }
        if (!NamedCommon.isRest) {
            if (!NamedCommon.tConnected) {
                NamedCommon.tConnected = SqlCommands.ConnectSQL();
                if (NamedCommon.tConnected) {
                    uCommons.uSendMessage("TargetDB: Connected");
                } else {
                    if (NamedCommon.datOnly) {
                        uCommons.uSendMessage("TargetDB not required");
                    } else {
                        uCommons.uSendMessage("TargetDB connection FAILURE");
                        System.exit(0);
                    }
                }
            }
        }
        uCommons.uSendMessage("Clearing old data from ./data/ins/ ------------------------------");
        int delCnt = 0;
        boolean delSw;
        File[] files = new File("./data/ins").listFiles();
        if (files != null) {
            for (File file : files) {
                delSw = false;
                if (file.getAbsolutePath().endsWith(".dat")) delSw = true;
                if (file.getAbsolutePath().endsWith(".upl")) delSw = true;
                if (file.getAbsolutePath().endsWith(".ids")) delSw = true;
                if (delSw) {
                    File theFile = new File(file.getAbsolutePath());
                    if (theFile.delete()) {
                        delCnt++;
                    } else {
                        uCommons.uSendMessage("Cannot remove " + file.getAbsolutePath());
                    }
                    theFile = null;
                }
            }
        }
        uCommons.uSendMessage("Removed " + delCnt + " old dat/upl/ids files");

        if (!u2Commons.SetupU2Controls(shutdown)) {
            uCommons.uSendMessage("<<FAIL>> cannot set rFuel STOP switch off");
        }

        if (NamedCommon.sConnected) {
            SourceDB.DisconnectSourceDB();
            uCommons.uSendMessage("SourceDB: Disconnected");
        }
        if (NamedCommon.tConnected) {
            SqlCommands.DisconnectSQL();
            uCommons.uSendMessage("TargetDB: Disconnected");
        }
        NamedCommon.MQgarbo.gc();
    }

    public static void ShutDown() {
        NamedCommon.Reset();
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) return;
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);

        ArrayList DDL = new ArrayList();
        u2Commons.SetManaged(true);
        u2Commons.SetupU2Controls(true);
        u2Commons.SetManaged(false);
        if (NamedCommon.ZERROR) return;
        if (NamedCommon.sConnected) SourceDB.DisconnectSourceDB();
        if (NamedCommon.tConnected) SqlCommands.DisconnectSQL();
        uCommons.uSendMessage("TargetDB:: Disconnected & workfiles removed");

        if (NamedCommon.restartWait > 0) {
            uCommons.uSendMessage("[ABORT] Stopping in " + NamedCommon.restartWait + " seconds.");
            Sleep(NamedCommon.restartWait);
        }
    }

    public static void Sleep(int i) {
        int time = i * 1000;
        if (i == 0) time = 500;
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            uCommons.uSendMessage("Thread Sleep error");
            uCommons.uSendMessage(e.getMessage());
        }
    }

    public static boolean isNumeric(String str) {
        boolean ans = true;
        try {
            double d = Double.parseDouble(str);
        } catch (NumberFormatException nfe) {
            ans = false;
        }
        return ans;
    }

    public static String oconvM(String base, String cc) {
        base.trim();
        if (!isNumeric(base)) return base;
        String theAns = base, sign = "", milliSec = "";
        if (base.contains("-")) {
            sign = "-";
            base = base.replaceAll("\\-", "");
        }
        if (base.contains("+")) {
            sign = "+";
            base = base.replaceAll("\\+", "");
        }
        if (cc.substring(1, 2).equals("D")) {
            int dpl;
            try {
                dpl = Integer.valueOf(cc.substring(2, 3)); //MD2,
            } catch (NumberFormatException nfe) {
                uCommons.uSendMessage("Bad conversion code: " + cc + " Setting to 0 decimal places.");
                dpl = 0;
            }
            String number = base;
            if (dpl == 0) {
                number += "";
            } else {
                int ix1 = (base.length() - dpl);
                while (ix1 < dpl) {
                    base = "0" + base;
                    ix1 = (base.length() - dpl);
                }
                String p1 = base.substring(0, ix1);
                String p2 = base.substring(ix1, base.length());
                number = p1 + "." + p2;
            }
            String cm = "", dlr = "", pct = "";
            if (cc.indexOf("~") > 0) cm = ",";
            if (cc.indexOf(",") > 0) cm = ",";
            if (cc.indexOf("$") > 0) dlr = "$";
            if (cc.indexOf("%") > 0) pct = "%";
            String fmt, zeros;
            zeros = "." + "0000000000".substring(0, dpl);
            if (zeros.equals(".")) zeros = "";
            if (cm.length() > 0) {
                fmt = "#,##0" + zeros;
            } else {
                fmt = "##0" + zeros;
            }
            double amount;
            try {
                amount = Double.parseDouble(number);
                DecimalFormat formatter = new DecimalFormat(fmt);
                theAns = dlr + formatter.format(amount) + pct;
            } catch (NumberFormatException e) {
                theAns = number;
            }
        }
        if (cc.substring(1, 2).equals("T")) {
            // Time
            base     = uCommons.FieldOf(base, "\\.", 1);
            int seconds;
            try {
                seconds =Integer.valueOf(base);
            } catch (NumberFormatException nfe2) {
                return base;
            }
            if (cc.toUpperCase().endsWith("M")) {
                // these are milliseconds, divide by 1000.
                long msec = seconds % 1000;
                seconds = (int) (seconds / 1000);
                // MTSm     : the time is in milliseconds - show the milliseconds
                // MTS-m    : the time is in milliseconds - do not show milliseconds
                if (!cc.toUpperCase().endsWith("-M")) {
                    milliSec= "." + String.valueOf(msec);
                    cc = cc.substring(0,cc.length()-1);
                } else {
                    cc = cc.substring(0,cc.length()-2);
                }
            }
            Calendar cal = Calendar.getInstance();
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            cal.add(Calendar.SECOND, seconds);
            Date timeOfDay = cal.getTime();
            String fmt = "kk:mm", tCode = "";
            if (cc.length() > 2) {
                if (seconds < 43200) fmt = "kk:mm a";
                cc = cc.toUpperCase();
                tCode = cc.substring(2, cc.length());
                switch (tCode) {
                    case "H":
                        fmt = "hh:mm a";
                        break;
                    case "S":
                        fmt = "HH:mm:ss";
                        break;
                    case "HS":
                        fmt = "hh:mm:ss a";
                        break;
                    case "SH":
                        fmt = "hh:mm:ss a";
                        break;
                    case ".":
                        fmt = "kk.mm";
                        break;
                }
            }
            SimpleDateFormat sdf = new SimpleDateFormat(fmt);
            theAns = sdf.format(timeOfDay);
            theAns = theAns.toLowerCase().replaceAll("\\ ", "");
            theAns = theAns + milliSec;
        }
        theAns = sign + theAns;
        return theAns;
    }

    public static String oconvD(String base, String cc) {
        base.trim();
        if (!isNumeric(base)) return base;
        Calendar cal = Calendar.getInstance();
        int addDays = 0;
        String sep = " ";
        if (cc.length() > 2) sep = cc.substring(2, 3);
        SimpleDateFormat D4 = new SimpleDateFormat("dd" + sep + "MM" + sep + "yyyy");
        SimpleDateFormat D2 = new SimpleDateFormat("dd" + sep + "MM" + sep + "yy");
        String Day0 = "31" + sep + "12" + sep + "1967";
        String theAns = "", fmt = "dmy";

        try {
            addDays = Integer.valueOf(base);
            Date dd = null;
            try {
                dd = D4.parse(Day0);
                cal.setTime(dd);
                cal.add(Calendar.DATE, addDays);
            } catch (ParseException e) {
                uCommons.uSendMessage("oconvD Parse error: " + e.getMessage());
                System.exit(0);
                theAns = base;
                return theAns;
            }
            if (!sep.equals(" ")) {
                if (cc.length() > 3) fmt = cc.substring(3, cc.length()).toLowerCase();
                cc = cc.substring(0, 2);
            }
            switch (sep) {
                case " ":
                    switch (fmt) {
                        case "dmy":
                            fmt = "dd MMM yyyy";
                            break;
                        case "mdy":
                            fmt = "MMM dd yyyy";
                            break;
                        case "ymd":
                            fmt = "yyyy MMM dd";
                            break;
                        case "ydm":
                            fmt = "yyyy dd MMM";
                            break;
                    }
                    break;
                default:
                    switch (fmt) {
                        case "dmy":
                            fmt = "dd" + sep + "MM" + sep + "yyyy";
                            break;
                        case "mdy":
                            fmt = "MM" + sep + "dd" + sep + "yyyy";
                            break;
                        case "ymd":
                            fmt = "yyyy" + sep + "MM" + sep + "dd";
                            break;
                        case "ydm":
                            fmt = "yyyy" + sep + "dd" + sep + "MM";
                            break;
                    }
            }
            switch (cc) {
                case "D4":
                    D4 = new SimpleDateFormat(fmt);
                    theAns = D4.format(cal.getTime());
                    if (sep.equals(" ")) theAns = theAns.toUpperCase();
                    break;
                case "D2":
                    if (sep.equals(" ")) {
                        fmt = "dd MMM yy";
                    } else {
                        fmt = "dd" + sep + "MM" + sep + "yy";
                    }
                    D2 = new SimpleDateFormat(fmt);
                    theAns = D2.format(cal.getTime());
                    if (sep.equals(" ")) theAns = theAns.toUpperCase();
                    break;
                default:
                    theAns = D4.format(cal.getTime());
            }
        } catch (NumberFormatException nfe) {
//            uCommons.uSendMessage("oconvD NFE " + nfe.getMessage());
            theAns = base;
        }
        return theAns;
    }

    public static String oconvT(String base, String cc) {
        String ans = base;
        String cnv = "";
        if (cc.length() > 2) cnv = cc.substring(2, cc.length()).toUpperCase();
        switch (cnv) {
            case "U":
                ans = base.toUpperCase();
                break;
            case "L":
                ans = base.toLowerCase();
                break;
            case "T":
                ans = ToProperCase(base);
                break;
            case "A":
                ans = ans.replaceAll("[^A-Za-z\\s]+", "");
                break;
            case "N":
                ans = ans.replaceAll("\\D+", "");
                break;
        }
        return ans;
    }

    public static int iconvD(String base, String cc) {
        // time:  MUST be hh:mm:ss
        // date:  MUST be yyyy-mm-dd
        if (cc.toUpperCase().startsWith("M")) {
            String[] tParts = base.split("\\:");
            int hh = Integer.valueOf(tParts[0]);
            int mm = Integer.valueOf(tParts[1]);
            int ss = Integer.valueOf(tParts[2]);
            LocalTime tm = LocalTime.of(hh, mm, ss);
            return tm.toSecondOfDay();
        }
        String d00 = "1967-12-31T00:00:00.00Z";
        String d01 = base + "T00:00:00.00Z";
        Instant x1 = Instant .parse(d00);
        Instant x2 = Instant .parse(d01);
        Duration d1 = Duration.between(x1, x2);
        int days = (int) d1.toDays();
        x1=null;
        x2=null;
        d1=null;
        return days;

//        String fromDte = "31-12-1967";
//        DateFormat fdf = new SimpleDateFormat("dd-MM-yyyy");
//        Date fromD;
//        try {
//            fromD = fdf.parse(fromDte);
//        } catch (ParseException e) {
//            NamedCommon.ZERROR = true;
//            NamedCommon.Zmessage = e.getMessage();
//            return 0;
//        }
//
//        String toDte   = base;
//        DateFormat sdt = new SimpleDateFormat(cc);
//        Date toD;
//        try {
//            toD = sdt.parse(toDte);
//        } catch (ParseException e) {
//            NamedCommon.ZERROR = true;
//            NamedCommon.Zmessage = e.getMessage();
//            return 0;
//        }
//
//        long difference = toD.getTime() - fromD.getTime();
//        float daysBetween = (difference / (1000*60*60*24));
//        days = (int) daysBetween;
//        return days;
    }

    public static String ToProperCase(String base) {
        String ans = "";
        String[] arr = base.split("\\ ");
        int eoi = arr.length;
        String tmp = "", cap = "", sep = "";
        for (int i = 0; i < eoi; i++) {
            tmp = arr[i].toLowerCase();
            cap = tmp.substring(0, 1).toUpperCase();
            tmp = cap + tmp.substring(1, tmp.length());
            ans += sep + tmp;
            sep = " ";
        }
        return ans;
    }

    public static String StringToMask(String cc, String Base) {
        if (Base.equals("")) return Base;
        String replString = "*@$!&";
        String[] replChr;
        replChr = new String[]{"", "", "", "", ""};
        replChr[0] = "*****************************";
        replChr[1] = "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@";
        replChr[2] = "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$";
        replChr[3] = "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
        replChr[4] = "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&";
        boolean isBrace;
        boolean isRepl;
        boolean atEnd=false;
        int posx = 0;
        int lCnv = cc.length();
        int BLen = Base.length();
        String thisAv, thisMv, thisSv;
        int eoa = u2Commons.sDcount(Base, "A");
        int eom = 0, eos = 0;
        String mask = "";
        String chR;
        int i = 1;
        for (int av = 1; av <= eoa; av++) {
            thisAv = u2Commons.sExtract(Base, av, 0, 0);
            eom = u2Commons.sDcount(thisAv, "M");
            if (av > 1) mask += NamedCommon.FMark;
            for (int mv = 1; mv <= eom; mv++) {
                thisMv = u2Commons.sExtract(thisAv, 1, mv, 0);
                eos = u2Commons.sDcount(thisMv, "S");
                if (mv > 1) mask += NamedCommon.VMark;
                for (int sv = 1; sv <= eos; sv++) {
                    thisSv = u2Commons.sExtract(thisMv, 1, 1, sv);
                    BLen = thisSv.length();
                    if (sv > 1) mask += NamedCommon.SMark;
                    i = 1;
                    posx = 0;
                    while (i < lCnv) {
                        chR = cc.substring(i, (i + 1));
                        isBrace = false;
                        isRepl = false;
                        if (chR.equals("(") || chR.equals(")")) isBrace = true;
                        if (replString.contains(chR)) isRepl = true;

                        if (!isBrace && !isRepl) {
                            switch (chR) {
                                case "#":
                                    if (cc.substring((i + 1), ((i + 1) + 1)).matches("^-?\\d+$")) {
                                        int nn = Integer.valueOf(cc.substring((i + 1), ((i + 1) + 1)));
                                        nn = posx + nn;
                                        if (nn > BLen) { nn = BLen; atEnd=true; }
                                        if (posx > nn) posx = nn;
//                                        mask = mask + Base.substring(posx, nn);
                                        mask += thisSv.substring(posx, nn);
                                        posx = nn;
                                        i++; // skip nn
                                    }
                                    break;
                                case "\\":
                                    mask += cc.substring((i + 1), (i + 1) + 1);
                                    i++; // skip the chr just inserted
                                    break;
                                case "*":
                                    mask += "*";
                            }
                        }
                        if (isRepl) {
                            int psi = replString.indexOf(chR);
                            String nextStr = cc.substring((i + 1), ((i + 1) + 1));
                            int nn = Integer.valueOf(nextStr);
                            int lstr = (psi + nn);
                            if (lstr > BLen) { lstr = BLen; atEnd=true; }
                            if (psi > lstr) psi = lstr;
                            mask += replChr[psi].substring(psi, lstr);
                            posx = posx + lstr;
                            i++; // skip the chr just used
                        }
                        i++;
                        if (atEnd) {
                            i = lCnv + 1;
                        }
                    }
                }
            }
        }
        return mask;
    }

    public static String StringFieldOf(String cc, String Base) {
        String thisAns = "MT";  /* pre-set to empty  */
        String lookIn = cc;     /* cc :: F*1         */
        String findMe = lookIn.substring(1, 2);
        String stripChr = "";
        //
        if (findMe.equals("\"") || findMe.equals("'")) {
            stripChr = findMe;
            String chkFld = cc.replaceAll(findMe, "");
            if (cc.length() - chkFld.length() != 2) {
                uCommons.uSendMessage("*** error in FieldOf conversion "+cc+" mis-matching quotes.");
                return Base;
            }
            findMe = FieldOf(cc, findMe, 2);
        }
        String dropStr = "F" + stripChr + findMe + stripChr;
        String findorig = findMe;
        Base = Base + findMe;
        char chr = findMe.charAt(0);
        int ascii = (int) chr;
        if (findMe.length() == 1) {
            boolean escapeIt = true;
            if (ascii > 64 && ascii < 91) escapeIt = false;          // do not escape alphabets
            if (ascii > 96 && ascii < 123) escapeIt = false;          // do not escape alphabets
            if (escapeIt) findMe = "\\" + findMe;
        }
        lookIn = cc.substring(dropStr.length(), cc.length());
        int occ = 0;
        if (lookIn.equals("L")) {
            String junk = Base;
            occ = junk == null ? 0 : (junk.length() - junk.replace(findMe, "").length()) + 1;
        } else {
            try {
                occ = Integer.valueOf(lookIn);
            } catch (NumberFormatException nfe) {
                uCommons.uSendMessage("*** error in FieldOf conversion ["+cc+"] Unknown \"occurance\".");
                return Base;
            }
        }
        if (Base.contains(findorig)) {
            thisAns = uCommons.FieldOf(Base, findMe, occ);
        }
        return thisAns;
    }

    public static int NumberOf(String val, String sep) {
        int ans = 0;
        String[] tmp;
        if (val.endsWith(sep)) val +="xxx";
        if (sep.length() == 1) sep = "\\" + sep;
        tmp = val.split(sep);
        ans = tmp.length-1;
        tmp = null;
        return ans;
    }

    public static String StringExtract(String cc, String Base) {
        String thisAns = "MT";
        // e.g. cc = [3-2]
        String tmp = uCommons.FieldOf(cc, "\\[", 2);
        tmp = uCommons.FieldOf(tmp, "\\]", 1);
        int f = Integer.valueOf(String.valueOf(uCommons.FieldOf(tmp, "-", 1)));
        int t = Integer.valueOf(String.valueOf(uCommons.FieldOf(tmp, "-", 2)));
        if (t > (Base.length() - f)) {
            t = Base.length() - f;
        }
        thisAns = Base.substring(f, t);
        return thisAns;
    }

    public static String UplFunction(String cc, String Base) {
        String thisAns = "MT", cnv = cc, datum = "";
        String result = "";

        int nbrAtrs = (Base.length() - Base.replaceAll(NamedCommon.FMark, "").length()) / NamedCommon.FMark.length();
        int nbrMvs = (Base.length() - Base.replaceAll(NamedCommon.VMark, "").length()) / NamedCommon.VMark.length();
        int nbrSvs = (Base.length() - Base.replaceAll(NamedCommon.SMark, "").length()) / NamedCommon.SMark.length();

        cc = cc.substring(1, cc.length());
        boolean not = (cc.substring(0, 1).equals("!"));
        if (not) cc = cc.substring(1, cc.length());
        if (cc.contains("|")) cc = cc.split("\\|")[0];
        switch (cc.toLowerCase()) {
            case "drange":
                break;
            case "cat":
                // =CAT|$var1$|" "|$var2$|' '|$var3$}=!isnull
                String hKey, hDat;
                thisAns = "";
//                List<String> junk = uStrings.gSplit2List(cnv, "|");
                List<String> junk = new ArrayList<>(Arrays.asList(cnv.split("\\|")));
                int nbrCats = junk.size(), fnd;
                for (int pp = 1; pp < nbrCats; pp++) {
                    hKey = junk.get(pp);
                    if (!hKey.equals("")) {
                        String str = hKey.substring(0, 1);
                        if (str.equals("'") || str.equals("\"")) {
                            // handle literals such as " " or ' '
                            hDat = hKey.replaceAll(str, "");
                        } else {
                            fnd = NamedCommon.SubsList.indexOf(hKey);
                            hDat = NamedCommon.DataList.get(fnd);
                        }
                        // ----------------------------------------------------------------------------------
                        if (hDat.contains(NamedCommon.VMark)) {
                            ArrayList<String> mvList = new ArrayList<>(Arrays.asList(hDat.split(NamedCommon.VMark)));
                            ArrayList<String> svList;
                            String dtm;
                            hDat = "";
                            int eov = mvList.size(), eos;
                            for (int mv=0; mv < eov ; mv++) {
                                if (mv > 0) hDat += NamedCommon.VMark;
                                svList =   new ArrayList<>(Arrays.asList(mvList.get(mv).split(NamedCommon.SMark)));
                                eos = svList.size();
                                for (int sv=0 ; sv < eos ; sv++) {
                                    if (sv > 0) hDat += NamedCommon.SMark;
                                    dtm = svList.get(sv);
                                    hDat += thisAns + dtm;
                                }
                                svList.clear();
                            }
                            mvList.clear();
                            thisAns = "";
                        }
                        // ----------------------------------------------------------------------------------
                        thisAns += hDat;
                    }
                }
                break;
            case "match":
                // if Base is in matchStr, then use it, else blank it
                String matchStr = cnv.substring(cnv.indexOf("|") + 1, cnv.length());
                for (int aa = 0; aa <= nbrAtrs; aa++) {
                    for (int mm = 0; mm <= nbrMvs; mm++) {
                        for (int ss = 0; ss <= nbrSvs; ss++) {
                            datum = Base;
                            datum = uCommons.FieldOf(datum, NamedCommon.FMark, (aa + 1));
                            datum = uCommons.FieldOf(datum, NamedCommon.VMark, (mm + 1));
                            datum = uCommons.FieldOf(datum, NamedCommon.SMark, (ss + 1));
//                            if (matchStr.indexOf(datum) > 0) {
                            if (datum.contains(matchStr)) {
                                thisAns = datum;
                                if (not) thisAns = "!x!";
                            } else {
                                thisAns = "!x!";
                                if (not) thisAns = datum;
                            }
                            result += NamedCommon.FMark + thisAns;
                        }
                    }
                }
                if (result.startsWith(NamedCommon.FMark))
                    result = result.substring(NamedCommon.FMark.length(), result.length());
                thisAns = result;
                break;
            case "assoc":
                NamedCommon.isAssoc = true;
                String assocSub = uCommons.FieldOf(cnv, "\\|", 2);
                int idx = NamedCommon.SubsList.indexOf(assocSub);
                if (idx < 0) {
                    NamedCommon.SubsList.add(assocSub);
                    NamedCommon.DataList.add("");
                    NamedCommon.AsocList.add(false);
                    idx = NamedCommon.SubsList.indexOf(assocSub);
                }
                for (int aa = 0; aa <= nbrAtrs; aa++) {
                    if (aa > 0 && aa <= nbrAtrs) result += NamedCommon.FMark;
                    for (int mm = 0; mm <= nbrMvs; mm++) {
                        if (mm > 0 && mm <= nbrMvs) result += NamedCommon.VMark;
                        for (int ss = 0; ss <= nbrSvs; ss++) {
                            if (ss > 0 && ss <= nbrSvs) result += NamedCommon.SMark;
                            datum = Base;
                            datum = uCommons.FieldOf(datum, NamedCommon.FMark, (aa + 1));
                            datum = uCommons.FieldOf(datum, NamedCommon.VMark, (mm + 1));
                            datum = uCommons.FieldOf(datum, NamedCommon.SMark, (ss + 1));
                            thisAns = datum;
                            result += thisAns;
                        }
                    }
                }
                if (result.startsWith(NamedCommon.FMark))
                    result = result.substring(NamedCommon.FMark.length(), result.length());
                thisAns = result;
                String existing = NamedCommon.DataList.get(idx);
                if (!existing.equals("")) {
                    NamedCommon.DataList.set(idx, existing + NamedCommon.FMark + thisAns);
                } else {
                    NamedCommon.DataList.set(idx, thisAns);
                }
                NamedCommon.AsocList.set(idx, true);
                break;
            case "isnull":
                if (Base.equals("")) {
                    thisAns = "true";
                    if (not) thisAns = "false";
                } else {
                    thisAns = "false";
                    if (not) thisAns = "true";
                }
                break;
            case "indexof":
                String[] errMsg = new String[10];
                errMsg[0] = "********************************************************************";
                errMsg[1] = "ERROR: with conversion [" + cnv + "]";
                errMsg[2] = "       should be \"=indexof|character|from|to\"";
                errMsg[3] = "       in the conv:- \"from\" MUST be a +/- followed by an integer. (E.g. +1)";
                errMsg[4] = "                  :- and \"to\" MUST be an integer. (E.g. 999)";
                errMsg[5] = "********************************************************************";
                String idxChar = uCommons.FieldOf(cnv, "\\|", 2);
                String strFrom = uCommons.FieldOf(cnv, "\\|", 3);
                String strTo = uCommons.FieldOf(cnv, "\\|", 4);
//                if (strFrom.equals("")) strFrom = "+0";
                if (strFrom.equals("")) strFrom = "0";
                if (strTo.equals("")) strTo = "0";
                int iFrom, iTo;

                String posFrAct = strFrom.substring(0, 1);
                String posToAct = strTo.substring(0, 1);

                if ("+-".contains(posFrAct)) {
                    strFrom = strFrom.substring(1, strFrom.length());
                } else {
                    posFrAct = "";
                }
                if ("+-".contains(posToAct)) {
                    strTo = strTo.substring(1, strTo.length());
                } else {
                    posToAct = "";

                }

//                if (!"+-".contains(posAct)) {
//                    NamedCommon.ZERROR = true;
//                    NamedCommon.Zmessage = "Bad conversion in DSL: " + cnv + " - see logs";
//                    for (int ex = 0; ex < 5; ex++) {
//                        uCommons.uSendMessage(errMsg[ex]);
//                    }
//                    return Base;
//                }



                try {
                    iFrom = Integer.valueOf(strFrom);           // idxPos is the actual pos. Then add iFrom
                } catch (NumberFormatException nfe) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Bad conversion in DSL: " + cnv + " - see logs";
                    for (int ex = 0; ex < 5; ex++) {
                        uCommons.uSendMessage(errMsg[ex]);
                    }
                    return Base;
                }
                try {
                    iTo = Integer.valueOf(strTo);
                } catch (NumberFormatException nfe) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Bad conversion in DSL: " + cnv + " - see logs";
                    for (int ex = 0; ex < 5; ex++) {
                        uCommons.uSendMessage(errMsg[ex]);
                    }
                    return Base;
                }

                int idxPos = Base.indexOf(idxChar);

                if (idxPos >= 0) {
                    switch (posFrAct) {
                        case "+":
//                            idxPos = idxPos + iFrom;
                            iFrom = idxPos + iFrom;
                            break;
                        case "-":
                            if (idxPos > iFrom) {
//                                idxPos = idxPos - iFrom;
                                iFrom = idxPos - iFrom;
                            } else {
                                iFrom = iFrom - idxPos;
//                                uCommons.uSendMessage("WARN: cannot subtract " + iFrom + " from " + idxPos + ". Left pointer at " + iFrom);
                            }
                            break;
                        default:
//                            idxPos = idxPos + iFrom;
                            // no action needed
                    }

                    switch (posToAct) {
                        case "+":
//                            idxPos = idxPos + iTo;
                            iTo = idxPos + iTo;
                            break;
                        case "-":
                            if (idxPos > iTo) {
//                                idxPos = idxPos - iTo;
                                iTo = idxPos - iTo;
                            } else {
                                idxPos = iTo - idxPos;
//                                uCommons.uSendMessage("WARN: cannot subtract " + iTo + " from " + idxPos + ". Left pointer at " + idxPos);
                            }
                            break;
                        default:
//                            idxPos = idxPos + iFrom;
                            // no action needed
                    }


                    int lx = Base.length();
                    if (lx < iTo) iTo = lx;
//                    thisAns = Base.substring(idxPos, iTo);
                    thisAns = Base.substring(iFrom, iTo);
                } else {
                    thisAns = Base;
                }
                break;
            default:
                thisAns = Base;
        }
        return thisAns;
    }

    public static boolean inDrange(String cc, String base) {
        uCommons.uSendMessage("------- inDrange("+cc+" , " + base);
        boolean ans = false;
        // -----------------------------------------------------------------
        //  cnv: =inrange|from|to
        // from: dtx        inclusive
        //   to: dtx        inclusive
        // ----------------------------------
        //  dtx: {NOW}{+-}{N[DWMY]}
        //
        //   EG: =inrange|now-6m|now
        // -----------------------------------------------------------------
        Date date = new Date();
        long rightnow = date.getTime();
        int today = (int) (rightnow / MAGIC);
        date = null;
        rightnow=0;

        String fr = uCommons.FieldOf(cc, "\\|", 3);
        String to = uCommons.FieldOf(cc, "\\|", 2);
        int low=0, high=0, holder=0;

        low = InDrangePrep(today, fr);
        high= InDrangePrep(today, to);

        if (NamedCommon.ZERROR) return ans;

        if (low > high) {
            holder = high;
            high = low;
            low  = holder;
        }

        if (low <= today && today <= high) ans = true;

        uCommons.uSendMessage("    low:  " + low);
        uCommons.uSendMessage("  today:  " + today);
        uCommons.uSendMessage("   high:  " + high);
        uCommons.uSendMessage(" answer:  " + ans);
        low=0;high=0;today=0;holder=0;

        return ans;
    }

    private static int InDrangePrep(int today, String frto) {
        int ans = 0;
        try {
            ans = Integer.valueOf(frto);
        } catch (NumberFormatException nfe) {
            if (frto.equals("now")) {
                Date date = new Date();
                long rightnow = date.getTime();
                ans = (int) (rightnow / MAGIC);
                date = null;
                rightnow = 0;
            } else if (frto.toLowerCase().startsWith("now")) {
                ans = DoDateMath(frto);
            } else {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = frto + " - ERROR in (inrange) definition";
            }
        }
        return ans;
    }

    public static int DoDateMath(String inVal) {
        inVal = inVal.replaceAll("\\ ", "");
        inVal = inVal.substring(3, inVal.length());
        int result=0;

        // -
        String math = inVal.substring(0,1);

        // 18
        String addx = inVal.substring(1,inVal.length());

        // y
        String ival = addx.substring(addx.length()-1, addx.length()).toLowerCase();

        if ("dwmy".contains(ival)) {
            addx = addx.substring(0,addx.length()-1);
        }
        try {
            result = Integer.valueOf(addx);
        } catch (NumberFormatException nfe2) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = inVal + " - ERROR in definition.";
        }
        if (math.equals("-")) result = 0 - result;
        Calendar cal = Calendar.getInstance();
        switch (ival) {
            case "d":
                cal.add(Calendar.DATE, result);
                break;
            case "w":
                cal.add(Calendar.DATE, result*7);
                break;
            case "m":
                cal.add(Calendar.MONTH, result);
                break;
            case "y":
                cal.add(Calendar.MONTH, result*12);
                break;
            default:
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "invalid timeframe [" + ival + "] - valid: DWMY only";
                return result;
        }
        Date newDate = cal.getTime();
        long dte = newDate.getTime();
        result = (int) (dte / MAGIC);
        newDate = null;
        dte =0;
        cal = null;
        return result;
    }

    public static void ReportRunError(String message) {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(NamedCommon.messageBrokerUrl);
        Connection connection = null;
        Session session = null;
        Message Qmessage = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();
            Destination destination = session.createQueue(NamedCommon.inputQueue);
            MessageConsumer consumer = session.createConsumer(destination);
            Qmessage = consumer.receive(1000);
        } catch (JMSException e) {
            uCommons.uSendMessage("*******************************************");
            uCommons.uSendMessage("** ABORT *** cannot consume the bad message");
            uCommons.uSendMessage("*******************************************");
            uCommons.Execute("rfuel -stop");
        }
        String sendMessage = "";
        if (Qmessage instanceof TextMessage) {
            try {
                sendMessage = ((TextMessage) Qmessage).getText();
            } catch (JMSException e) {
                sendMessage = message + "\nMessage Stripper Error: " + e.getMessage();
            }
        } else {
            sendMessage = message;
        }

        if (sendMessage.contains(message) || message.contains(sendMessage)) {
            uCommons.uSendMessage("the correct message was stopped **************");
        } else {
            uCommons.uSendMessage("*** check that the correct message was stopped");
        }
        String corr = "<<FAIL>> " + NamedCommon.xMap;
        Hop.start(sendMessage, "", GetNextBkr(NamedCommon.Broker), "RunERRORS", "", corr);
        Hop.start(sendMessage, "", GetNextBkr(NamedCommon.Broker), NamedCommon.AlertQ, "", corr);

    }

    public static String GetNextBkr(String broker) {
        String nextBrk = "";
        if (broker.endsWith(".bkr")) {
            nextBrk = NamedCommon.Broker.substring(0, (NamedCommon.Broker.length() - 4));
        } else {
            nextBrk = NamedCommon.Broker;
        }
        return nextBrk;
    }

    public static String DynamicSubs(String stg) {
        String ans = stg, var = "", dat = "";
        int a = 0;
        for (int x = 0; x < APImsg.GetVLISTSize(); x++) {
            var = APImsg.APIgetVLISTKey(x);
            if (ans.toUpperCase().contains(var)) {
                dat = APImsg.APIgetVLISTVal(x);
                String part1 = ans.substring(0, ans.toUpperCase().indexOf(var));
                String part2 = ans.substring((ans.toUpperCase().indexOf(var) + var.length()), ans.length());
                ans = part1 + dat + part2;
            }
        }
        if (!ans.equals(stg) && NamedCommon.debugging)
            uCommons.uSendMessage("   .) Dynamic subs: " + stg + "  >>  " + ans);
        return ans;
    }

    public static void GetThostDetails(String thost) {
        // check if it has already been done. If it has, the details
        // are in APImsg with the thost suffix.

        if (!APImsg.APIget("sqldb:" + thost).equals("")) return;

        // get the details from file and load APImsg pojo

        String key = "", val = "";
        String fqn = NamedCommon.BaseCamp + "/conf/" + thost;
        String hostDets = uCommons.ReadDiskRecord(fqn);
        if (hostDets.equals("")) {
            if (thost.equals("base-sql")) {
                SetBaseSql();
                hostDets = uCommons.ReadDiskRecord(fqn);
            } else {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = thost + " is empty - no details found!";
                uCommons.uSendMessage(NamedCommon.Zmessage);
            }
        }

        if (!NamedCommon.ZERROR) {
            String[] lines = hostDets.split("\\r?\\n");
            String line;
            int nbrLines = lines.length;
            for (int ll = 0; ll < nbrLines; ll++) {
                line = lines[ll];
                if (!line.equals("")) {
                    if (line.contains("=ENC(")) {
                        int lx = line.indexOf("=ENC(");
                        String tmp0 = line.substring(lx + 5, (line.length() - 1));
                        line = line.substring(0, lx + 1) + uCipher.Decrypt(tmp0);
                    }
                    String[] tmp1 = line.split("=");
                    key = (tmp1[0] + ":" + thost).toUpperCase();
                    if (tmp1.length > 1) {
                        val = tmp1[1];
                    } else {
                        val = APImsg.APIget(tmp1[0]);
                    }
                    APImsg.APIset(key, val);
                }
            }
            String tst;
            tst = APImsg.APIget("sqldb:" + thost);
            if (tst.equals("")) APImsg.APIset("sqldb:" + thost, APImsg.APIget("sqldb"));

            tst = APImsg.APIget("schema:" + thost);
            if (tst.equals("")) APImsg.APIset("schema:" + thost, APImsg.APIget("schema"));
        }

    }

    public static void SetupReturnCodes() {
        for (int rc = 0; rc < 1100; rc++) { NamedCommon.ReturnCodes.add(""); }
        int nbrvals;
        String hCodes = ReadDiskRecord(NamedCommon.BaseCamp + "/conf/http-return-codes.csv");
        if (!hCodes.equals("")) {
            String[] tmp = hCodes.split("\\r?\\n");
            nbrvals = tmp.length;
            int idx;
            for (int rc = 0; rc < nbrvals; rc++) {
                String[] tmp1 = tmp[rc].split(",");
                try {
                    idx = Integer.valueOf(tmp1[0]);
                    NamedCommon.ReturnCodes.set(idx, tmp1[1]);
                } catch (NumberFormatException nfe) {
                    // skip the line
                }
            }
        }
    }

    public static void Register(String uTask, String uQue, String pid) {
        String thisProc = uTask + "\t" + uQue + "\t" + pid;
        String active = ReadDiskRecord(NamedCommon.BaseCamp + "/conf/running.jobs");
        ArrayList<String> lines = new ArrayList<String>(Arrays.asList(active.split("\n")));
        boolean exists = false;
        for (int l=0 ; l<lines.size() ; l++) {
            if (lines.get(l).equals(thisProc)) { exists = true; break; }
        }
        if (!exists) {
            active += thisProc + "\n";
            WriteDiskRecord(NamedCommon.BaseCamp + "/conf/running.jobs", active);
        }
        active = "";
        lines.clear();
    }

    public static void Deregister(String uTask, String uQue, String pid) {
        String thisProc = uTask + "\t" + uQue + "\t" + pid;
        String active = ReadDiskRecord(NamedCommon.BaseCamp + "/conf/running.jobs");
        ArrayList<String> lines = new ArrayList<String>(Arrays.asList(active.split("\n")));
        String data = "";
        for (int l=0 ; l<lines.size() ; l++) {
            if (lines.get(l).equals(thisProc)) {
                lines.remove(l);
             } else {
                data += lines.get(l) + "\n";
            }
        }
        WriteDiskRecord(NamedCommon.BaseCamp + "/conf/running.jobs", data);
        active = "";
        data = "";
        lines.clear();
    }

    public static ArrayList ArrayStringCopy(ArrayList<String> SourceList, ArrayList<String> TargetList) {
        int eol = SourceList.size();
        for (int i=0 ; i<eol ; i++) { TargetList.add(SourceList.get(i)); }
        return TargetList;
    }

    public static ArrayList ArrayBoolCopy(ArrayList<Boolean> SourceList, ArrayList<Boolean> TargetList) {
        int eol = SourceList.size();
        for (int i = 0; i < eol; i++) { TargetList.add(SourceList.get(i)); }
        return TargetList;
    }

    public static String jParse(String value) {

        if (value == null || value.equals("")) return value;

        boolean hasJsonJunk = false;
        String escChar;
        if (NamedCommon.jsonChars[0][0] == null) NamedCommon.ResetEscChars();
        int nbrChrs = NamedCommon.jsonChars.length;

        for (int c=0 ; c < nbrChrs ; c++) {
            escChar = NamedCommon.jsonChars[c][0];
            if (escChar.equals("")) continue;
            if (value.contains(escChar)) {
                hasJsonJunk = true;
                break;
            }
        }

        if (!hasJsonJunk) return value;

        char c = 0;
        int i;
        int len = value.length();
        StringBuilder sb = new StringBuilder(len + 4);
        String t;

        for (i = 0; i < len; i += 1) {
            c = value.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '"':
                    sb.append("\\\"");
                    break;
                case '/':
//                    sb.append("\\\\");
                    sb.append(c);
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    if (c < ' ') {
                        t = "000" + Integer.toHexString(c);
                        sb.append("\\u" + t.substring(t.length() - 4));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    public static void StopProcessNow() {
        System.out.println(" ");
        System.out.println("  ===============================================================");
        System.out.println("  Master stop switch turned ON. Stopping now.");
        System.out.println("  ===============================================================");
        System.out.println(" ");
        System.exit(0);
    }

    public static boolean TableExists(String rawTable) {
        boolean found = false;
        switch (NamedCommon.SqlDBJar) {
            case "MSSQL":
                found = uMSSQLCommons.Exists(rawTable);
                break;
            case "SNOWFLAKE":
                found = uSnowflakeCommons.Exists(rawTable);
                break;
            case "MYSQL":
                found = uMariaDBCommons.Exists(rawTable);
                break;
            case "ORACLE":
                found = uOracleCommons.Exists("raw_" + rawTable);
                break;
        }
        return found;
    }

    public static void WaitOnFiles() {
        String lookFor = "_(" + NamedCommon.pid + ")_";
        String datDir  = uCommons.insDir;
        if (NamedCommon.SmartMovers) datDir = uCommons.outDir;
        String lookin  = NamedCommon.BaseCamp + uCommons.uMQDir + datDir;
        String insDir  = "";
        uCommons.uSendMessage("   .) Check for files in " + lookin);
        uCommons.uSendMessage("   .) Looking for " + lookFor);
        String[] fetchFiles;
        File dir = null;
        File[] matchFiles = null;
        int nbrMatches=0;
        boolean isFinished = false;
        while (!isFinished) {
            matchFiles = null;
            dir = null;
            nbrMatches=0;
            if (NamedCommon.MultiMovers && NamedCommon.maxFdir > 1) {
                nbrMatches=0;
                for (int d=1 ; d<= NamedCommon.maxFdir ; d++) {
                    insDir = lookin + NamedCommon.slash + uCommons.RightHash("000" + String.valueOf(d), 3);
                    dir = new File(insDir);
                    matchFiles = dir.listFiles(new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                            return name.contains(lookFor);
                        }
                    });
                    nbrMatches += matchFiles.length;
                }
            } else {
                dir = new File(lookin);
                matchFiles = dir.listFiles(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        return name.contains(lookFor);
                    }
                });
                nbrMatches += matchFiles.length;
            }
            if (nbrMatches == 0) { isFinished=true; break; }
            uCommons.uSendMessage("   .) Waiting on " + nbrMatches + " files");
            uCommons.Sleep(5);
        }
        uCommons.uSendMessage("   .) Found no files in " + lookin);
        uCommons.uSendMessage("   .) Continuing ....");
        matchFiles = null;
        dir = null;
    }

    public static void CleanupSettings(String inDir, String inVal) {
        File dir = new File(inDir);
        File[] files = dir.listFiles((d, name) -> name.startsWith(inVal));
        if (files != null) for (File file : files) {file.delete();}
        dir = null;
        files = null;
    }
}
