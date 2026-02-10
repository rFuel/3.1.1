package com.unilibre.dataworks;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

/* The umask must be set to 000 in /etc/login.defs   */
/* Also in the configuration of supervisord (000)    */

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import com.unilibre.core.coreCommons;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class MoveData {

    public static ArrayList<String> BadFiles = new ArrayList<>();
    public static ArrayList<String> DeleteFiles = new ArrayList<>();
    public static List<String> DDL;
    public static String[] ListOfFiles;
    public static File datfiles;
    public static final String kinit = "kinit";
    public static final String klist = "klist";
    public static String MyTask;
    public static String fLocn;
    public static String eLocn;
    public static String baseFle;
    public static String remote = "";
    public static String datPath = "";
    public static String sqlHost = "";
    public static String sqlDir = "";
    public static String scp = "/usr/bin/scp $ ";
    public static String ssh = "/usr/bin/ssh ";
    public static String ddir = "";
    public static String matchStr = "dat";
    public static String External = "/data/external/";
    public static String tSchema = "";
    public static String config = "";
    public static String watchDir = "";
    public static boolean ok2delete = true, busy = false;
    public static boolean isRaw = false;
    public static boolean heartbeat = false;
    public static int failCnt = 0;
    private static long lastEvent = 0, now = 0;
    private static double laps, div = 1000000000.00;
    private static int wMax = 2000;
    private static String krbExpDate = "";
    private static String krbExpTime = "";
    private static String krbUser = "";
    private static String krbPass = "";
    private static boolean krbCheck = NamedCommon.krbDefault;

    public static void main (String[] args) throws Exception {
        //
        // Since 2.0 - this is only used for BULK INSERT of data.
        //
        // -------------------------------------------------------------------------
        // fLocn                : where to find the dat files on rFuel (local)
        // NamedCommon.datPath  : where SQL can bulk insert from       (SQl  )
        // -- these are completely different ---------------------------------------
        String slash = "";
        if (NamedCommon.upl.equals("")) NamedCommon.upl = System.getProperty("user.dir");
        tSchema = System.getProperty("schema", "");
        matchStr= System.getProperty("ext", matchStr);
        watchDir= System.getProperty("insdir", "");

        if (tSchema.equals("raw")) isRaw = true;
        if (NamedCommon.upl.contains("/")) slash = "/";
        if (NamedCommon.upl.contains("\\")) slash = "\\";
        if (NamedCommon.upl.contains("/home/andy")) {
            String old = NamedCommon.BaseCamp;
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            String knw = NamedCommon.BaseCamp;
            uCommons.uSendMessage("Resetting BaseCamp from " + old + " to " + knw);
            slash = "/";
            NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
        }
        NamedCommon.slash = slash;
        config = NamedCommon.BaseCamp + slash + "conf";
        MyTask = System.getProperty("task", "");
        System.out.println();
        System.out.println();
        System.out.println();
        NamedCommon.Reset();
        uCipher.SetAES(false, "", "");
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);
        krbCheck = NamedCommon.krbDefault;

        NamedCommon.debugging = System.getProperty("debug", "false").toLowerCase().equals("true");
        if (krbCheck) {
            krbUser = uCommons.GetValue(runProps, "jdbcUsr", "");
            krbPass = uCommons.GetValue(runProps, "jdbcPwd", "");
            if (krbUser.contains("\\")) {
                String[] usrParts = krbUser.split("\\\\");
                krbUser = usrParts[usrParts.length - 1];
                usrParts = null;
            }
        }
        String title = "", look4 = "";
        GarbageCollector.setStart(System.nanoTime());
        fLocn = NamedCommon.BaseCamp + "/data/ins/";
        if (NamedCommon.MultiMovers) {
            System.out.println();
            if (watchDir.equals("")) {
                NamedCommon.ZERROR = true;
                uCommons.uSendMessage("***");
                uCommons.uSendMessage("*** Watch directory MUST be set in shell script.");
                uCommons.uSendMessage("***");
                System.exit(1);
            }
            String dirPart = uCommons.RightHash("000"+watchDir, 3) + NamedCommon.slash;
            fLocn += dirPart;
            NamedCommon.datPath += dirPart;
        } else {
            uCommons.uSendMessage("Single data pickup directorty");
        }
        datPath = fLocn;
        matchStr = ".dat";
        if (!NamedCommon.BulkLoad) matchStr = ".sql";
        title = "MoveData()";
        look4 = matchStr + " files";

        uCommons.uSendMessage(" ");
        uCommons.uSendMessage("StartUp " + title);
        if (isRaw) {
            uCommons.uSendMessage("Searching for [raw] schema ONLY");
        } else {
            uCommons.uSendMessage("Searching for non [raw] schema dat files");
        }
        uCommons.uSendMessage("=================================================");
        initialise();
        if (!NamedCommon.jdbcAdd.equals("") && krbExpDate.equals("") && krbCheck) {
            SetKrbControls();
            CheckKrbTicket();
        }

        if (NamedCommon.SmartMovers) {
            InsertCollecter.Prepare();
            RunControl.SetVolumes(false);
            RunControl.SetMaxFiles(100);
            krbCheck = false;
        } else {
            if (!NamedCommon.tConnected) SqlCommands.ConnectSQL();
            if (!NamedCommon.tConnected) return;
        }

        boolean DBokay = NamedCommon.SmartMovers;
        if (!DBokay) {
            if (SourceDB.ConnectSourceDB().contains("<<PASS>>")) {
                SourceDB.DisconnectSourceDB();
                DBokay = true;
            }
        }
        if (DBokay) {
            if (!NamedCommon.tConnected && !NamedCommon.SmartMovers) {
                SqlCommands.ConnectSQL();
                if (!NamedCommon.tConnected) return;
                if (!NamedCommon.jdbcAdd.equals("") && krbExpDate.equals("") && krbCheck) {
                    SetKrbControls();
                    CheckKrbTicket();
                }
            }
            uCommons.uSendMessage("Look for " + look4 + " in " + ddir);
            String hStr=matchStr, HBmsg="[heartbeat] waiting for data .... ";
            int waiting = 0, ticketTime = 0;
            boolean stopFlg = false;
            while (!stopFlg) {
                CheckStopNow();
                if (RunControl.ThingsToDo()) {
                    if (NamedCommon.SmartMovers) {
                        InsertCollecter.handleFiles(ListOfFiles);
                    } else {
                        APImsg.instantiate();
                        waiting = 0;
                        if (!busy) waiting = wMax - 5;
                        busy = true;
                        RunControl.MoveFiles();
                    }
                    if (NamedCommon.ZERROR) stopFlg = true;
                    lastEvent = System.nanoTime();
                } else {
                    waiting++;
                    if (waiting > 50) if (krbCheck) CheckKrbTicket();
                    if (waiting > wMax && !heartbeat) heartbeat = true;
                    if (heartbeat) {
                        now = System.nanoTime();
                        laps = (now - lastEvent) / div;
                        if (laps > wMax) {
                            if (krbCheck) {
                                ticketTime = TicketExpires();
                                uCommons.uSendMessage(HBmsg + " ticket expires in " + ticketTime + " seconds");
                            } else {
                                uCommons.uSendMessage(HBmsg);
                            }
                            lastEvent = now;
                        }
                        heartbeat = false;
                        busy = false;
                        waiting = 0;
                        uCommons.Sleep(1);
                        GarbageCollector.CleanUp();
                        if (krbCheck) CheckKrbTicket();
                    }
                    CheckStopNow();
                }
                if (failCnt > 10) {
                    uCommons.uSendMessage("Too many FAIL attempts : <<rfuel>> Re-initialise run");
                    initialise();
                }
            }
        } else {
            if (heartbeat) {
                uCommons.uSendMessage("Pausing ... SourceDB is unavailable");
                heartbeat = false;
            }
            uCommons.Sleep(10);
        }
        if (!NamedCommon.SmartMovers) SqlCommands.DisconnectSQL();
    }

    private static void CheckStopNow() {
        boolean stopNow = coreCommons.StopNow();
        if (stopNow) {
            System.out.println(" ");
            while (stopNow) {
                uCommons.uSendMessage("<<heartbeat>> Waiting for the pid to die.");
                uCommons.Sleep(3);
                System.exit(1);
            }
        }
    }

    private static void CheckKrbTicket() {
        if (!krbCheck) return;
        if (krbUser.equals("")) return;
        if (krbPass.equals("")) return;
        if (krbExpDate.equals("")) return;
        if (krbExpTime.equals("")) return;
        // -------------------------------------------------------------------------
        // Warning: Kerberos shows the date in US format of YMD
        //        : I allow rFuel to show the date in AUS format D-M-Y
        //        : krbExpDate is held as a number - iconv'ed
        // -------------------------------------------------------------------------
        boolean loopSW = true, renewed = false;
        while (loopSW) {
            int timeRemaining = TicketExpires();
            if (timeRemaining < 600) {
                // Auto apply for a new kerberos ticket.
                if (timeRemaining < 0) {
                    uCommons.uSendMessage("****** Ticket has expired. *******");
                } else {
                    uCommons.uSendMessage("Current ticket expires: " + uCommons.oconvD(krbExpDate, "D4-") + "   " + uCommons.oconvM(krbExpTime, "MTS"));
                    uCommons.uSendMessage("                      : " + timeRemaining + " seconds from now.");
                }
                // use a process builder to write the password into the active command
                // this way, the password is never shared anywhere!
                renewed = true;
                try {
                    ProcessBuilder pb = new ProcessBuilder(kinit, krbUser);
                    Process process = pb.start();
                    try (OutputStream os = process.getOutputStream()) {
                        os.write((krbPass + "\n").getBytes());
                        os.flush();
                        os.close();
                        uCommons.Sleep(0);
                    }

                    // Read the output and error streams

                    try (
                            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                            BufferedReader errors = new BufferedReader(new InputStreamReader(process.getErrorStream()))
                    )
                    {
                        String line;
                        String pfx = "   ) ";
                        while ((line = reader.readLine()) != null) { uCommons.uSendMessage(pfx + line); }
                        while ((line = errors.readLine()) != null) { uCommons.uSendMessage(pfx + line); renewed = false; }
                        reader.close();
                        errors.close();
                    }
                    process.waitFor();
                } catch (IOException | InterruptedException | IllegalStateException e) {
                    uCommons.uSendMessage("Error: " + e.getMessage());
                    renewed = false;
                }
                if (renewed) {
                    SetKrbControls();
                    uCommons.uSendMessage("    New ticket expires: " + uCommons.oconvD(krbExpDate, "D4-") + "   " + uCommons.oconvM(krbExpTime, "MTS"));
                    uCommons.Sleep(2);
                } else {
                    uCommons.uSendMessage("CANNOT continue without a Kerberos ticket. Stopping now.");
                    uCommons.Execute("rfuel -stop");
                    System.exit(1);
                }
                // --------------------------------------------------------------------------------------------------
            } else {
                loopSW = false;
            }
        }
        if (renewed) {
            uCommons.uSendMessage("----------------------------------------------");
            uCommons.uSendMessage("**** Auto created a new Kerberos ticket. *****");
            uCommons.uSendMessage("----------------------------------------------");
        }
    }

    private static int TicketExpires() {
        int timeRemaining=0;
        String Today = uCommons.GetToday();
        String time  = uCommons.GetTime();
        int diffD = Integer.valueOf(krbExpDate) - Integer.valueOf(Today);
        diffD = diffD * 86400;
        int diffT = Integer.valueOf(krbExpTime) - Integer.valueOf(time);
        timeRemaining = diffD + diffT;
        return timeRemaining;
    }

    public static void initialise() {

        BadFiles.clear();
        eLocn = "fails/";
        NamedCommon.tHostList = new ArrayList<>();

        DDL = new ArrayList<>();
        if (!sqlHost.equals("")) remote = sqlHost + ":" + sqlDir;

        ddir    = fLocn;
        datPath = fLocn;
        datfiles = new File(fLocn);
        failCnt = 0;
    }

    public static void SetKrbControls() {
        if (!krbCheck) return;
        //
        // Auto test for kerberos ticket expiry
        //
        uCommons.uSendMessage("Loading Kerberos ticket details (" + klist + ")");
        String cmd = klist;
        String jReply = uCommons.nixExecCmd(cmd, 999999);
        String[] jLines = jReply.split("\\r?\\n");
        String jLine, dd = "", tt = "";
        int expPos = 0;
        boolean thisGrp = false;
        for (int i = 0; i < jLines.length; i++) {
            jLine = jLines[i];
            if (jLine.contains(krbUser)) thisGrp = true;
            if (!thisGrp) continue;
            if (expPos > 0) {
                String[] lParts = jLine.substring(expPos, (expPos + 20)).split("\\ ");
                dd = lParts[0];
                tt = lParts[1];
                break;
            }
            if (jLine.contains("Expires")) expPos = jLine.indexOf("Expires");  // the next line is required
        }
        // assumption is that Kerberos ALWAYS returns the date as yyyy/mm/dd
        if (!dd.contains("-")) {
            dd = dd.replace("/", "-");
            dd = dd.replace(" ", "-");
        }
        String[] baseDD = dd.split("\\-");
        if (baseDD.length == 3) {
            dd = baseDD[2] + "-" + baseDD[0] + "-" + baseDD[1];
            dd = String.valueOf(uCommons.iconvD(dd, "D4-MDY"));
            tt = String.valueOf(uCommons.iconvD(tt, "MTS"));
            krbExpDate = dd;
            krbExpTime = tt;
            krbCheck = true;
            uCommons.uSendMessage("Done.");
        } else {
            uCommons.uSendMessage("Cannot understand Kerberos expiry date: [" + dd + "]");
            krbCheck = false;
        }
    }

    public static void SetKRBdetails(String user, String password, boolean krbSW) {
        krbUser = user;
        krbPass = password;
        krbCheck= krbSW;
    }

}
