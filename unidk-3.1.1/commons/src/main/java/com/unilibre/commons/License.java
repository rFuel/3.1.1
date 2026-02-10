package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED  */

import com.unilibre.cipher.uCipher;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

public class License {

    private static final String lv = "LICENSE VIOLATION: ";
    private static final String em = "Email: support@unilibre.com.au";
    private static final String ld = NamedCommon.slash + "conf" + NamedCommon.slash + "licence";
    private static final String dbghost = NamedCommon.DevCentre;
    public static String domain = "", expiry="";
    private static String license = "";
    private static String whoami ="";
    private static ArrayList<Integer> range = new ArrayList<>();
    private static Properties props = new Properties();
    private static boolean isTesting = false;

    public static void SetTest(boolean inval) { isTesting = inval; }

    public static boolean IsValid() {

        if (props.size() > 0) {
            String pKey = "";
            for (Object key : props.keySet()) {
                pKey = (String) key;
                props.setProperty(pKey, "");
            }
        }

        boolean licPass = ReadLicence(NamedCommon.BaseCamp + ld);
        if (isTesting) System.out.println("Obtained   : "+licPass);
        String cmd;
        if (licPass) {
            range.clear();
            if (whoami.equals("")) { cmd = InitialiseRange(); whoami=cmd;} else { cmd = whoami; }
            licPass = GetDomain(cmd);
        } else {
            System.out.println("Can't find " + NamedCommon.BaseCamp + ld);
        }

        if (licPass) {
            uCipher.SetTest(isTesting);
            String licChk = uCipher.Decrypt(license);
            String LicDom = uCommons.FieldOf(licChk, "\\*", 1);
            if (isTesting) System.out.println("Licenced To: "+LicDom);
            if (isTesting) System.out.println("Running On : "+domain);
            if (domain.equals(LicDom) || LicDom.toLowerCase().contains("trial")) {
                String ExpDate = uCommons.FieldOf(licChk, "\\*", 2);
                if (ExpDate.equals("")) ExpDate = "0";
                licPass = CheckExpiryDate(ExpDate);
                String licType = uCommons.FieldOf(licChk + " * * * * ", "\\*", 4).trim();
                NamedCommon.licBulk = false;
                NamedCommon.licRest = false;
                NamedCommon.licNRT = false;
                if (licType.equals("")) licType="123";
                int lx = licType.length();
                String licChr="";
                for (int x=0; x < lx ; x++) {
                    licChr = licType.substring(x,(x+1));
                    switch (licChr) {
                        case "1":
                             NamedCommon.licBulk = true;
                            break;
                        case "2":
                             NamedCommon.licRest = true;
                            break;
                        case "3":
                             NamedCommon.licNRT = true;
                            break;
                    }
                }
                // Once Heritage, RAB & Kiwibank have upgraded licence keys, remove the following lines
                NamedCommon.licBulk = true;
                NamedCommon.licRest = true;
                NamedCommon.licNRT  = true;
            } else {
                licPass = false;
            }
            if (licChk.contains(NamedCommon.noLic)) licPass = true;
        }
        return licPass;
    }

    public static boolean CheckExpiryDate(String expDate) {
        String Today = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
        Today = Today.replaceAll("\\-", "");
        boolean licPass = false;
        int exp, now, days=0;
        exp = Integer.parseInt(expDate);
        now = Integer.parseInt(Today);
        if (exp > now) {
            licPass = true;
            NamedCommon.isValid = true;
            NamedCommon.licChecked = Today;
            // --------------------------------------------------------------------------------------------
            int yy1 = Integer.parseInt(expDate.substring(0,4));
            int mm1 = Integer.parseInt(expDate.substring(4,6));
            int dd1 = Integer.parseInt(expDate.substring(6,8));

            int yy2 = Integer.parseInt(Today.substring(0,4));
            int mm2 = Integer.parseInt(Today.substring(4,6));
            int dd2 = Integer.parseInt(Today.substring(6,8));

            int yydif = (yy1 - yy2) * 365;
            int mmdif = (mm1 - mm2) * 30;
            int dddif = (dd1 - dd2);

            days = yydif + mmdif + dddif;

            // --------------------------------------------------------------------------------------------
            if ((days) <= NamedCommon.licWarning) {
                String licMessage = "(" + NamedCommon.inputQueue + ") rFuel LICENCE EXPIRES in approximatetly "+ days + " days on " + domain;
                uCommons.uSendMessage("*************************************************");
                uCommons.uSendMessage(licMessage);
                uCommons.uSendMessage(em);
                uCommons.uSendMessage("*************************************************");
                if (!NamedCommon.Broker.equals("") && !NamedCommon.AlertQ.equals("")) {
                    String sendMessage = "{\"status\": \"402\",\"message\": \"rFuel Licence Expiry\",\"response\": \"" + licMessage + "\"}";;
                    Hop.start(sendMessage, "", uCommons.GetNextBkr(NamedCommon.Broker), NamedCommon.AlertQ, "", "LICENCE-EXPIRY");
                }
            }
        } else {
            licPass = false;
            uCommons.uSendMessage("**");
            uCommons.uSendMessage("**");
            uCommons.uSendMessage("**");
            uCommons.uSendMessage("*************************************************");
            uCommons.uSendMessage("Your rFuel license period has EXPIRED. ");
            uCommons.uSendMessage(em);
            uCommons.uSendMessage("*************************************************");
            uCommons.uSendMessage("**");
            uCommons.uSendMessage("**");
            uCommons.uSendMessage("**");
        }
        if (licPass) expiry = expDate;
        return licPass;
    }

    public static String Decypher(String encString, String license) {
        String order, original, licChk;
        int lx = license.length();
        range = new ArrayList<Integer>();
        for (int rr = 65; rr < 91; rr++) {
            range.add(rr);
        }
        for (int rr = 97; rr < 123; rr++) {
            range.add(rr);
        }
        for (int rr = 48; rr < 58; rr++) {
            range.add(rr);
        }
        range.add(46);
        range.add(95);
        range.add(42);
        range.add(45);
        range.add(32);

        order = "";
        int eor = range.size(), chx = 0;
        for (int rr = 0; rr < eor; rr++) {
            chx = range.get(rr);
            order += Character.toString((char) (int) chx);
        }
        original = order;
        licChk = "";
        String chr = "";
        int chk = 0, pos = 0;
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
        return licChk;
    }

    public static boolean GetDomain(String cmd) {
        boolean licPass = true;
        String s = "", error = "";
        domain = uCommons.GetMemory("domain");

        if (domain.equals("") && NamedCommon.isDocker) {
            domain = NamedCommon.hostname;          // from rFuel.properties
            if (domain.equals("")) {
                try {
                    Process p = Runtime.getRuntime().exec(cmd);
                    BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader es = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    while ((s = is.readLine()) != null) { domain += s; }
                    while ((s = es.readLine()) != null) { error += s; }
                } catch (IOException e) {
                    System.out.println("Cannot identify hostname: " + e.getMessage());
                    System.exit(1);
                }
            }
            uCommons.SetMemory("domain", domain);
        }

        if (domain.equals("")) {
            try {
                if (uCommons.GetMemory("os.name").toLowerCase().contains("windows")) {
                    if (System.getProperty("user.dir").toLowerCase().contains("/home/andy")) {
                        domain = dbghost;
                    } else {
                        try {
                            InetAddress addr = InetAddress.getLocalHost();
                            domain = addr.getHostName();
                        } catch (UnknownHostException ex) {
                            System.out.println("Hostname can not be resolved");
                        }
                    }
                    uCommons.SetMemory("domain", domain);
                } else {
                    Process p = Runtime.getRuntime().exec(cmd);
                    BufferedReader is = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader es = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    while ((s = is.readLine()) != null) { domain += s; }
                    while ((s = es.readLine()) != null) { error += s; }
                    uCommons.SetMemory("domain", domain);
                }
                if (!error.equals("")) uCommons.uSendMessage(error);
            } catch (IOException e) {
                System.out.println(" ");
                System.out.println("RunTime() Error in License Checker. " + em);
                System.out.println(" ");
                licPass = false;
            }
            uCommons.SetMemory("domain", domain);
        }

        return licPass;
    }

    public static String InitialiseRange() {
        range = new ArrayList<Integer>();
        String cmd = "";
        range.add(104);
        range.add(111);
        range.add(115);
        range.add(116);
        range.add(110);
        range.add(97);
        range.add(109);
        range.add(101);
        range.add(32);
        range.add(45);
        range.add(45);
        range.add(102);
        range.add(113);
        range.add(100);
        range.add(110);
        int eor = range.size(), chx = 0;
        for (int rr = 0; rr < eor; rr++) {
            chx = range.get(rr);
            cmd += Character.toString((char) (int) chx);
        }
        range.clear();
        return cmd;
    }

    private static boolean ReadLicence(String fname) {
        boolean licPass = true;
        InputStream ls = null;
        try {
            ls = new FileInputStream(fname);
        } catch (FileNotFoundException e) {
            System.out.println(" ");
            System.out.println(lv + "\nCannot find license \n" + em);
            System.out.println(" ");
            licPass = false;
        }
        if (ls != null) {
            try {
                props.load(ls);
                license = props.getProperty("lic", "ERROR");
                ls.close();
                ls = null;
            } catch (IOException e) {
                System.out.println(" ");
                System.out.println(lv + "Cannot load license " + em);
                System.out.println(" ");
                licPass = false;
            }
        } else {
            System.out.println(" ");
            System.out.println(lv + " " + em);
            System.out.println(" ");
            licPass = false;
        }
        return licPass;
    }
}
