package com.unilibre.core;

import com.unilibre.commons.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class coreCommons {

    private static String mscDir = NamedCommon.BaseCamp + "/mscatalog/", mscat = "", payload = "";
    private static String slowfile = NamedCommon.BaseCamp + NamedCommon.slash + "conf" + NamedCommon.slash + "SLOW";
    public static String uTask = "055", plf = "", esbFMT = "xml", retVal;
    private static boolean badMsg = false;

    private static void DecodeInstruction(String instr) {
        badMsg = false;

        String mTask = APImsg.APIget("TASK");
//        String ppty = APImsg.APIget("SHOST");
        esbFMT = APImsg.APIget("FORMAT");
        mscat = APImsg.APIget("MSCAT");
        plf = APImsg.APIget("PLFMT");
        payload = APImsg.APIget("PAYLOAD");

        payload = payload.replaceAll("\\\t", "");
        if (payload.contains("=")) payload = payload.replaceAll("\\=", "\t");
        if (payload.contains(" ")) payload = payload.replaceAll("\\ ", "\b");

        if (esbFMT.equals("")) esbFMT = NamedCommon.esbfmt;

        if ((mTask + mscat).contains("-X-")) {
            badMsg = true;
            return;
        }

        if (payload.indexOf("\t") > -1) payload = payload.replaceAll("\\\t", "=");
        if (payload.indexOf("\b") > -1) payload = payload.replaceAll("\\\b", " ");

        if (!mTask.equals(uTask)) {
            NamedCommon.Zmessage = "Wrong task [" + mTask + "] sent to " + uTask + " processor";
            NamedCommon.ZERROR = true;
            return;
        }
        if (mscat.equals("0")) {
            NamedCommon.Zmessage = ("Unknown MSCat in message ");
            NamedCommon.ZERROR = true;
            return;
        }

        switch (plf) {
            case "XML":
                if (!payload.substring(0, 3).contains("<")) {
                    NamedCommon.Zmessage = ("XML was expected as message payload.");
                    NamedCommon.ZERROR = true;
                    return;
                }
                break;
            case "LIXI":
                if (!payload.substring(0, 3).contains("<")) {
                    NamedCommon.Zmessage = ("LIXI was expected as message payload.");
                    NamedCommon.ZERROR = true;
                    return;
                }
                break;
            case "JSON":
                if (!payload.substring(0, 3).contains("{")) {
                    NamedCommon.Zmessage = ("JSON was expected as message payload.");
                    NamedCommon.ZERROR = true;
                    return;
                }
                break;
            default:
                if (payload.equals("") || payload.equals("\"\"")) {
                    if (payload.equals("\"\"")) payload = "";
                    plf = esbFMT;
                } else {
                    if (payload.substring(0, 3).contains("<")) {
                        plf = "XML";
                    } else {
                        if (payload.substring(0, 3).contains("{")) {
                            plf = "JSON";
                        } else {
                            NamedCommon.Zmessage = ("Unknown format of message payload.");
                            NamedCommon.ZERROR = true;
                            return;
                        }
                    }
                }
        }
    }

    public static String HandleMicroService(String msg) {
        NamedCommon.Zmessage = "";
        int status = 200;
        String response = "";
        String reply = "";
        DecodeInstruction(msg);
        if (!NamedCommon.ZERROR && !badMsg) {
            if (NamedCommon.debugging) uCommons.uSendMessage("-------------------------------------------------------------");
            String[] args = new String[10];
            for (int aa = 0; aa < 10; aa++) { args[aa] = ""; }
            args[0] = uTask + "2";
            args[4] = mscat;
            args[5] = payload;
            args[6] = APImsg.APIget("CORRELATIONID");
            args[7] = esbFMT;
            args[8] = mscDir;
            args[9] = plf;
            retVal = "";
            if (NamedCommon.threader) {
                ThreadManager.begin(args, null);
            } else {
                retVal = microservice.RESTput(args);
                if (NamedCommon.ZERROR) status = 500;
            }
        } else {
            status = 500;
            if (badMsg) status = 412;
        }
        reply = retVal;
        if (status != 200) reply = NamedCommon.Zmessage;

        if (!NamedCommon.sentU2) {
            String descr = NamedCommon.ReturnCodes.get(status);
            if (descr.equals("")) descr = "ReturnCode [" + status + "] not found.";
            response = DataConverter.ResponseHandler(String.valueOf(status), descr, reply, esbFMT);
            if (NamedCommon.debugging) uCommons.uSendMessage("uRESTmanager.finished(*)");
        } else {
            if (NamedCommon.isWebs) response = reply;
        }
        return response;
    }

    public static void SlowDown(String process) {
        boolean paused=false;
        String slowChk = ReadFromDisk(slowfile+process);
        slowChk = slowChk.replaceAll("\\r?\\n", "");
        int loopCnt=0;

        if (!slowChk.equals("")) {
            if (NamedCommon.debugging) {
                uCommons.uSendMessage("************* rFuel SLOW indicator is set ***********************");
                uCommons.uSendMessage("*** " + slowfile+process);
                uCommons.uSendMessage("*** ");
            }
            while(!slowChk.equals("")) {
                paused=true;
                uCommons.Sleep(2);
                loopCnt++;
                if (loopCnt > 60) {
                    // not waiting longer than 2 minutes
                    uCommons.DeleteFile(slowfile+process);
                    uCommons.Sleep(2);
                    loopCnt = 0;
                }
                slowChk = ReadFromDisk(slowfile+process);
            }
        }
        if (paused  && NamedCommon.debugging)  {
            uCommons.uSendMessage("*** ");
            uCommons.uSendMessage("*** restarting ...............");
            uCommons.uSendMessage("*****************************************************************");
        }
    }

    public static boolean StopNow() {
        if (NamedCommon.masterStop) return true;
        boolean stopyesno=false;
        String stopfile = NamedCommon.BaseCamp + NamedCommon.slash
                + "conf" + NamedCommon.slash + "STOP";

        String stopNow = ReadFromDisk(stopfile);
        if (stopNow.toLowerCase().contains("stop")) {
            System.out.println(" ");
            System.out.println(" ");
            uCommons.uSendMessage("Master STOP event.");
            System.out.println(" ");
            System.out.println(" ");
            uCommons.uSendMessage("<><><><><><><><><><><><><><><><><><><><><><><><><><>");
            uCommons.uSendMessage("<><><><><><><><> Shutdown in progress <><><><><><><>");
            uCommons.uSendMessage("<><><><><><><><><><><><><><><><><><><><><><><><><><>");
            uCommons.uSendMessage("Shutdown SourceDB:");
            NamedCommon.masterStop = true;
            SourceDB.DisconnectSourceDB();
            uCommons.uSendMessage("Shutdown MQ Connections:");
            mqCommons.CloseConnection();
            pmqCommons.CloseConnection();
            uCommons.uSendMessage("<><><><><><><><><><><><><><><><><><><><><><><><><><>");
            stopyesno=true;
        }
        stopfile= "";
        stopNow = "";
        return stopyesno;
    }

    public static String ReadFromDisk(String fqn) {
        BufferedReader BRin = null;
        FileReader fr = null;
        String rec = "";
        try {
            fr = new FileReader(fqn);
            BRin = new BufferedReader(fr);
            String line = null;
            try {
                line = BRin.readLine();
            } catch (IOException e) {
                return rec;
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    return rec;
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                uCommons.uSendMessage("File Close FAIL on " + fqn);
                uCommons.uSendMessage(e.getMessage());
            }
        } catch (IOException e) {
            fr = null;
            BRin = null;
        }
        return rec;
    }

    public static ArrayList StringToList(String inStr, String splitChar) {
        // Converts a string into an ArrayList, splitting on a specified char

        inStr = inStr.replaceAll("\\r", "");
        String splitOn = splitChar;
        if (splitOn.equals("")) splitOn = "\n";
        char charOn = splitOn.charAt(0);

        ArrayList<String> answer = new ArrayList<>();
        String newLine;
        int len, index;
        char c;
        StringBuilder sbVar = new StringBuilder();

        while (inStr.length() > 0) {
            len = inStr.length();
            index = 0;
            newLine = "";
            sbVar.delete(0, sbVar.length());
            while (index < len && inStr.charAt(index) != charOn) {
                c = inStr.charAt(index);
                sbVar.append(c);
                newLine = sbVar.toString();
                index++;
            }
            newLine = sbVar.toString();
            answer.add(newLine);
            if ((index+1) < len) {
                inStr = inStr.substring((index + 1), len);
            } else {
                inStr = "";
            }
        }
        return answer;
    }

    public static String UUIDv7Generator() {
        long timestamp = Instant.now().toEpochMilli();          // 48-bit timestamp
        long randomA = ThreadLocalRandom.current().nextLong();  // 64 bits
        int randomB = ThreadLocalRandom.current().nextInt();    // 32 bits

        // Embed the timestamp into the high bits.
        // shift the time left by 16 bits
        long msb = (timestamp << 16) | 0x7000;                  // v7 marker (0x7)

        // shift the random bits left by 32 bits
        long lsb = ((long) randomB << 32) | (randomA & 0xFFFFFFFFL);

        // In Java (and most languages), the | symbol is the bitwise OR operator.
        // It compares each bit of two numbers and returns a new number where each
        // bit is set to 1 if either of the original bits was 1.
        // eg:
        // a = 0b0101;  // 5
        // b = 0b0011;  // 3
        // c = a | b;   // 0b0111 → 7

        return new java.util.UUID(msb, lsb).toString();

    }
}
