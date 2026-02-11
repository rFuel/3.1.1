package com.unilibre.tester;

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class EncryptBP {

    public static void main(String[] args) {
        System.out.println(" ");
        System.out.println(" ");
        System.out.println("Obfuscating PICK Programs --------------------------------------------");

        BufferedWriter bWriter = null;
        Scanner scanner;

        String[] encPgms = null;
        String extn;
        String rfVersion = "3.1.1";
        String obfusVer = rfVersion.replaceAll("\\.", "");
        String bpraw, bpFile;
        String[] pFiles = new String[3];
        pFiles[0] = "BP.UPL";
        pFiles[1] = "BP.KIWI";
        pFiles[2] = "";
//        System.out.println(" ");
        for (int bp=0 ; bp < 3 ; bp++) {
            if (pFiles[bp].equals("")) continue;
            System.out.println("Obfuscating "+pFiles[bp]);
        }
//        System.out.println(" ");
        uCipher.SetAES(false, "", "");

        for (int bp=0 ; bp < 3 ; bp++) {
            bpFile = pFiles[bp];
            if (bpFile.isEmpty()) continue;
//            System.out.println(" ");
//            System.out.println(" ");
            System.out.println("-----------------------------------------------------------------------");
            String bpPath = "/home/andy/rFuel/DEV/";
            String deploy = "/home/andy/rFuel/Deploy/";
            bpraw = bpPath +  rfVersion + "/dbProgs/" + bpFile;
            String bpenc = "";
            bpenc = deploy + rfVersion + "/lib";
            String[] pgms;

            System.out.println("Read  from : " + bpraw);
            System.out.println("Encrypt to : " + bpenc);

            List<String> changes = new ArrayList<>();

            // ----------- Get last update date and make LONG -----------------------------------------

            long lastEncryptionRun;
            String rec = uCommons.ReadDiskRecord(bpPath + rfVersion + "/dbProgs/lastEncrypt.txt");
            if (rec.isEmpty()) rec = "01-01-2025";
            rec = rec.replaceAll("\\r?\\n", "");
            System.out.println("Last encryption: "+rec+"  *-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-");
            scanner = new Scanner(System.in);
            System.out.print("Continue (Y/N) : ");
            String myans =  scanner.nextLine().trim().toUpperCase();
            if (!myans.equalsIgnoreCase("y")) System.exit(0);

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

            scanner = new Scanner(System.in);
            System.out.print("Copy to s3://core/rfuel/3.1.1/staging (Y/N) ");
            myans =  scanner.nextLine().trim().toUpperCase();

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
                    System.out.println("    -type f -newer "+bpPath+"/dbProgs/last_encrypt_marker \\");
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

    private static void EncryptProgs(BufferedWriter bWriter, String[] pgms, String bpraw, String bpenc) {
        if (!bpraw.contains("BP.UPL")) return;
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
}
