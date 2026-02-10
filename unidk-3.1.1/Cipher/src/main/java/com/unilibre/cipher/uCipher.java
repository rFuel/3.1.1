package com.unilibre.cipher;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import java.util.ArrayList;
import java.util.Random;

public class uCipher {

    public static String keyBoard25 = "!|#$%&()*+,-.0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]{}^_abcdefghijklmnopqrstuvwxyz";
    public static String keyBoard18 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._*- ";
    public static String keyBoard = keyBoard25;
    public static String domain = "";
    private static String upl = "unilibreptyltdaustralia";
    private static String encSeed = "";
    private static String alts = "`@#$%^&*().,/<>?;:-_=+|][}{", tilde="~";
    private static String characterEncoding       = "UTF-8";
    private static String aesEncryptionAlgorithem = "AES";
    private static String cipherTransformation    = "AES/CBC/PKCS5PADDING";

    public static boolean isLic=true;
    private static boolean AES = false;
    private static boolean isTesting = false;
    private static String secret, salt;

    public static void SetAES(boolean val, String se, String sa) {
        AES = val;
        secret = se;
        salt = sa;
    }

    public static boolean GetAES() { return AES; }

    public static String Encrypt(String inStr, String eSeed) {
        encSeed = eSeed;
        String ans = Encrypt(inStr).split(tilde)[0];
        encSeed = "";
        return ans;
    }

    public static String Encrypt(String inStr) {
        String encrypted = "", chr, repl = "";
        if (AES) {
            encrypted = AESEncryption.encrypt(inStr, secret, salt);
        } else {
            if (inStr.contains(tilde)) {
                int flg = 0;
                for (int idx = 1; idx <= alts.length(); idx++) {
                    repl = alts.substring((idx - 1), idx);
                    if (!inStr.contains(repl)) {
                        flg = idx;
                        break;
                    }
                }
                if (flg == 0) {
                    repl = "\t";
                    while (inStr.contains(tilde)) {
                        inStr = inStr.replaceAll(tilde, repl);
                    }
                    inStr = inStr + tilde + tilde + repl;
                    return inStr; // cannot encrypt this string
                }
                while (inStr.contains(tilde)) {
                    inStr = inStr.replace(tilde, repl);
                }
            }
            Random rdm = new Random();
            int ri = 0;
            String order = "", original = "";
            order = keyBoard;
            original = order;
            int lx = order.length();
            if (encSeed.equals("")) {
                // randomise keyboard into an encSeed
                while (order.length() > 0) {
                    ri = rdm.nextInt(lx);
                    chr = order.substring(ri, (ri + 1));
                    encSeed += chr;
                    order = order.substring(0, ri) + order.substring(ri + 1, lx);
                    lx = order.length();
                }
            }
            String encr = "";
            int pos = 0;
            lx = inStr.length();
            // Scarmbler: positionally replace raw chars with encSeed chars
            for (int x = 0; x < lx; x++) {
                chr = inStr.substring(x, (x + 1));
                pos = original.indexOf(chr);
                if (pos < 0) {
                    encr += chr;
                } else {
                    encr += encSeed.substring(pos, pos + 1);
                }
            }

            int chk = 0;
            for (int x = lx; x > 0; x--) {
                chr = encr.substring(x - 1, x);
                if (chk == 4 && isLic) {
                    chk = 0;
                    encrypted = "," + encrypted;
                }
                encrypted = chr + encrypted;
                chk++;
            }
            encrypted = encrypted + "~" + encSeed + "~" + repl;
            encSeed = "";
        }
        return encrypted;
    }

    public static String Decrypt(String inStr) {
        if (inStr.contains("ENC(")) {
            while (!inStr.startsWith("ENC(")) {
                // get rid of spaces or crap at the start
                inStr = inStr.substring(1, inStr.length());
            }
            inStr = inStr.substring(4, inStr.length());
            while (!inStr.endsWith(")")) {
                // get rid of spaces or n-ines or other crap on the end
                inStr = inStr.substring(0, inStr.length()-1);
            }
            if (inStr.endsWith(")")) inStr = inStr.substring(0, inStr.length()-1);
        }
        if (isTesting) System.out.println("Decrypting : "+inStr);

        String decrypted = "";

        if (AES) {
            AESEncryption.SetTest(isTesting);
            decrypted = AESEncryption.decrypt(inStr, secret, salt);
        } else {
            String repl = FieldOf(inStr, tilde, 3);
            String encSeed = FieldOf(inStr, tilde, 2);
            inStr = FieldOf(inStr, tilde, 1);
            if (repl.equals("\t")) {
                if (!inStr.equals(repl)) {
                    while (inStr.contains(repl)) {
                        inStr = inStr.replace(repl, tilde);
                    }
                }
                return inStr;
            }

            if (encSeed.equals("")) return inStr;
            if (repl.equals("!")) return inStr;

            String original = "";
            original = keyBoard;
            int lx = inStr.length(), lo = original.length();
            String chr = "", nChr;
            int chk = 0, pos = 0;
            for (int x = lx; x > 0; x--) {
                chr = inStr.substring(x - 1, x);
                if (chr.equals(",") && (chk == 4) && isLic) {
                    chk = 0;
                    continue;
                }
                pos = encSeed.indexOf(chr);
                if (pos < 0 || pos > lo) {
                    nChr = chr;
                } else {
                    nChr = original.substring(pos, pos + 1);
                }
                decrypted = nChr + decrypted;
                chk++;
            }

            if (!repl.equals("")) {
                while (decrypted.contains(repl)) {
                    decrypted = decrypted.replace(repl, tilde);
                }
            }
        }
        return decrypted;
    }

    public static String SetBase() {
        // -------------------------------------------------------------------
        // This creates a variable who's value is ALL keyboard characters.
        // It is more advanced than the keyboard25 variable.
        // -------------------------------------------------------------------
        ArrayList<Integer> tempRange = new ArrayList<Integer>();
        String order = "";
        for (int rr = 32; rr < 126; rr++) { tempRange.add(rr); }

        int eor = tempRange.size(), chx = 0;
        for (int rr = 0; rr < eor; rr++) {
            chx = tempRange.get(rr);
            order += Character.toString((char) (int) chx);
        }
        return order;
    }

    private static String FieldOf(String inStr, String findme, int occ) {
        String ans;         // = new String();
        String[] tmpStr;    // = new String[]{};
        tmpStr = inStr.split(findme);
        if (occ <= tmpStr.length) {
            ans = tmpStr[occ - 1];
        } else {
            ans = "";
        }
        return ans;
    }

    public static String main(String inStr, String action) {
        String ans = inStr;
        int trigger = 0;
        if (action.equals("E")) trigger = 1;
        if (action.equals("D")) trigger = 2;
        switch (trigger) {
            case 1:
                ans = Encrypt(inStr);
            case 2:
                ans = Decrypt(inStr);
        }
        return ans;
    }

    public static String v2Scramble(String keyboard, String rawData, String encSeed) {
        String answer = "", chr, nChr;
        if (AES) {
            answer = AESEncryption.encrypt(rawData, secret, salt);
        } else {
            ArrayList<String> rawArr = new ArrayList<>();
            ArrayList<String> encArr = new ArrayList<>();
            ArrayList<Integer> encPos = new ArrayList<>();
            int lx = rawData.length(), cPos;
            for (int c = 1; c <= lx; c++) {
                chr = rawData.substring(c - 1, c);
                rawArr.add(chr);
                encArr.add(chr);
                encPos.add(c - 1);
            }

            int rPos, ePos;
            lx = rawArr.size();
            int am = encSeed.length();
            int cc = 0;
            while (rawArr.size() > 0) {
                chr = rawArr.get(cc);
                if (chr.length() > 1) {
                    rawArr.remove(0);
                    encPos.remove(0);
                    continue;
                }
                if (!keyboard.contains(chr)) {
                    rawArr.remove(0);
                    encPos.remove(0);
                    continue;
                }
                cPos = keyboard.indexOf(chr);
                try {
                    nChr = encSeed.substring(cPos, cPos + 1);
                } catch (StringIndexOutOfBoundsException e) {
                    nChr = chr;
                    System.out.println(e.getMessage());
                    System.exit(1);
                }
                rPos = cc;
                while (rPos > -1) {
                    ePos = encPos.get(rPos);
                    encArr.set(ePos, nChr);
                    rawArr.remove(rPos);
                    encPos.remove(rPos);
                    rPos = rawArr.indexOf(chr);
                }
            }
            StringBuilder ans = new StringBuilder();
            for (int c = 0; c < lx; c++) {
                ans.append(encArr.get(c));
            }
            answer = ans.toString();
            rawArr.clear();
            encArr.clear();
            encPos.clear();
        }
        return answer;
    }

    public static String v2UnScramble(String keyBoard, String encrypt, String encSeed) {
        String answer, chr, nChr, done="";
        if (AES) {
            answer = AESEncryption.decrypt(encrypt, secret, salt);
        } else {
            if (encSeed.equals("")) return encrypt;
            ArrayList<String> rawArr = new ArrayList<>();
            ArrayList<String> encArr = new ArrayList<>();
            int lx = encrypt.length(), cPos;
            for (int c = 1; c <= lx; c++) {
                chr = encrypt.substring(c - 1, c);
                encArr.add(chr);
                rawArr.add(chr);
            }

            String andy="";
            int rPos, ePos;
            lx = rawArr.size();
            for (int c = 0; c < lx; c++) {
                chr = encArr.get(c);
                if (done.contains(chr) || chr.length() > 1) continue;
                if (keyBoard.contains(chr)) {
                    cPos = encSeed.indexOf(chr);
                    nChr = keyBoard.substring(cPos, cPos + 1);
                    ePos = encArr.indexOf(chr);
                    while (ePos > -1) {
                        andy = encArr.get(ePos);
                        encArr.set(ePos, "!" + chr);
                        rawArr.set(ePos, nChr);
                        andy = encArr.get(ePos);
                        andy = "";
                        ePos = encArr.indexOf(chr);
                    }
                }
                done += chr;
            }
            StringBuilder ans = new StringBuilder();
            for (int c = 0; c < lx; c++) {
                ans.append(rawArr.get(c));
            }
            answer = ans.toString();
        }
        return answer;

    }

    public static String GetEncKey() {
        return (domain + upl).substring(0,16);
    }

    public static String GetEncSeed() { return encSeed; }

    public static String GetCipherKey() {
        Random rdm = new Random();
        int ri = 0;
        String order = "", chr = "", encSeed = "";
        order = uCipher.keyBoard;
        int lx = order.length();
        // randomise keyboard into an encSeed
        while (order.length() > 0) {
            ri = rdm.nextInt(lx);
            chr = order.substring(ri, (ri + 1));
            encSeed += chr;
            order = order.substring(0, ri) + order.substring(ri + 1, lx);
            lx = order.length();
        }
        return encSeed;
    }

    public static String Randomise(String original) {
        int lx = original.length();
        int ri = 0;
        Random rdm = new Random();
        String encSeed="", chr;
        while (original.length() > 0) {
            ri = rdm.nextInt(lx);
            chr = original.substring(ri, (ri + 1));
            encSeed += chr;
            original = original.substring(0, ri) + original.substring(ri + 1, lx);
            lx = original.length();
        }
        return encSeed;
    }

    public static void SetTest(boolean inval) {
        isTesting = inval;
    }
}
