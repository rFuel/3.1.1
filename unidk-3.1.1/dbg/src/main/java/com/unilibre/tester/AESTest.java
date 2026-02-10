package com.unilibre.tester.tester;

import com.unilibre.cipher.uCipher;
import com.unilibre.cipher.AESEncryption;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.UUID;

public class AESTest {

    public static void main(String[] args) {
        NamedCommon.ShowDateTime = true;
        ArrayList<String> secrets = new ArrayList<>();
        // read contents from file - pre-scrambled by rFuel
        // this is example ONLY ---------------------------
        secrets.add(0, "256");
        secrets.add(1, "65536");
        secrets.add(2, "UTF-8");
        String encString = "MR Quick Brown-Fox has a credit card 4641 3654 1289";
        for (int i=0 ; i < 10 ; i++) {
            encString += encString;
        }
        if (!encString.equals("")) {
            String secret = UUID.randomUUID().toString();
            String salt   = "31122004";
            String scrambled = uCipher.Encrypt(encString);
            String encryptedText = AESEncryption.encrypt(scrambled, secret, salt);
            String decryptedText  = AESEncryption.decrypt(encryptedText, secret, salt);
            String unscrambled = uCipher.Decrypt(decryptedText);
            DecimalFormat decfmt = new DecimalFormat("#,###.####");
            long startT = System.nanoTime();
            uCommons.uSendMessage("           Secret : " + secret + "  (" + secret.length() + " bytes)");
            uCommons.uSendMessage("             Salt : " + salt);
            uCommons.uSendMessage("    Original text : " + encString);
            uCommons.uSendMessage("  rFuel scrambled : " + scrambled);
            uCommons.uSendMessage("    AES encrypted : " + encryptedText);
            uCommons.uSendMessage("    AES decrypted : " + decryptedText);
            uCommons.uSendMessage("rFuel unscrambled : " + unscrambled);
            if (unscrambled.equals(encString)) uCommons.uSendMessage("PASS");
            long finishT = System.nanoTime();
            double div = 1000000000.00;
            double laps = (finishT - startT) / div;
            uCommons.uSendMessage("Scramble/Encrypt  : " + decfmt.format(laps) + " seconds ");
            System.exit(0);
        }
    }
}
