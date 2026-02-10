package com.unilibre.cipher;



import javax.crypto.*;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.Arrays;
import java.util.Base64;

public class AESEncryption {

    private static int KEY_LENGTH = 256;
    private static int ITERATION_COUNT = 65536;
    private static boolean isTesting = false;
    private static final String CHAR_SET= "UTF-8";
    private static final String TXFM = "AES/GCM/NoPadding";
    private static final SecureRandom secureRandom = new SecureRandom();
    private static SecretKeySpec secretKeySpec = null;

    public static String encrypt(String strToEncrypt, String secretKey, String salt) {
        strToEncrypt = strToEncrypt.replaceAll("\\r?\\n", "");
        //
        //  This can suffer with "entropy depletion" - when it can't find suitable random-ness
        //  Due to massive CPU usage:
        //
        byte[] IV = new byte[12];

        secureRandom.nextBytes(IV);
//        SecureRandom secureRandom = null;
//        try {
// //            secureRandom = SecureRandom.getInstance("SHA1PRNG");
//            secureRandom = SecureRandom.getInstanceStrong();
//            secureRandom.setSeed(System.currentTimeMillis()); // Optional: Prevent blocking
//            secureRandom.nextBytes(IV);
//        } catch (NoSuchAlgorithmException e) {
//            ShowError(e.getMessage());
//            return strToEncrypt;
//        }
// //        new SecureRandom().nextBytes(IV);

        try {
            GCMParameterSpec gcmSpec = new GCMParameterSpec(128, IV);
//            if (secretKeySpec == null) deriveKey(secretKey, salt);
            if (secretKeySpec == null) secretKeySpec = deriveKey(secretKey, salt);
            if (secretKeySpec == null) return strToEncrypt;
//            if (cipher == null) cipher = Cipher.getInstance(TXFM);
            Cipher cipher = Cipher.getInstance(TXFM);
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, gcmSpec);
            byte[] cipherText = cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8));
            byte[] encryptedData = new byte[IV.length + cipherText.length];
            System.arraycopy(IV, 0, encryptedData, 0, IV.length);
            System.arraycopy(cipherText, 0, encryptedData, IV.length, cipherText.length);

            return Base64.getEncoder().encodeToString(encryptedData);
        } catch(InvalidAlgorithmParameterException ie) {
            ShowError(ie.getMessage());
            return strToEncrypt;
        } catch (Exception e) {
            ShowError(e.getMessage());
            return strToEncrypt;
        }
    }

    private static SecretKeySpec deriveKey(String secretKey, String salt) {
        if (secretKeySpec != null) return secretKeySpec;
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            if (isTesting) System.out.println("           : factory   has been set");
            KeySpec spec = new PBEKeySpec(secretKey.toCharArray(), salt.getBytes(StandardCharsets.UTF_8), ITERATION_COUNT, KEY_LENGTH);
            if (isTesting) System.out.println("           : KeySpec   has been set");
            SecretKey tmp = factory.generateSecret(spec);
            if (isTesting) System.out.println("           : SecretKey has been generated");
            secretKeySpec = new SecretKeySpec(tmp.getEncoded(), "AES");
            if (isTesting) System.out.println("           : secretKeySpec has been set");
        } catch (NoSuchAlgorithmException e) {
            ShowError(e.getMessage());
        } catch (InvalidKeySpecException e) {
            ShowError(e.getMessage());
        }
        return secretKeySpec;
    }

    public static String decrypt(String strToDecrypt, String secretKey, String salt) {
        strToDecrypt = strToDecrypt.replaceAll("\\r?\\n", "");

        try {
            byte[] encryptedData = Base64.getDecoder().decode(strToDecrypt);

            if (isTesting) System.out.println("encData    : byte[" + encryptedData.length + "]");

            // 🔒 Sanity check before extracting IV
            if (encryptedData.length < 12) {
                ShowError("Encrypted data too short to contain IV");
                return strToDecrypt;
            }
            // Extract IV correctly
            byte[] IV = Arrays.copyOfRange(encryptedData, 0, 12);
            if (isTesting) System.out.println("IV length  : byte[" + IV.length + "]");

            if (encryptedData.length <= 12) {
                ShowError("Encrypted data too short to contain ciphertext");
                return strToDecrypt;
            }

            GCMParameterSpec gcmSpec = new GCMParameterSpec(128, IV);

            if (secretKeySpec == null) secretKeySpec = deriveKey(secretKey, salt);
            if (isTesting) System.out.println("deriveKey  : "+secretKeySpec.getAlgorithm().toString());
            if (secretKeySpec == null) return strToDecrypt;
            Cipher cipher = Cipher.getInstance(TXFM);
            if (isTesting) System.out.println("           : cipher  has been set");
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, gcmSpec);
            if (isTesting) System.out.println("           : cipher  initialised");

            // Extract the actual ciphertext
            byte[] cipherText = Arrays.copyOfRange(encryptedData, 12, encryptedData.length);
            if (isTesting) System.out.println("           : cipherText to ARRAY");
            byte[] decryptedText = cipher.doFinal(cipherText);
            if (isTesting) System.out.println("           : decryptedText obtained");
            return new String(decryptedText, StandardCharsets.UTF_8);
        } catch (Exception e) {
            ShowError(e.getMessage() + "   ###");
            return strToDecrypt;
        }
    }

    private static void ShowError(String eMsg) {
        System.out.println("AES Encrypt ERROR: ----------------------------------------------------");
        System.out.println(eMsg);
        System.out.println(" ");
    }

    public static void SetTest(boolean inval) {
        isTesting = inval;
    }
}
