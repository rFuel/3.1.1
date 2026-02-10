package com.unilibre.restful;

// The SOLE purpose of this class is to manage the UniLibre keystore!!
// -------------------------------------------------------------------
//
// In the keystore are keypairs of private-key & signed certs
//      one for EACH webservice.
// The list of webservices are found in rFuel.properties "webservices"
//
//  1. Generate a CSR for a certificate to be used for a web service (don't share)
//  2. Get it signed (I self sign for testing)
//  3. Bundle the cert and private key into the keystore under the alias.
//
//  TODO
//  *   check for certified aliases and make sure their cert exits - else remove the alias.
//  *   TEST on ubuntu !! It's working in windows debug mode.
//
//  Use the keystore in a web app like MessageReceiver
//  When opening the app, if not cert is found, bring them here.
// -------------------------------------------------------------------

import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509KeyManager;
import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class KeystoreManager {

    // ---------------------------------------------------------------------------------
    private final String flat = "--------------------------------------------------";
    private final boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");
    private String alias, flatline;
    private String iPath = "conf/ssl/";
    private String execCamp = NamedCommon.BaseCamp+"/";
    private String keysPath = execCamp+ NamedCommon.keystorePath;
    private String sslPath  = execCamp+ "conf/ssl/";
    private int daysThreshold = 60;  // expires within 60 days
    private char[] keystorePassword;
    private KeyStore keystore;
    private ArrayList<String> certArr, expDates, faulty, warnings;

    static {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }
    // ---------------------------------------------------------------------------------

    // this is the only way in !!
    public KeystoreManager(String svc) throws Exception {
        flatline = flat + flat + flat + flat + flat;
        alias = "";
        if (!svc.equals("")) alias = svc;
        boolean okay   = loadProperties();
        if (okay) okay = loadKeystore();
        if (okay) {
            if (alias.equals("")) {
                ShowHeading();
                List<String> services = getAllWebservices();
                int eol = services.size(), max=0, wsvc=0, idx;

                for (int i = 0; i < eol; i++) { if (services.get(i).length() > max) max=services.get(i).length()+5; }

                while (true) {
                    GetKeystoreStats();
                    String tmp, line;
                    int lnx;
                    System.out.println(" ");
                    System.out.println("Keystore: UniLibreKeys");
                    line = "   " + uCommons.LeftHash("------ alias ------------------", max)+ ":"+
                            uCommons.LeftHash("------------ certificate expiry -------------------------", 45) + ":"+
                            uCommons.LeftHash("-- warnings -------------", 15);
                    lnx = line.length();
                    System.out.println(line);
                    for (int i = 0; i < eol; i++) {
                        tmp = "";
                        idx = certArr.indexOf(services.get(i));
                        if (idx >-1) tmp = "Cert expires: " + expDates.get(i);
                        if (faulty.indexOf(services.get(i)) > -1) tmp = "Faulty Certificate!!";
                        line = uCommons.LeftHash(services.get(i), max) + ":";
                        line += uCommons.LeftHash(tmp, 45) + ":";
                        if (idx > -1) line += warnings.get(i);
                        System.out.println((i + 1) + ". " + line);
                    }
                    System.out.println(uCommons.LeftHash(flatline, lnx));
                    System.out.print("Choose 1 - " + eol + " or [Q]uit: ");
                    String ans = GetUserInput();
                    if (ans.toUpperCase().equals("Q")) {
                        System.out.println("Thank you for your help with the keystore. Bye for now.");
                        System.out.println(" ");
                        return;
                    }
                    try {
                        wsvc = Integer.valueOf(ans);
                    } catch (NumberFormatException nfe) {
                        System.out.println("Please make your choice from the options provided.");
                        continue;
                    }
                    if (wsvc < 1 || wsvc > eol) {
                        System.out.println("Please make your choice from the options provided.");
                        continue;
                    }
                    System.out.println("****************************************************");
                    wsvc--;
                    handleWebservice(services.get(wsvc));
                    loadKeystore();
                }
            }
        }
    }

    public boolean KeyStoreIsReady() throws KeyStoreException {
        if (alias.equals("")) return false;
        return keystoreContainsAlias(alias);
    }

    private String GetUserInput() {
        Scanner reader = new Scanner(System.in);
        String val = reader.nextLine();
        return val;
    }

    // Get a bunch of stats from the keystore and show the user
    private void GetKeystoreStats() throws KeyStoreException {
        certArr = new ArrayList<>();
        expDates = new ArrayList<>();
        faulty = new ArrayList<>();
        warnings = new ArrayList<>();;
        Enumeration<String> aliases = keystore.aliases();
        String tmp, warn;
        Certificate cert;
        long now = System.currentTimeMillis(), threshold;

        while (aliases.hasMoreElements()) {
            tmp = aliases.nextElement();
            cert = keystore.getCertificate(tmp);
            warn = "";
            if (cert instanceof X509Certificate) {
                X509Certificate x509 = (X509Certificate) cert;
                Date expiry = x509.getNotAfter();
                threshold = now + daysThreshold * 24L * 60 * 60 * 1000;
                if (expiry.getTime() < threshold) warn = "  *** expires soon ***";
                certArr.add(tmp);
                expDates.add(expiry.toString());
            } else {
                faulty.add(tmp);
            }
            warnings.add(warn);
        }

    }

    // Load configuration properties
    private boolean loadProperties() {
        if (!NamedCommon.BaseCamp.equals(NamedCommon.DevCentre)) {
            String opsys = System.getProperty("os.name");
            if (opsys.toLowerCase().contains("windows")) {
                String old = NamedCommon.BaseCamp;
                NamedCommon.BaseCamp = NamedCommon.DevCentre;
                String knw = NamedCommon.BaseCamp;
                uCommons.uSendMessage("Resetting BaseCamp to " + knw);
                String slash = "/";
                NamedCommon.gmods = NamedCommon.BaseCamp + slash + "lib" + slash;
            }
        }

        // keystorePath is HARD-CODED to "conf/UniLibreKeys.p12" do not change !!
        NamedCommon.keystorePath = NamedCommon.BaseCamp + NamedCommon.slash + NamedCommon.keystorePath;
        iPath = NamedCommon.BaseCamp + NamedCommon.slash + iPath;

        Properties rProps = uCommons.LoadProperties("rFuel.properties");if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(rProps);

        if (NamedCommon.KeystorePassword.equals("")) {
            System.out.println("No password found for the UniLibre keystore. Please contact support@unilibre.com.au");
            return false;
        }
        keystorePassword = NamedCommon.KeystorePassword.toCharArray();
        return true;
    }

    // Initialize keystore (load existing or create new)
    private boolean loadKeystore() {
        try {
            keystore = KeyStore.getInstance("PKCS12");
            File ksFile = new File(NamedCommon.keystorePath);
            if (ksFile.exists()) {
                try (InputStream in = new FileInputStream(ksFile)) {
                    keystore.load(in, keystorePassword);
                }
            } else {
                keystore.load(null, keystorePassword);
                saveKeystore(); // Write empty keystore
                System.out.println(NamedCommon.keystorePath + " did not exist so an empty keystore has been created.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    // Save keystore to disk
    private void saveKeystore() {
        try (OutputStream out = new FileOutputStream(NamedCommon.keystorePath)) {
            keystore.store(out, keystorePassword);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // OPTION A: Called directly with a specific webservice
    private void handleWebservice(String alias) throws Exception {
        if (!keystoreContainsAlias(alias)) {
            String csrSubject = "CN=rfuel, OU=Data Services, O=UniLibre, L=Brisbane, ST=QLD, C=AU";
            String sslDir = iPath;
            Path csrPath= Paths.get(iPath);
            if (Files.notExists(csrPath)) Files.createDirectory(csrPath);
            // Check IF the csr was previously generated
            String csrFile = iPath+alias+".csr";
            File check = new File(csrFile);
            if (check.exists()) {
                System.out.println("NOTE: a CSR has already been generated for "+alias);
                System.out.println("      you can over-write but that will cause issues for your CA");
                // you build a csr, sent it to security for signing, they approve it and send you a cert
                // then you over-write the agreed csr?! Not a nice thing to do!
            }
            System.out.println(" ");
            int task=0;
            while (true) {
                System.out.println("     Context::  "+alias);
                System.out.println("     ----------------------------------------------");
                System.out.println("     1. Create a CSR & Private Key");
                System.out.println("     *  Send to CA for signing");
                System.out.println("     *  Load the CERT into "+iPath+"/"+alias+".crt");
                System.out.println("     2. Bundle (Import) the Certificate and Private Key");
                System.out.print("        Choose 1, 2 or [Q]uit: ");
                String ans = GetUserInput();
                System.out.println("     ----------------------------------------------");
                if (ans.toUpperCase().equals("Q")) break;
                try {
                    task = Integer.valueOf(ans);
                } catch (NumberFormatException nfe) {
                    System.out.println("Please choose from the optins");
                    continue;
                }
                switch (task) {
                    case 1:
                        System.out.println("     Generate CSR for "+alias);
                        generateCSR(alias, csrSubject, csrFile);
                        break;
                    case 2:
                        System.out.println("     Bundle .crt and .key for "+alias);
                        bundleCrtAndKey(alias);
                        break;
                    default:
                        System.out.println("     Please make your choice from the options provided.");
                }
            }
        } else {
            System.out.println(alias+" is already in the keystore.");
            System.out.println("----------------------------------------------------");
            showCertDetails(alias);
            System.out.println("----------------------------------------------------");
            System.out.print("Press enter to go back or [D]elete the cert.");
            String ans = GetUserInput();
            if (!ans.toUpperCase().equals("D")) return;
            System.out.println("DELETING the cert allows you to start the process again:");
            System.out.println("Create a certificate signing request - CSR");
            System.out.println("Send the CSR to your CA for signing");
            System.out.println("Bundle the signed certificate with its private key");
            System.out.println("Import them into this managed keystore.");
            System.out.println("");
            System.out.print("Do you want to DELETE this certificate (y/N): ");
            ans = GetUserInput();
            if (!ans.toUpperCase().equals("Y")) return;
            System.out.println("DELETING the certificate now ...");
            keystore.deleteEntry(alias);
            uCommons.DeleteFile(iPath+alias+".crt");
            uCommons.DeleteFile(iPath+alias+".csr");
            uCommons.DeleteFile(iPath+alias+".key");
            saveKeystore();
            uCommons.Sleep(5);
            loadKeystore();     // get an updated copy of the keystore
        }
    }

    // OPTION B: Display all services from configuration properties
    private List<String> getAllWebservices() {
        return Arrays.stream(NamedCommon.webservices.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    // Find the alias in the managed Keystore
    private boolean keystoreContainsAlias(String alias) throws KeyStoreException {
        return keystore.containsAlias(alias);
    }

    // Generate a certificate signing request
    private void generateCSR(String alias, String subjectDN, String csrFile) throws Exception {

        // Generate RSA KeyPair (can be replaced with loaded keys from Keystore)
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        System.out.println("        > Generating KeyPair ... please wait ...");
        KeyPair keyPair = keyGen.generateKeyPair();

        // Build subject DN
//        X500Name subject = new X500Name(String.format("CN=%s, O=%s, C=%s", "rfuel", "UniLibre", "AU"));
        X500Name subject = new X500Name(String.format(subjectDN));

        // Create the CSR
        PKCS10CertificationRequestBuilder csrBuilder = new JcaPKCS10CertificationRequestBuilder(subject, keyPair.getPublic());

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());

        var csr = csrBuilder.build(signer);

        // Write to file
        System.out.println("        > Writing    csr      to file: conf/ssl/"+alias+".csr");
        try (PemWriter pemWriter = new PemWriter(new FileWriter(csrFile))) {
            pemWriter.writeObject(new org.bouncycastle.util.io.pem.PemObject("CERTIFICATE REQUEST", csr.getEncoded()));
        }

        // Save Private Key to file
        String keyFile = csrFile.replace(".csr", ".key");
        System.out.println("        > Writing Private Key to file: conf/ssl/"+alias+".key");
        try (PemWriter keyWriter = new PemWriter(new FileWriter(keyFile))) {
            keyWriter.writeObject(new PemObject("PRIVATE KEY", keyPair.getPrivate().getEncoded()));
        }

        if (!isWindows) {
            Path keyPath = Paths.get(keyFile);
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
            Files.setPosixFilePermissions(keyPath, perms);
        }

        System.out.println("        > CSR and Private Key successfully generated for alias: " + alias);
        System.out.println("        > Please send it to your CA for signing.");
        System.out.println("        > Once it is signed, load it into /upl/ssl/"+alias+".crt");
    }

    // Package the signed certificate with the private key
    private void bundleCrtAndKey(String alias) {

        List<String> cmdList = new ArrayList<>();

        if (isWindows) {
            // Assemble OpenSSL command as a single string
            // REMEMBER ssh means this runs on ubuntu !!
            String remoteCmd = "openssl pkcs12 -export " +
                    "-inkey " + sslPath + alias + ".key " +
                    "-in " + sslPath + alias + ".crt " +
                    "-name " + alias + " " +
                    "-out " + keysPath + " " +
                    "-passout pass:" + NamedCommon.KeystorePassword;
            cmdList.add("ssh");
            cmdList.add("unilibre@rfuel14");
            cmdList.add(remoteCmd);
        } else {
            cmdList.addAll(Arrays.asList(
                    "openssl", "pkcs12", "-export",
                    "-inkey", sslPath + alias + ".key",
                    "-in", sslPath + alias + ".crt",
                    "-name", alias,
                    "-out", keysPath,
                    "-passout", "pass:" + NamedCommon.KeystorePassword
            ));
        }

        String randFile = System.getProperty("os.name").toLowerCase().contains("win") ? "NUL" : "/tmp/.rnd";
        String line="";
        try {
            ProcessBuilder pb = new ProcessBuilder(cmdList);
            Map<String, String> env = pb.environment();
            env.put("RANDFILE", randFile);
            Process process = pb.start();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroy();
                System.err.println("     > OpenSSL command timed out.");
                return;
            }

            // Read the output and error streams
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            BufferedReader errors = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String pfx = "   ) ";
            try {
                while ((line = reader.readLine()) != null) {System.out.println(pfx + line);}
                while ((line = errors.readLine()) != null) {System.err.println(pfx + line);}
            } catch (IOException e) {
                System.out.println("Reader Error: " + e.getMessage());
            }
            uCommons.Sleep(2);
            loadKeystore();     // get an updated copy of the keystore
        } catch (IOException | InterruptedException | IllegalStateException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    // obtain details of the certs in the keystore
    private void showCertDetails(String alias) {
        try {
            loadKeystore(); // Your method
            Certificate cert = keystore.getCertificate(alias);

            if (cert instanceof X509Certificate) {
                X509Certificate x509 = (X509Certificate) cert;
                System.out.println("   Alias      : " + alias);
                System.out.println("   Subject    : " + x509.getSubjectDN());
                System.out.println("   Issuer     : " + x509.getIssuerDN());
                System.out.println("   Valid From : " + x509.getNotBefore());
                System.out.println("   Valid Until: " + x509.getNotAfter());
            } else {
                System.out.println("Certificate for alias '" + alias + "' is not X.509.");
            }
        } catch (Exception e) {
            System.err.println("Error reading certificate: " + e.getMessage());
        }
    }

    // show the certificate process flow
    private void ShowHeading() {
        System.out.println(" ");
        System.out.println("Create CSR >> Send to CA >> Import CERT >> Maintain keystore");
    }

    // Get the sslContext for alias
    public SSLContext GetSSLcontext() throws Exception {
        String certAlias = alias;

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream(keysPath)) {
            ks.load(fis, keystorePassword);
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, keystorePassword);

        X509KeyManager origKm = null;
        for (KeyManager km : kmf.getKeyManagers()) {
            if (km instanceof X509KeyManager) {
                origKm = (X509KeyManager) km;
                break;
            }
        }

        if (origKm == null) throw new IllegalStateException("No X509KeyManager found");

        X509KeyManager finalOrigKm = origKm;
        X509KeyManager certAliasKm = new X509KeyManager() {
            @Override public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
                return certAlias;
            }
            @Override public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
                return certAlias;
            }
            @Override public X509Certificate[] getCertificateChain(String requestedAlias) {
                return finalOrigKm.getCertificateChain(certAlias);
            }
            @Override public PrivateKey getPrivateKey(String requestedAlias) {
                return finalOrigKm.getPrivateKey(certAlias);
            }
            @Override public String[] getClientAliases(String keyType, Principal[] issuers) {
                return finalOrigKm.getClientAliases(keyType, issuers);
            }
            @Override public String[] getServerAliases(String keyType, Principal[] issuers) {
                return finalOrigKm.getServerAliases(keyType, issuers);
            }
        };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(new KeyManager[]{certAliasKm}, null, null);
        return sslContext;
    }

    // When sending or using windows paths to files
    public static String sanitiseForWindows(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Path cannot be null or empty");
        }

        // Normalize to backslashes (Windows format)
        String backslashPath = path.replace("/", "\\");

        // Escape for Windows shell
        String escaped = backslashPath.replace("\\", "\\\\");

        // Wrap in double quotes
//        return "\"" + escaped + "\"";
        return escaped;
    }

    private void savePrivateKeyAsPem(PrivateKey privateKey, Path keyPath) throws IOException {
        // keyPath and csrPath should be the same.
        byte[] encoded = privateKey.getEncoded();
        String pem = "-----BEGIN PRIVATE KEY-----\n" +
                     Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(encoded) +
                     "\n-----END PRIVATE KEY-----\n";
        Files.write(keyPath, pem.getBytes());
    }

}