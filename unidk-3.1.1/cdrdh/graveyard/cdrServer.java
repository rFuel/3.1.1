package com.unilibre.cdroem;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.KeyStore;
import java.util.Properties;
import java.util.logging.Level;

public class cdrServer {

    private static final char[] TEST_PASSWORD = "passw0rd".toCharArray();
    private static Properties siteConfig;
    private static boolean stopNow=false;
    private static boolean working=false;
    private static boolean monitor=true;
    public static String status;
    private static String siteSecret="";
    private static int IOthreads = 1, workerThreads = 10;

    private static void SecureProcess() throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.debug(String.valueOf(Level.OFF));
        root.info(String.valueOf(Level.OFF));
        root.warn(String.valueOf(Level.OFF));

        String cwd = System.getProperty("user.dir")+ "/cdr";
        commons.uSendMessage("Ruinning in " + cwd);
        commons.uSendMessage("[ Initialising ] ------------------------------------------------");
        commons.SetCommons(commons.LoadProperties(cwd + "/rFuel.properties"));
        if (commons.ZERROR) return;
        int servePort;
        try {
            servePort = Integer.valueOf(commons.GetValue("urlport", "8188"));
        } catch (NumberFormatException e) {
            commons.uSendMessage("ERROR in urlport setting - not an integer. Default to 8188");
            servePort = 8188;
        }

        cdrCommons.setSecure(commons.GetValue("secure", "").toLowerCase().equals("true"));
        cdrCommons.setURLhost(commons.GetValue("urlhost", "localhost"));
        cdrCommons.setURLpath(commons.GetValue("urlpath", "cdr/banking/"));
        cdrCommons.setURLport(servePort);

        siteSecret = commons.GetValue("secret", "");
        try {
            IOthreads      = Integer.valueOf(commons.GetValue("iothreads", "2"));
            workerThreads  = Integer.valueOf(commons.GetValue("workerthreads", ""));
            commons.Expiry = Long.valueOf(commons.GetValue("responseTTL", ""));
        } catch (NumberFormatException nfe) {
            commons.uSendMessage("Error in cdr.properties: "+nfe.getMessage());
            System.exit(0);
        }

        if (commons.Expiry < 100) commons.Expiry = 10000;
        
        cdrCommons.Initialise();

        if (commons.ZERROR) {
            commons.uSendMessage("Initialisation ERROR: cannot proceed");
            commons.uSendMessage(commons.Zmessage);
            return;
        }

        commons.uSendMessage("[ Set Security Features ON ] ");
        KeyStore ks = commons.loadKeyStore(commons.GetValue("server.keystore", "Not-in-Properties"));
        KeyStore ts = commons.loadKeyStore(commons.GetValue("server.truststore", "Not-in-Properties"));
        SSLContext sslContext = commons.CreateContext(ks, ts);

        String chkPath = "/" + cdrCommons.getPath();
        commons.uSendMessage(" ");
        commons.uSendMessage("-----------------------------------------------------------------");
        commons.uSendMessage("     BaseCamp: " + commons.BaseCamp);
        commons.uSendMessage("    IOthreads: " + IOthreads);
        commons.uSendMessage("workerThreads: " + workerThreads);
        commons.uSendMessage("  Message TTL: " + (commons.Expiry / 1000) + " seconds.");
        commons.uSendMessage("-----------------------------------------------------------------");
        commons.uSendMessage("Create Undertow on (" + cdrCommons.getHost() + ":" + cdrCommons.getPort() + ")");
        commons.uSendMessage("-----------------------------------------------------------------");
        commons.uSendMessage(" ");
        new cdrReceiver(IOthreads, workerThreads, sslContext, siteSecret, chkPath);

        if (!commons.ZERROR) {
            commons.uSendMessage("Ready to receive requests on URL " + cdrCommons.getAddress());
        }
    }

    public static void main(final String[] args) throws Exception {
        SecureProcess();
    }
}