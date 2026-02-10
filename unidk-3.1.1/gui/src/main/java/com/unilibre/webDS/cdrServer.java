package com.unilibre.webDS;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.KeyStore;
import java.util.Properties;
import java.util.logging.Level;

public class cdrServer {

    private static final char[] TEST_PASSWORD = "passw0rd".toCharArray();
    private static Properties siteConfig;
    private static String siteSecret="";
    private static int IOthreads = 1, workerThreads = 10;

    public static void main(final String[] args) throws Exception {
        SecureProcess();
    }

    private static void SecureProcess() throws Exception {
        logger.uSendMessage("Running in " + System.getProperty("user.dir"));
        logger.uSendMessage("[ Initialising ] ------------------------------------------------");

        Logger root = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.debug(String.valueOf(Level.OFF));
        root.info(String.valueOf(Level.OFF));
        root.warn(String.valueOf(Level.OFF));

        int servePort;
        try {
            servePort = Integer.parseInt(commons.GetValue("urlport", "8188"));
        } catch (NumberFormatException e) {
            logger.uSendMessage("ERROR in urlport setting - not an integer. Default to 8188");
            servePort = 8188;
        }

        cdrCommons.setSecure(commons.GetValue("secure", "").equalsIgnoreCase("true"));
        cdrCommons.setURLhost(commons.GetValue("urlhost", "localhost"));
        cdrCommons.setURLpath(commons.GetValue("urlpath", "cdr/banking/"));
        cdrCommons.setURLport(servePort);

        if (siteConfig == null) {
            String fname = System.getProperty("config", "cdr.properties");
            logger.uSendMessage("Loading: "+fname);
            siteConfig   = commons.LoadProperties(fname);
            siteSecret   = siteConfig.getProperty("secret", "");

            // IOthreads        :   2 per cpu
            // workerThreads    :  10 per cpu

            try {
                IOthreads      = Integer.parseInt(siteConfig.getProperty("iothreads",     String.valueOf(IOthreads)));
                workerThreads  = Integer.parseInt(siteConfig.getProperty("workerthreads", String.valueOf(workerThreads)));
                commons.Expiry = Long.parseLong(   siteConfig.getProperty("responseTTL",   String.valueOf(10000)));
            } catch (NumberFormatException nfe) {
                logger.uSendMessage("Error in cdr.properties: "+nfe.getMessage());
                System.exit(0);
            }

        }

        if (commons.Expiry < 100) commons.Expiry = 10000;

        cdrCommons.Initialise();

        logger.uSendMessage(" ");
        logger.uSendMessage("[ Set Security Features ON ] ------------------------------------");
        SSLContext sslContext = SSLContext.getDefault();
        if (cdrCommons.isSecure()) {
            KeyStore ks = commons.loadKeyStore("server.keystore");
            KeyStore ts = commons.loadKeyStore("server.truststore");
            sslContext = commons.CreateContext(ks, ts);
        }

        String chkPath = "/" + cdrCommons.getPath();

      //  if (commons.debugging) {
            logger.uSendMessage("------------------------------------------");
            logger.uSendMessage("     BaseCamp: " + commons.BaseCamp);
            logger.uSendMessage("    IOthreads: " + IOthreads);
            logger.uSendMessage("workerThreads: " + workerThreads);
            logger.uSendMessage("  Message TTL: " + (commons.Expiry / 1000) + " seconds.");
            logger.uSendMessage("------------------------------------------");
            logger.uSendMessage("Create Undertow on (" + cdrCommons.getHost() + ":" + cdrCommons.getPort() + ")");
            logger.uSendMessage("------------------------------------------");
            logger.uSendMessage(" ");
      //  }

        new cdrReceiver(IOthreads, workerThreads, sslContext, siteSecret, chkPath);

        logger.uSendMessage("Ready to receive requests on URL " + cdrCommons.getAddress());
    }
    
}