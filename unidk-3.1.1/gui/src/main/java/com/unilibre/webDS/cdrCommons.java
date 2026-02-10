package com.unilibre.webDS;


import com.northgateis.reality.rsc.RSCException;
import java.io.IOException;
import java.util.Objects;
import static com.unilibre.webDS.Constants.*;


public class cdrCommons {

    private static boolean secureHTTP = false;
    private static String URLhost;
    private static String URLpath;
    private static String bindAddess = "";
    private static int URLport;

    public static void Initialise() throws IOException {
        bindAddess = "";
        String protocol = "http";

        boolean URLnotset = false;
        if (Objects.equals(URLpath, ""))   URLnotset = true;
        if (URLnotset) {
            // URL can be over-riden by script property
            String checkURL = System.getProperty("serveurl", "");
            if (!checkURL.equals("")) URLpath = checkURL;
        }

        if (secureHTTP) protocol = "https";
        bindAddess = protocol + "://" + URLhost + ":" + URLport;
        if (!URLpath.startsWith("/")) bindAddess += "/";
        bindAddess += URLpath;
        setURLhost(URLhost);
        setURLport(URLport);
        setURLpath(URLpath);
    }

    public static String HandleResponse(nsCommonData comData, String request) {

        String action;
        if (request.length() > 0) {
            action = comData.nsMsgGet("action");
            logger.uSendMessage("   .) RequestHandler(" + action + ")");
        } else {
            return commons.ResponseHandler("400", "Bad Request", "ERROR: The request body was empty");
        }

        String CorrID = comData.nsMsgGet("correlationid");

        logger.uSendMessage("   .) Message:  task = " + action);
        logger.uSendMessage("   .) Correll:  " + CorrID);
        logger.uSendMessage("   .) Calling commons.HandleRequest()");
        //            reply = comData.HandleRequest("");
        try {
            return HandleRequest(comData, "");
        } catch (RFuelException rFuelException) {
            rFuelException.printStackTrace();
            return commons.ResponseHandler(Integer.toString(rFuelException.getStatus()),"RFuel Exception",rFuelException.getMessage());
        }catch (RSCException rscException) {
            rscException.printStackTrace();
            return commons.ResponseHandler("500","RSC Exception",rscException.toString());
        } catch (IOException e) {
            e.printStackTrace();
            return commons.ResponseHandler("500","IO Exception",e.getMessage());
        }
    }

    public static String HandleRequest(nsCommonData comData, String prefix) throws IOException {
        comData.logHeader = prefix;
        if (!comData.logHeader.endsWith(" ")) comData.logHeader += " ";
        String reply = "";

        String xMap = comData.nsMsgGet("map");
        String fqn =  baseCamp + slash + xMap;
        String mapDetails = commons.ReadDiskRecord(fqn);
        comData.nsMapLoader(mapDetails);

        if (comData.nsMapGet("class").equalsIgnoreCase("cdrob")) {
            String type = comData.nsMapGet("domain").toLowerCase();

            nsOBMethods methods = new nsOBMethods();

            switch (type) {
                case "customerid":
                    reply = methods.GetCustomerID(comData);
                    break;
                case "customer":
                    reply = methods.GetCustomer(comData);
                    break;
                case "account":
                    reply = methods.GetAccounts(comData);
                    break;
                case "transactions":
                    reply = methods.GetTransactions(comData);
                    break;
                case "transactionsv2":
                    reply = methods.GetTransactionsV2(comData);
                    break;
                case "payees":
                    reply = methods.GetPayees(comData);
                    break;
                case "payments":
                    reply = methods.GetPayments(comData);
                    break;
                case "":
                    break;
            }
        } else {
            logger.logthis(comData.logHeader +   " Issue in Action map - it needs a class and domain definition");
        }

        logger.logthis(comData.logHeader +   " rFuel has completed instruction for " + xMap);
        return reply;
    }

    public static void setURLhost(String val) {
        if (!val.equals(URLhost)) {
            logger.uSendMessage("Changing URLhost from " + URLhost + "  to   " + val);
            URLhost = val;
        }
    }

    public static void setURLport(int val) { URLport = val; }

    public static void setURLpath(String val) { URLpath = val; }

    public static void setSecure(boolean val) {secureHTTP = val; }

    public static String getAddress() { return bindAddess; }

    public static String getPath() { return URLpath; }

    public static int getPort() { return URLport; }

    public static String getHost() { return URLhost; }

    public static boolean isSecure() {
        return secureHTTP;
    }

}
