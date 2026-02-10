package com.unilibre.gui;


import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.UnrecoverableKeyException;
import java.text.SimpleDateFormat;
import java.util.*;

public class guiMethods {

    private static String authCode = "";
    private static String reqCode = "";
    private static ArrayList<String> files = null;
    private static ArrayList<String> users = null;
    private boolean register = false;
    private boolean deregister = false;
    private final String reqBody = "record";
    private final String reqDir  = "dir";
    private final String reqID   = "id";
    private final String CWD  = System.getProperty("user.dir");
    //
    public ArrayList<String> keystoreNames = new ArrayList<>();
    public Properties props = new Properties();
    public SSLContext sslContext;
    public ArrayList<String> keystoreLocns = new ArrayList<>();
    private String appjson = "application/json";
    private String keystoreName = "server.keystore";
    private String truststoreName = "server.truststore";
    private String bindAddess = "";
    private String protocol = "https";
    private String URLhost;
    private int URLport;
    private String URLpath;


    public String ReadDiskRecord(String infile) {

        String err = "<<ERROR>>", rec = "";
        BufferedReader BRin = null;
        FileReader fr = null;
        try {
            fr = new FileReader(infile);
            BRin = new BufferedReader(fr);
            String line = null;
            try {
                line = BRin.readLine();
            } catch (IOException e) {
                uSendMessage("read FAIL on " + infile);
                uSendMessage(e.getMessage());
                rec = err;
            }
            while ((line) != null) {
                rec = rec + line + "\n";
                try {
                    line = BRin.readLine();
                } catch (IOException e) {
                    uSendMessage("read FAIL on " + infile);
                    uSendMessage(e.getMessage());
                    rec = err;
                }
            }
            try {
                BRin.close();
                BRin = null;
                fr.close();
                fr = null;
            } catch (IOException e) {
                uSendMessage("File Close FAIL on " + infile);
                uSendMessage(e.getMessage());
                rec = err;
            }
        } catch (IOException e) {
            uSendMessage(e.getMessage());
            rec = err;
        }
        return rec;
    }

    public void uSendMessage(String inMsg) {
        if (inMsg == null) return;
        String mTime, mDate, MSec;
        String iam = "{" + (Thread.currentThread().getName().replaceAll("\\ ", "-")) + "} ";
        int ThisMS;
        mTime = "";
        mDate = "";
        MSec = "";
        mTime = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
        mDate = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
        ThisMS = Calendar.getInstance().get(Calendar.MILLISECOND);
        MSec = "." + (ThisMS + "000").substring(0, 3);
        System.out.println(iam + " " + mDate + " " + mTime + MSec + " " + inMsg);
        mTime = "";
        mDate = "";
        MSec = "";
        ThisMS = 0;
        inMsg = "";
    }

    public String GetValue(String inValue, String def) {
        String value = System.getProperty(inValue, def);
        if (value.equals(null) || value.equals("")) value = def;
        return value;
    }

    public String BuildJsonReply(String status, String response, String laps) {
        String message = "";
        if (status == "200") {
            message = "OK";
        } else if (status == "401") {
            message = "Login required.";
        } else {
            message = "Bad request";
        }
        JSONObject jMsg = new JSONObject();
        jMsg.put("response", response);
        jMsg.put("message", message);
        jMsg.put("status", status);
//        jMsg.put("query-time", laps);
        return jMsg.toString();
    }

    public char[] password(String name) {
        String pw = name + ".password";
        if (this.keystoreNames.indexOf(name) < 0) {
            uSendMessage("     location for [" + name + ".password] has not been provided.");
            System.exit(1);
        } else {
            pw = this.keystoreLocns.get(this.keystoreNames.indexOf(name)) + ".password";
        }
        String pword = ReadDiskRecord(pw);
        if (pword.equals("<<ERROR>>")) System.exit(1);
        while (pword.endsWith("\n")) { pword = pword.substring(0,pword.length()-1); }
        return pword.toCharArray();
    }

    public SSLContext CreateContext(KeyStore kstore, KeyStore tstore) throws Exception {

        KeyManager[] keyManagers;
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        try {
            keyManagerFactory.init(kstore, password(this.keystoreName));
        } catch (UnrecoverableKeyException e) {
            uSendMessage(">>>");
            uSendMessage(">>> SECURITY ABORT: SSLContext for KeyStore: invalid password ");
            uSendMessage(">>>");
            System.exit(1);
        }
        keyManagers = keyManagerFactory.getKeyManagers();

        TrustManager[] trustManagers;
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(tstore);
        trustManagers = trustManagerFactory.getTrustManagers();

        this.sslContext = SSLContext.getInstance("TLS");
        this.sslContext.init(keyManagers, trustManagers, null);
        return this.sslContext;
    }

    public KeyStore loadKeyStore(String name) throws Exception {
        String storeLoc = System.getProperty(name);
        if (this.keystoreNames.indexOf(name) < 0) {
            this.keystoreNames.add(name);
            this.keystoreLocns.add(storeLoc);
        }
        final InputStream stream;
        if (storeLoc == null) {
            stream = GuiReceiver.class.getResourceAsStream(name);
        } else {
            stream = Files.newInputStream(Paths.get(storeLoc));
        }

        if (stream == null) {
            throw new RuntimeException("Could not load keystore [" + name + "]");
        }
        try (InputStream is = stream) {
            KeyStore loadedKeystore = KeyStore.getInstance("JKS");
            loadedKeystore.load(is, password(name));
            return loadedKeystore;
        }
    }

    public void BreakDown(guiMethods gm, cdrCommons com, String messageIn) {
        JSONObject obj;
        try {
            obj = new JSONObject(messageIn);
        } catch (JSONException je) {
            com.SetError(true, je.getMessage());
            return;
        }

        com.SetReqArrays();
        Iterator<String> jKeys = null;
        jKeys = obj.getJSONObject("request").keys();
        String zkey, zval;
        while (jKeys.hasNext()) {
            zkey = jKeys.next();
            zval = obj.getJSONObject("request").get(zkey).toString();
            zkey = zkey.toLowerCase();
            com.AddReqDetails(zkey, zval);
        }

    }

    private boolean Verify(guiMethods gm, cdrCommons com, String auth) {
        Base64.Decoder decoder = Base64.getDecoder();
        String dStr = "";
        try {
            dStr = new String(decoder.decode(auth));
            // dStr: -------------------------------
            // [0]  : 2020-05-05|11:14:63
            // [1]  : admin     (username)
            // [2]  : admin     (password)
        } catch (IllegalArgumentException e) {
            gm.uSendMessage("ERROR: Base64 - " + e.getMessage());
            com.SetError(true, e.getMessage());
            return false;
        }
        String[] authParts = dStr.split("\\:");
        if (authParts.length < 2) { return false; }
        String usr = authParts[0];
        String pwd = authParts[1];

//        Properties userProps = LoadProperties(gm, com, com.GetBasecamp() + CWD + "/conf/uxusers/" + usr);
        Properties userProps = LoadProperties(gm, com, com.GetBasecamp() + "/conf/uxusers/" + usr);
        String pword = userProps.getProperty("password");
        if (pword == null) pword = "";
        userProps = null;

        if (!pword.equals(pwd)) {
            com.SetError(true, "Authorisation Failure - username / password invalid");
            return false;
        } else {
            return true;
        }
    }

    private String GetRequestValue(guiMethods gm, cdrCommons com, String key) throws JSONException {
        String ans = "";
        ans = com.FindReqKey(key);
        return ans;
    }

    public String HandleRequest(guiMethods gm, cdrCommons com, String type, HttpServerExchange exchange, String request) {
        String reply = "";
        if (type.equals("GET")) {
            reply = HandleGet(gm, com, exchange, request);
        } else if (type.equals("POST")) {
            reply = HandlePost(gm, com, exchange, request);
        }
        return reply;
    }

    private String HandleGet(guiMethods gm, cdrCommons com, HttpServerExchange exchange, String request) {
        String ans = "";

//        String dir = com.FindReqKey("dir");
//        if (dir.equals("")) return ErrorMsg("No Directory provided.");
//
//        String id  = com.FindReqKey("id");
//        String ext = com.FindReqKey("ext");
//        String record = com.FindReqKey("record");
//
//        adminFiles inreq = new adminFiles(dir, ext, id, record, "", "", "");
//        adminFilesService services = new adminFilesService();
//        adminFilesDataMethods methods = new adminFilesDataMethods(services);
//        adminFilesAPI apiCall = new adminFilesAPI(methods);
//
//        if (!id.equals("")) {
//            adminFiles outreq = apiCall.getFileById(inreq);
//            services.RecToArray(outreq.getRecord());
//            outreq = null;
//            ans = services.buildJsonArray(dir, id, services.GetKeys(), services.GetVals());
//        } else if (!ext.equals("")) {
//            List<String> list = apiCall.getAllFilesByExt(inreq);
//            ans = services.buildJsonArray(dir, id, list, null);
//        } else {
//            return ErrorMsg("No 'id' and no 'ext' parameters to work with.");
//        }
//
//        inreq = null;
//        services = null;
//        methods = null;
//        apiCall = null;

        return ans;
    }

    private String HandlePost(guiMethods gm, cdrCommons com, HttpServerExchange session, String request) {
        gm.BreakDown(gm, com, request);
        files = null;
        String rmv      = GetRequestValue(gm, com,"remove");
        String reg      = GetRequestValue(gm, com,"register");
        String exe      = GetRequestValue(gm, com,"execute");
        String dir      = GetRequestValue(gm, com, reqDir );
        String id       = GetRequestValue(gm, com, reqID  );
        String record   = GetRequestValue(gm, com, reqBody);
        while (record.contains("<im>")) { record = record.replace("<im>", "\n"); }

        if (record.length() == 0) {
            if (!rmv.equals("")) {
                int pos = users.indexOf(authCode);
                if (pos >= 0) {
                    users.remove(pos);
                    deregister = true;
                    return BuildJsonReply("200", "De-registered user", "");
                }
                reqCode = "";
                authCode = "";
            } else if (!reg.equals("")) {
                if (users == null) users = new ArrayList<>();
                if (Verify(gm, com, reg)) {
                    if (users.indexOf(reg) < 0) {
                        users.add(reg);
                        register = true;
                    }
                    return BuildJsonReply("200", "Registered user.", "");
                } else {
                    return BuildJsonReply("400", "Incorrect credentials.", "");
                }
            } else if (!exe.equals("")) {
                exe = exe.replace("execute=", "");
                String output = Execute(exe);
                return BuildJsonReply("200", output, "");
            }
            return BuildJsonReply("400", "Unknown actions.", "");
        }

        if (dir.equals("") || id.equals("")) return BuildJsonReply("400", "Parameters missing", "");
        files = new ArrayList<String>(Arrays.asList(record.split("\\r?\\n")));
        String line="", part1="", part2="";
        record = "";
        int px=0;

        for (int i=0 ; i < files.size() ; i++) {
            line = files.get(i);
            if (line.toUpperCase().contains("ENC(")) {
                uSendMessage("Encrypting " + line);
                if (line.endsWith(")")) line = line.substring(0, line.length()-1);
                px = line.toUpperCase().indexOf("ENC(") + 4;
                part1 = line.substring(0, px);
                part2 = line.replace(part1, "");
                uSendMessage("      Raw: " + part2);
//                part2 = uCipher.Encrypt(part2);
                uSendMessage("      Enc: " + part1 + part2 + ")");
                line = part1 + part2 + ")";
            }
            record += line + "\n";
        }

        files = null;
        line = "";

//        String fqfn = NamedCommon.BaseCamp + NamedCommon.slash + dir + NamedCommon.slash + id;
//        if (!dir.equals("") && !id.equals("")) {
//            if (WriteDiskRecord(fqfn, record)) {
//                return BuildJsonReply("200", "POST: \nwrite body [" + record + "] \\n----------------------------\\nto " + dir + "/" + id, "");
//            } else {
//                return BuildJsonReply("400", "File Handling Error", "");
//            }
//        } else {
//            return BuildJsonReply("400", "Unknown POST action", "");
//        }
        return "";
    }

    private String Execute(String cmd) {
        Runtime rt = Runtime.getRuntime();
        Process p = null;
        String output = "";
        try {
            p = rt.exec(cmd);
            uSendMessage("[dbg]   Get stdInput");
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            uSendMessage("[dbg]   Get stdError");
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line = "************************ Command Output ************************";
            while (line != null) {
                output += line + "\n";
                line = GetLine(stdInput);
            }
            line = "************************ Command Error *************************";
            while (line != null) {
                output += line + "\n";
                line = GetLine(stdError);
            }
        } catch (IOException e) {
            output = e.getMessage();
        }
        return output;
    }

    private String GetLine(BufferedReader stream) {
        String line;
        try {
            line = stream.readLine();
        } catch (IOException e) {
            line = null;
        }
        uSendMessage("[dbg]   line: " + line);
        return line;
    }

    private String GetBody(String component, HttpServerExchange session) {
        String ans = "";
        return ans;
    }

    public Properties LoadProperties(guiMethods gm, cdrCommons com, String fname) {

        if (!fname.contains("/")) { fname = com.GetBasecamp() + "/conf/" +  fname; }

        Properties props = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(fname);
        } catch (FileNotFoundException e) {
            System.out.println("Cannot find " + fname);
            System.out.println(e.getMessage());
        }
        if (is != null) {
            try {
                props.load(is);
            } catch (IOException e) {
                System.out.println("Cannot load " + fname);
                System.out.println(e.getMessage());
            } catch (IllegalArgumentException iae) {
                System.out.println(iae.getMessage());
                System.exit(0);
            }
            try {
                is.close();
            } catch (IOException e) {
                System.out.println("Cannot close '" + fname + "'  " + e.getMessage());
            }
        } else {
            System.out.println("Please load '" + fname + "'");
        }
        return props;
    }

    public void getHeaders(cdrCommons com, HeaderMap requestHeaders) {
        com.SetReqArrays();
        HeaderMap reqMap = requestHeaders;
        int i=0;
        for (HeaderValues entry : reqMap) {
            String ky = entry.getHeaderName().toString();
            String vl = entry.element();
            com.AddReqDetails(ky, vl);
            i++;
        }
        reqMap = null;

    }

    public String ErrorMsg(String msg) {
        return BuildJsonReply("400",  msg, "");
    }

    public String getURLhost() {
        return URLhost;
    }

    public String getURLpath() {
        return URLpath;
    }

    public String getBindAddess() {
        return bindAddess;
    }

    public int getURLport() {
        return URLport;
    }

}
