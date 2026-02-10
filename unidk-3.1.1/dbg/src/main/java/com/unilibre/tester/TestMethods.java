package com.unilibre.tester.tester;

import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.Locale;

public class TestMethods {

    public static boolean doTest = false;

    public static void SetDebugger(boolean val) { doTest = val; }

    public static void HttpTester() {
        if (!doTest) return;
        String message = "{\"property\":\"value\",\"name\":\"Andy\"}";
        String url = "http://rfuel22:8161/api/message";
        String unsub= "action=unsubscribe";
        String ityp = "queue";
        String inam = "outbound";
        String otyp = "queue";
        String onam = "outbound";
        String svc = "?type="+ ityp;
        String get  = "?type=" + otyp;
        String user = "admin", passwd = "admin", sep = ":";
        String usp  = user + sep + passwd;
        String auth = "Basic " + new String(Base64.getEncoder().encode(usp.getBytes()));
        String clid = "clientId=debugger"; // + ManagementFactory.getRuntimeMXBean().getName().split("\\@")[0];

        String sendto = url + "/" + inam + svc + "&" + clid;
        String getfrm = url + "/" + inam + get + "&" + clid;
        String cleanr = url + "/" + inam + "?" + clid + "&" + unsub;

        System.out.println(" Message: " + message);
        System.out.println(" Send To: " + sendto);
        System.out.println("Get From: " + getfrm);
        System.out.println("Unsubsci: " + cleanr);

        try {
            HttpSender(sendto,  auth,  message);
            HttpReceier(getfrm, auth);
            HttpSender(cleanr,  auth,  "");
            System.exit(0);
        } catch (IOException e) {
            System.out.println("   ERROR: " + e.getMessage());
            System.exit(0);
        }
    }

    private static String HttpReceier(String url, String auth) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestMethod("GET");
        conn.setRequestProperty ("Authorization", auth);
        // READ response -------------------------------------------------------
        BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
        StringBuilder  sb = new StringBuilder();
        String rec;
        while ((rec = br.readLine()) != null) { sb.append(rec); }
        System.out.println("Received: " + sb.toString());
        conn.disconnect();
        conn = null;
        NamedCommon.MQgarbo.gc();
        return sb.toString();
    }

    private static void HttpSender(String url, String auth, String message) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty ("Authorization", auth);
        // SEND request --------------------------------------------------------
        OutputStream os = conn.getOutputStream();
        os.write(message.getBytes());
        os.flush();
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            System.out.println(" Failed : HTTP error code : (" + conn.getResponseCode() + ") " + NamedCommon.ReturnCodes.get(conn.getResponseCode()));
            System.exit(0);
        }
        conn.disconnect();
        conn = null;
        NamedCommon.MQgarbo.gc();
    }

    public static void JWTtests() {
        if (!doTest) return;
        String partner = "Mulesoft";
        String client  = "HeritageBank";
        String jti      = "x-rfuel-api";            //
        String iss      = "UniLibre Pty.Ltd.";      // or the partner name e.g. Mulesoft / Salesforce / etc...
        String sub      = partner + "-" + client;   // this is what is validated
        String SECRET_KEY = "";                     // this would have been the rfuel site licence
        SECRET_KEY = partner + "-" + client;
        while (SECRET_KEY.length() < 64) { SECRET_KEY = "@" + SECRET_KEY; }
//        SECRET_KEY = "tPv987x-378-AUS-COM";

//        ArrayList<String> jwtKeys = new ArrayList<>();
//        ArrayList<String> jwtVals = new ArrayList<>();
//        jwtKeys.add("u_bsb")        ;   jwtVals.add("543000");
//        jwtKeys.add("u_action")     ;   jwtVals.add("GetCustomer");
//        jwtKeys.add("u_primaryKEY") ;   jwtVals.add("100001");
//        jwtKeys.add("u_foreignKEYS");   jwtVals.add("[\"\"]");
//        jwtKeys.add("u_page")       ;   jwtVals.add("1");
//        jwtKeys.add("u_page-size")  ;   jwtVals.add("1000");

        String jwt = CreateJWT(SECRET_KEY, jti, iss, sub, partner, client);

        System.out.println("");
        System.out.println(jwt);
        System.out.println("");

        boolean validJWT = ValidateJWT(SECRET_KEY, jwt);

        Base64.Decoder decoder = Base64.getDecoder();
        String Str = new String(decoder.decode(jwt.split("\\.")[1]));
        System.out.println(Str);

        System.exit(1);
    }

    private static String CreateJWT(String SECRET_KEY, String jti, String iss, String sub, String partner, String client) {
//        String keys="", vals="";
//        for (int kv=0 ; kv < jwtKeys.size() ; kv++) {
//            keys += NamedCommon.IMark+jwtKeys.get(kv);
//            vals += NamedCommon.IMark+jwtVals.get(kv);
//        }
//        keys = keys.substring(NamedCommon.IMark.length(), keys.length());
//        vals = vals.substring(NamedCommon.IMark.length(), vals.length());
//        jwtKeys.clear();
//        jwtVals.clear();

//        long nowMillis = System.nanoTime();
//        Date now = new Date(nowMillis);
//        Date exp = now;
//        long ttlMillis = 500000;
//
//        //if it has been specified, let's add the expiration
//        if (ttlMillis > 0) {
//            long expMillis = nowMillis + ttlMillis;
//            exp = new Date(expMillis);
//        }

        // The JWT signature algorithm we will be using to sign the token
        SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;

        SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy", Locale.ENGLISH);
        String dateIssued = "07-07-2013";
        String dateExpire = "31-12-2023";
        Date issued = null, expire = null;
        try {
            issued = formatter.parse(dateIssued);
            expire = formatter.parse(dateExpire);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

        //We will sign our JWT with our ApiKey secret
        byte[] apiKeySecretBytes = DatatypeConverter.parseBase64Binary(SECRET_KEY);
        Key signingKey = new SecretKeySpec(apiKeySecretBytes, signatureAlgorithm.getJcaName());
        SecretKey signingSecretKey = Keys.hmacShaKeyFor(SECRET_KEY.getBytes(StandardCharsets.UTF_8));

        // Set the JWT Claims

        String answer="";
        try {
            answer = Jwts.builder()
                    .setHeaderParam("typ", "JWT")
                    .setId(jti)
                    .setIssuedAt(issued)
                    .setSubject(sub)
                    .setIssuer(iss)
                    .setExpiration(expire)
                    .claim("partner", partner)
                    .claim("client", client)
                    .signWith(signatureAlgorithm, signingKey)
                    .compact()
            ;
        } catch (AbstractMethodError ame) {
            System.out.println("JWT Failure: " + ame.getMessage());
            System.out.println("");
            System.exit(0);
        }
        return answer;
    }

    private static boolean ValidateJWT(String SECRET_KEY, String jwt) {
        uCommons.uSendMessage("Startig ValidateJWT");
        Claims claim;
        byte[] dc = DatatypeConverter.parseBase64Binary(SECRET_KEY);
        uCommons.uSendMessage("SECRET_Key converted to byte[]");
        try {
            claim = Jwts.parser().setSigningKey(dc).parseClaimsJws(jwt).getBody();
            uCommons.uSendMessage("Claims are parsed");
        } catch (JwtException se) {
            System.out.println("JWT Failure: " + se.getMessage());
            System.out.println("");
            return false;
        }

        // the signature of SECRET_KEY has validated

        System.out.println("CLAIMS   : ------------------------------------");
        System.out.println("   apikey: " + claim.getId());
        System.out.println("    Issuer: " + claim.getIssuer());
        System.out.println("   Issued: " + claim.getIssuedAt() );
        System.out.println("   Expires: " + claim.getExpiration());
        System.out.println("  subject: " + claim.getSubject());
        System.out.println("");
        System.out.println("PAYLOAD  : ------------------------------------");
        System.out.println("  partner: " + claim.get("partner"));
        System.out.println("   client: " + claim.get("client"));
        System.out.println("-----------------------------------------------");
        System.out.println("");

        return true;
    }

    private static void OraTrigger(String tableName) {
        Statement statement = null;
        Connection connection = null;
        ResultSet rs = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Fail: " + e.getMessage());
            System.exit(1);
        }
        String cmdGetCols = "select col.column_id, " +
                "col.owner as schema_name, " +
                "col.table_name, " +
                "col.column_name, " +
                "col.data_type, " +
                "col.data_length, " +
                "col.data_precision, " +
                "col.data_scale, " +
                "col.nullable " +
                "from sys.all_tab_columns col " +
                "inner join sys.all_tables t " +
                "on col.owner = t.owner " +
                "and col.table_name = t.table_name " +
                "where col.owner = 'UNILIBRE' " +
                "and col.table_name = '" + tableName + "' " +
                "order by col.column_id";
        String strCols = "";
        String strVals = "";

        //  --------------------------------------------------
        String userName = "unilibre";                       //  jdbcUsr
        String password = "unilibre_svc";                   //  jdbcPwd
        String dbHost = "192.168.48.144";                   //  jdbcCon
        String port = "1521";                               //  jdbcCon
        String sid = "64a52f53a7683286e053cda9e80aed76";    //  jdbcSid
        //  --------------------------------------------------
        String url = "jdbc:oracle:thin:@" + dbHost + ":" + port + "/" + sid;
        //
        try {
            System.out.println("URL: [" + url + "]");
            System.out.println("USR: [" + userName + "]");
            System.out.println("PWD: [" +password + "]");
            connection = DriverManager.getConnection(url, userName, password);
        } catch (SQLException e) {
            HandleError(e);
            System.exit(1);
        }
        if (connection != null) {
            System.out.println("Successfully connected to Oracle DB");
            String colName, valPart;
            try {
                statement = connection.createStatement();
                System.out.println("Execute: " + cmdGetCols);
                rs = statement.executeQuery(cmdGetCols);
                System.out.println("sql command completed successfully");
                while (rs.next()) {
                    colName = rs.getString("COLUMN_NAME");
                    valPart = ":new."+colName+"";
                    strCols += "<ft>" + colName;
                    strVals += "||<ft>||"+valPart;
                }
            } catch (SQLException e) {
                HandleError(e);
                System.out.println("Table not created. No action taken");
                System.exit(1);
            }
            String trigger = "CREATE OR REPLACE TRIGGER TRG_"+tableName + " " +
                    "BEFORE INSERT ON "+tableName + " " +
                    "FOR EACH ROW " +
                    "BEGIN " +
                    "  INSERT INTO AUDLOG.TRXLOGS (COLS, VALS) VALUES(" + strCols + ", " + strVals + ") " +
                    "END; /";
            System.out.println(trigger);
            try {
                statement.close();
                connection.close();
                rs.close();
            } catch (SQLException throwables) {
                HandleError(throwables);
                System.exit(1);
            }
        } else {
            System.out.println("nFailed to connect to Oracle DB");
        }
        System.exit(1);
    }

    private static void TestOra() {
        String sqlCmd = "\nCREATE TABLE upl_TEST (\n   SEQN INT NOT NULL,\n   RECID VARCHAR(150) NOT NULL,\n   NAME VARCHAR(45) NOT NULL,\n   DOB DATE NOT NULL,\n   EMAIL VARCHAR(45) NOT NULL\n)";
        Statement statement = null;
        Connection connection = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Fail: " + e.getMessage());
            System.exit(1);
        }
        //  --------------------------------------------------
        //  jdbcCon=jdbc:oracle:thin:@192.168.48.144:1521
        //  jdbcDvr=oracle.jdbc.driver.OracleDriver
        //  jdbcUsr=unilibre
        //  jdbcPwd=unilibre_svc
        //  jdbcSid=64a52f53a7683286e053cda9e80aed76
        //  --------------------------------------------------
        String userName = "unilibre";                       //  jdbcUsr
        String password = "unilibre_svc";                   //  jdbcPwd
        //  --------------------------------------------------
        String dbHost = "192.168.48.144";                   //  jdbcCon
        String port = "1521";                               //  jdbcCon
        String sid = "64a52f53a7683286e053cda9e80aed76";    //  jdbcSid
        //  --------------------------------------------------
        String url = "jdbc:oracle:thin:@" + dbHost + ":" + port + "/" + sid;
        //
        try {
            System.out.println("URL: [" + url + "]");
            System.out.println("USR: [" + userName + "]");
            System.out.println("PWD: [" +password + "]");
            connection = DriverManager.getConnection(url, userName, password);
        } catch (SQLException e) {
            HandleError(e);
            System.exit(1);
        }
        if (connection != null) {
            System.out.println("Successfully connected to Oracle DB");
            try {
                statement = connection.createStatement();
                System.out.println("Execute: " + sqlCmd);
                statement.execute(sqlCmd);
                System.out.println("Table Created Successfully");
            } catch (SQLException e) {
                HandleError(e);
                System.out.println("Table not created. No action taken");
            }
        } else {
            System.out.println("nFailed to connect to Oracle DB");
        }
        System.exit(1);
    }

    private static void HandleError(SQLException e) {
        System.out.println(e.getMessage().replaceAll("\\r?\\n", ""));
    }

}
