package com.unilibre.commons;


import com.unilibre.cipher.uCipher;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ConnectionPool {

    public static List<Connection> jdbcPool = new ArrayList<>();
    public static List<String> objPool = new ArrayList<>();
    public static List<String> ConString = new ArrayList<>();
    public static List<String> ConFails = new ArrayList<>();
    private static String URL;
    private static String USERID;
    private static String PASSWORD;

    public static void AddToPool(String Url, String UserId, String password) throws SQLException {
        if (Url.equals("")) return;
        if (UserId.equals("")) return;
        if (password.equals("")) return;
        //
        if (UserId.startsWith("ENC(")) {
            UserId = UserId.substring(4, UserId.length());
            if (UserId.endsWith(")")) UserId = UserId.substring(0, UserId.length()-1);
            UserId = uCipher.Decrypt(UserId);
        }
        if (password.startsWith("ENC(")) {
            password = password.substring(4, password.length());
            if (password.endsWith(")")) password = password.substring(0, password.length()-1);
            password = uCipher.Decrypt(password);
        }
        URL = Url;
        USERID = UserId;
        PASSWORD = password;
        if (ConString.indexOf(Url) < 0 ) {
            createConnection();
        } else {
            if (!NamedCommon.isNRT) uCommons.uSendMessage(Url+" is already in the jdbc Connection Pool");
        }
    }

    private static void createConnection() {
        if (!NamedCommon.isNRT && !NamedCommon.runSilent) uCommons.uSendMessage("Connecting to: " + URL);
        Connection con = null;


        if (NamedCommon.runSilent) {
            // remove any instances of it when it has re-tried and failed.
            int idx=0;
            while (true) {
                if (ConFails.size() == 0) break;
                if (ConFails.get(idx).equals(URL)) {
                    ConFails.remove(idx);
                } else {
                    idx++;
                }
                if (idx > ConFails.size()) break;
            }
        }
        if (ConFails.indexOf(URL) < 0) {
            NamedCommon.tConnected = false;
            try {
                if (!NamedCommon.jdbcDvr.equals("")) {
                    try {
                        Class.forName(NamedCommon.jdbcDvr);
                    } catch (ClassNotFoundException e) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "Class.forName() failed - " + e.getMessage();
                        if (!NamedCommon.runSilent) System.out.println(NamedCommon.Zmessage);
                        return;
                    }
                }
                NamedCommon.sqlLite = false;            // For disposable target DB's
                if (NamedCommon.SqlDBJar.toUpperCase().equals("SNOWFLAKE")) {
                    con = DriverManager.getConnection(NamedCommon.jdbcCon);
                } else {
                    con = DriverManager.getConnection(URL, USERID, PASSWORD);
                    // This is for MS SQL only !!
                    DatabaseMetaData metaData = con.getMetaData();
                    String ProductName = metaData.getDatabaseProductName();
                    if (ProductName.toLowerCase().contains("lite")) NamedCommon.sqlLite= true;
                }
                ConString.add(URL);
                // -----------------
                jdbcPool.add(con);
                // -----------------
                NamedCommon.tConnected = true;
                if (NamedCommon.runSilent) {
                    NamedCommon.runSilent = false;
                } else {
                    uCommons.uSendMessage("Connected: ");
                }
                NamedCommon.ZERROR = false;
                NamedCommon.Zmessage = "";
            } catch (SQLException e) {
                if (!NamedCommon.runSilent) {
                    NamedCommon.Zmessage = "Connection FAILED: " + e.getMessage();
                    uCommons.uSendMessage(NamedCommon.Zmessage);
//                    uCommons.uSendMessage("                 : " + URL + " added to failures list.");
                    if (!NamedCommon.runSilent) ConFails.add(URL);
                }
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "";
            }
        } else {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = URL + " has previously failed to connect.";
            if (!NamedCommon.runSilent) {
                ConFails.add(URL);
                uCommons.uSendMessage(NamedCommon.Zmessage);
            }
        }
    }

    public static Connection getConnection(String Url) {
        if (ConString.size() == 1) return jdbcPool.get(0);
        for (int fnd=0 ; fnd < ConString.size() ; fnd++) {
            if (NamedCommon.jdbcCon.startsWith(ConString.get(fnd))) {
                return jdbcPool.get(fnd);
            }
        }
//        uCommons.uSendMessage(Url+" must be added to the connection pool.");
//        return null;
        return jdbcPool.get(0);
    }

    public static boolean releaseConnection(String Url) {
        NamedCommon.tConnected = true;
        int fnd = ConString.indexOf(Url);
        boolean ans=false;
        if (fnd > -1) {
            NamedCommon.uCon = getConnection(Url);
            NamedCommon.jdbcCon = Url;
            if (!NamedCommon.runSilent) uCommons.uSendMessage("Disconnecting from "+Url);
            SqlCommands.ReleaseSQL();
            if (!NamedCommon.runSilent) uCommons.uSendMessage("Disconnected");
            jdbcPool.remove(fnd);
            ConString.remove(fnd);
            ans=true;
        }
        return ans;
    }

    public static String GetUsername() {
        return USERID;
    }

}
