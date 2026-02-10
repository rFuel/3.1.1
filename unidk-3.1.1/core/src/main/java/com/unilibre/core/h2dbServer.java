package com.unilibre.core;


import com.unilibre.commons.NamedCommon;
import org.apache.activemq.console.command.PurgeCommand;
import org.h2.security.auth.H2AuthConfig;
import org.h2.tools.Server;
import java.sql.*;
import java.util.ArrayList;

public class h2dbServer {

    private static Connection h2Conn = null;
    private static String workfile = "";
    private static final String h2conString = "jdbc:h2:mem:uBulk;DB_CLOSE_DELAY=-1";
    private static final String insCmd = "INSERT INTO $$ (uId) VALUES (**)";
    private static final String chkTbl = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%$$%'";
    // -------------------------------------------------------------------------------------------------

    public static boolean CreateServer () {
        return Connect();
    }

    public static boolean Connect() {
        if (h2Conn != null) return true;
        try {
            h2Conn = DriverManager.getConnection(h2conString,  "sa", "sa");
            h2Conn.setAutoCommit(true);
        } catch (SQLException e) {
            h2Conn = null;
        }
        return CreateWF();
    }

    private static boolean CreateWF() {
        if (h2Conn == null) return false;
        workfile = "H2_WORKFILE_"+NamedCommon.pid;
        String create = "CREATE TABLE IF NOT EXISTS "+workfile+" (UID VARCHAR(500) PRIMARY KEY);";
        if (h2Execute(create)) {
            if (didTableCreate(workfile)) NamedCommon.H2Server = true;
            return NamedCommon.H2Server;
        } else {
            return false;
        }
    }

    public static boolean isConnected() { return (h2Conn != null);}

    public static void Shutdown() {
        try {
            if (h2Conn != null) h2Conn.close();
        } catch (SQLException e) {
            System.out.println("h2dbServer.Shutdown()   "+e.getMessage());
        }
        h2Conn = null;
    }

    public static boolean Insert(String uID) {
        if (h2Conn == null) return false;
        String cmd2 = insCmd;
        cmd2 = cmd2.replace("$$", workfile);
        cmd2 = cmd2.replace("**", uID);
        return h2Execute(cmd2);
    }

    public static boolean Insert(ArrayList list) {
        if (h2Conn == null) return false;

        // arraylist list only has uID's in it. Nothing else !!

        String cmd1 = insCmd.replace("$$", workfile);
        String cmd2 = "";
        int eoi = list.size();
        for (int i=0 ; i < eoi ; i++) {
            try {
                cmd2 = cmd1.replace("**", list.get(i).toString());
                Statement stmt = h2Conn.createStatement();
                stmt.execute(cmd2);
                stmt.close();
                return true;
            } catch (SQLException e) {
                System.out.println(" ");
                System.out.println("H2 Error: " + e.getMessage());
                System.out.println(" ");
                return false;
            }
        }
        return false;
    }

    public static boolean HasProcessed(String uid) {
        if (!didTableCreate(workfile)) {
            System.out.println(workfile+"  Cannot be found");
        }
        boolean found = false;
        String checkExists = "SELECT 1 FROM "+workfile+" WHERE UID = '"+uid+"'";
        try {
            Statement stmt = h2Conn.createStatement();
            ResultSet rs = stmt.executeQuery(checkExists);
            while (rs.next()) { found = true; }
            rs.close();
            rs = null;
            stmt.close();
        } catch (SQLException e) {
            try {  h2Conn.close(); } catch (SQLException ex) {
                System.out.println("---------------------------------------------------------------");
                System.out.println("H2 ERROR: " + ex.getMessage());
                System.out.println("Terminating rFuel resilience mode");
                System.out.println("---------------------------------------------------------------");
            }
            h2Conn = null;
            return false;
        }
        checkExists = "";
        return found;
    }

    public static int AlreadyProcessed() {
        if (!didTableCreate(workfile)) {
            System.out.println(workfile+"  Cannot be found");
        }
        String cmd = "SELECT * FROM "+workfile+";";
        int answer = 0;
        try {
            Statement stmt = h2Conn.createStatement();
            ResultSet rs = stmt.executeQuery(cmd);
            while (rs.next()) {
                answer++;
            }
            rs.close();
            rs = null;
            stmt.close();
        } catch (SQLException e) {
            //
        }
        return answer;
    }

    public static boolean DropWorkfiles() {
        boolean answer = false;
        try {
            String cmd = chkTbl.replace("$$", "H2_WORKFILE");
            Statement stmt = h2Conn.createStatement();
            ResultSet rs = stmt.executeQuery(cmd);
            String table = "";
            while (rs.next()) {
                table = rs.getString("TABLE_NAME");
                workfile = table;
//                System.out.println("Dropping "+table);
                Cleanup();
                answer = true;
            }
            if (!answer) {
                System.out.println("No workfiles were found to clean-up.");
            }
            stmt.close();
            stmt = null;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }

        return answer;
    }

    public static boolean Cleanup() {
        String cmd = "DROP TABLE IF EXISTS "+workfile;
        h2Execute(cmd);
        return true;
    }

    public static boolean h2Execute(String cmd) {
        try {
            Statement stmt = h2Conn.createStatement();
            stmt.executeUpdate(cmd);
            stmt.close();
        } catch (SQLException e) {
            try {  h2Conn.close(); } catch (SQLException ex) {
                System.out.println("---------------------------------------------------------------");
                System.out.println("H2 ERROR: " + ex.getMessage());
                System.out.println("Terminating rFuel resilience mode");
                System.out.println("---------------------------------------------------------------");
            }
            h2Conn = null;
            return false;
        }
        return true;
    }

    private static boolean didTableCreate(String tname) {
        boolean answer = false;
        try {
            String sel = chkTbl.replace("$$", tname.toUpperCase());
            Statement stmt = h2Conn.createStatement();
            ResultSet rs = stmt.executeQuery(sel);
            String table = "";
            while (rs.next()) {
                table = rs.getString("TABLE_NAME");
                answer = true;
            }
            if (!answer) {
                System.out.println("No workfile tables were found.");
            }
            stmt.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        return answer;
    }
}
