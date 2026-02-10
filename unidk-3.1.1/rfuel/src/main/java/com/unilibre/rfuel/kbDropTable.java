package com.unilibre.rfuel;

import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.SqlCommands;
import com.unilibre.commons.uCommons;

import java.util.ArrayList;
import java.util.Properties;

public class kbDropTable {

    public static void main(String[] args) {
        // ------- [DEV / PROD housekeeping -------------------------------------------
        if (NamedCommon.upl.equals("")) NamedCommon.upl = System.getProperty("user.dir");
        if (NamedCommon.upl.contains("/")) NamedCommon.slash = "/";
        if (NamedCommon.upl.contains("\\")) NamedCommon.slash = "\\";

        if (NamedCommon.upl.contains("/home/andy")) {
            NamedCommon.BaseCamp = NamedCommon.DevCentre;
            NamedCommon.gmods = NamedCommon.BaseCamp + NamedCommon.slash + "lib" + NamedCommon.slash;
        }

        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) System.exit(0);
        uCommons.SetCommons(runProps);

        NamedCommon.u2Source = System.getProperty("file", "NOSUCHFILE");
        NamedCommon.datAct   = System.getProperty("acct", "NOSUCHACCT");

        if (NamedCommon.SqlDatabase.equals("")) NamedCommon.SqlDatabase = NamedCommon.rawDB;

        String takeFile = NamedCommon.u2Source + "_" + NamedCommon.datAct;
        NamedCommon.sqlTarget = takeFile;
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\.", "_");
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\,", "_");
        NamedCommon.sqlTarget = NamedCommon.sqlTarget.replaceAll("\\ ", "_");

        String dbfocus = "[" + NamedCommon.rawDB + "].[raw].[" + NamedCommon.sqlTarget + "]";
        DropTable(dbfocus);

        dbfocus = "[uni].[" + NamedCommon.vwPrefix + NamedCommon.u2Source + "]";
        DroptView(dbfocus);

        dbfocus = "[" + NamedCommon.SqlDatabase + "].[uni].[" + NamedCommon.u2Source + "]";
        DropTable(dbfocus);

    }

    private static void DroptView(String dbfocus) {
        ArrayList<String> dll = new ArrayList<>();
        String cmd = "use " + NamedCommon.SqlDatabase + " DROP VIEW " + dbfocus;
        dll.add(cmd);
        SqlCommands.ExecuteSQL(dll);
        System.out.println(cmd);
        dll = null;
    }

    private static void DropTable(String dbfocus) {
        ArrayList<String> dll = new ArrayList<>();
        String cmd = "DROP TABLE " + dbfocus;
        dll.add(cmd);
        SqlCommands.ExecuteSQL(dll);
        System.out.println(cmd);
        dll = null;
    }

}
