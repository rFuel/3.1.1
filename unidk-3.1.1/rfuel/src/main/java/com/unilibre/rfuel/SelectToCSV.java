package com.unilibre.rfuel;

// Usage:  SelectToCSV.sh {fully qualified path to file}

import com.unilibre.cipher.uCipher;
import com.unilibre.commons.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class SelectToCSV {

    // Assumptions ----------------------------------------------------
    //      Input   : A config file.
    //              : TWO formats
    //              : outfile={name of file to create}
    //              : outpath={where to write it to}
    //              : sudo={true / false}
    //              : password=ENC({sudo password})
    //              : # 1
    //                  : command={file holding the sql}
    //              : # 2
    //                  : database=
    //                  : columns={comma separated list IN ORDER ... or blank and it will work it out}
    //                  : from={table, join, where, and, or ... etc.}
    //      Process : Build select statememt
    //                Place columns in an array.
    //                Add "from" line to the end of the statement
    //      Execute : Using standard rFuel.properties constring
    //      Output  : to outpath + outfile.
    //    properties:
    //          database            db name
    //          outfile             name of the files to produce
    //          outpath             directory to write them to
    //          fieldterminator     usually <FT>
    //          filesize            maximum size of the output files
    //          batchsize           SQL batch fetch size
    //          sudo                true / false
    //          password            sudo password
    //          command             SQL command to generate the list
    // ----------------------------------------------------------------

    private static final String newLine = System.getProperty("line.separator");
    private static ArrayList<String> filenames  = new ArrayList<>();
    private static ArrayList<String> filelength = new ArrayList<>();
    private static ArrayList<String> filesizes  = new ArrayList<>();
    private static String[] cols;
    private static String curfile = "";         // current file name
    private static String outfile = "";
    private static String outpath = "";
    private static String password= "";
    private static String fileext = "";
    private static String FT      = "";
    private static String readyFile = "READY.txt";
    private static final String pass = "<<PASS>>";
    private static final String fail = "<<FAIL>>";
    private static String buildDir= "data/sqlout/";
    private static boolean sudo  = false;
    private static boolean verbose=true;
    private static boolean multiCmd = false;
    private static int filesize  = 1499999980;                 //  ~ 1.5 GB
    private static int batchsize = 5000;
    private static int showAt    = 10000;
    private static int fileNbr   = 0;
    private static int nbrLines  = 0;
    private static BufferedWriter dataWriter = null;
    private static ResultSet rs   = null;
    private static Statement stmt = null;
    private static File newFile = null;

    public static void main(String[] args) throws SQLException, IOException {
        if (NamedCommon.BaseCamp.contains("/home/andy")) NamedCommon.BaseCamp = NamedCommon.DevCentre;
        if (args.length == 0) {
            uCommons.uSendMessage("Usage:  SelectToCSV.sh {fully qualified path to file}");
            return;
        }
        if (args.length > 1) {
            if (args[1].toLowerCase().contains("mode")) {
                String mode = uCommons.FieldOf(args[1], "=", 2);
                if (mode.toLowerCase().equals("background")) verbose = false;
            }
        }

        NamedCommon.Reset();
        uCipher.SetAES(false, "", "");
        Properties runProps = uCommons.LoadProperties("rFuel.properties");
        if (NamedCommon.ZERROR) return;
        uCommons.SetCommons(runProps);
        uCipher.SetAES(NamedCommon.AES, NamedCommon.secret, NamedCommon.salt);

        if (!NamedCommon.tConnected) {
            if (verbose) uCommons.uSendMessage("Intialise TargetDB -----------------------------------");
            boolean okay = SqlCommands.ConnectSQL();
            if (!okay) return;
            NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
            NamedCommon.uCon.setClientInfo("responseBuffering", "adaptive");
            if (NamedCommon.uCon == null) {
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "ERROR: jdbc connection failure.";
                return;
            }
            if (verbose) System.out.println();
        }

        if (verbose) uCommons.uSendMessage("Processing: ------------------------------------------");
        multiCmd = false;
        if (!args[0].startsWith(NamedCommon.BaseCamp)) {
            if (!NamedCommon.BaseCamp.endsWith(NamedCommon.slash)) {
                args[0] = NamedCommon.BaseCamp + NamedCommon.slash + args[0];
            } else {
                args[0] = NamedCommon.BaseCamp + args[0];
            }
        }
        if (verbose) uCommons.uSendMessage("   ) Getting properties   "+ args[0]);
        ProcessSqlOutput(args[0]);
        NamedCommon.uCon.close();
    }

    public static boolean ProcessSqlOutput(String fqfn) throws SQLException, IOException {
        //
        //          MUST be a fully qualified file name !!!!
        //
        Properties props = uCommons.LoadProperties(fqfn);
        outfile     = uCommons.GetValue(props, "outfile",           "");
        outpath     = uCommons.GetValue(props, "outpath",           "");
        FT          = uCommons.GetValue(props, "fieldterminator",   ",");
        readyFile   = uCommons.GetValue(props, "ready.file",   readyFile);
        try {
            filesize = Integer.parseInt(uCommons.GetValue(props, "filesize", String.valueOf(filesize)));
        } catch (NumberFormatException nfe) {
            //
        }
        try {
            batchsize = Integer.parseInt(uCommons.GetValue(props, "batchsize", String.valueOf(batchsize)));
        } catch (NumberFormatException nfe) {
            //
        }
        try {
            showAt = Integer.parseInt(uCommons.GetValue(props, "show.at", String.valueOf(showAt)));
        } catch (NumberFormatException nfe) {
            //
        }
        //
        // sudo and password are for the mv command - requires elevated permissions
        //
        sudo        = uCommons.GetValue(props, "sudo",              "false").toLowerCase().equals("true");
        password    = uCommons.GetValue(props, "password",          "");
        String cmd  = uCommons.GetValue(props, "command",           "");
        if (outfile.equals("")) return false;
        if (outpath.equals("")) return false;

        if (!buildDir.startsWith(NamedCommon.BaseCamp)) {
            buildDir = NamedCommon.BaseCamp + NamedCommon.slash + buildDir;
        }
        if (!outpath.startsWith(NamedCommon.BaseCamp)) {
            outpath = NamedCommon.BaseCamp + NamedCommon.slash + outpath;
        }
        String[] dotParts = outfile.split("\\.");
        if (dotParts.length > 0) {
            fileext = "." + dotParts[dotParts.length - 1];
            outfile = outfile.replace(fileext, "");
        } else {
            fileext = ".csv";
        }

        boolean okay = false;
        if (cmd.equals("")) {
            if (verbose) {
                uCommons.uSendMessage(fqfn + "  does not have a command to execute.");
            } else {
                uCommons.uSendMessage(fail);
            }
            return false;
        } else {
            //  command can be either a full select statement on one line OR  'some-file.sql'
            String[] commands = cmd.split("\\,");
            if (commands.length == 1) {
                multiCmd = false;
                if (cmd.toLowerCase().endsWith(".sql")) {
                    fqfn = fqfn.replace('\\', '/');
                    String[] jArr = fqfn.split("/");
                    jArr[jArr.length - 1] = cmd;
                    String fqsn = String.join("/", jArr);
                    cmd = new String(Files.readAllBytes(Paths.get(fqsn)));
                    fqsn = "";
                    jArr = null;
                    //  run a single query
                    if (verbose) uCommons.uSendMessage("   ) Getting command(s) from " + fqsn);
                    okay = ExecuteSQL(cmd);
                    if (okay) okay = GenerateOutput();
                }
                // will assume the command is a one line statement.
            } else {
                //  Sometimes, one query produces too much data for SQL to handle,
                //  breaking it into multiple queries allows us to handle big data.
                //
                // CAVEAT: Each command MUST produce output with the same metadata (columns)  !!!!
                //
                if (verbose) uCommons.uSendMessage("   ) Running " + commands.length + " queries to obtain output.");
                fqfn = fqfn.replace('\\', '/');
                String[] jArr = fqfn.split("/");
                jArr[jArr.length - 1] = "";

                for (int i=0 ; i < commands.length ; i++) {
                    cmd = commands[i];
                    if (cmd.toLowerCase().endsWith(".sql")) {
                        fqfn = fqfn.replace('\\', '/');
                        jArr[jArr.length - 1] = cmd;
                        String fqsn = String.join("/", jArr);
                        cmd = new String(Files.readAllBytes(Paths.get(fqsn)));
                        okay = ExecuteSQL(cmd);
                        if (okay) okay = GenerateOutput();
                    } else {
                        if (verbose) uCommons.uSendMessage("   ) Using command[" + i + "]");
                        okay = ExecuteSQL(cmd);
                        if (okay) okay = GenerateOutput();
                    }
                    multiCmd = true;    // do not delete old files. do not reset fileNbr
                }
            }
        }
        if (okay) okay = MoveOutput();
        return okay;
    }

    private static boolean ExecuteSQL(String cmd) throws SQLClientInfoException {
        if (!NamedCommon.tConnected) {
            if (verbose) System.out.println(" ");
            if (verbose) uCommons.uSendMessage("Connecting to SQL host -------------------------------");
            boolean okay = SqlCommands.ConnectSQL();
            if (!okay) return false;
            NamedCommon.uCon = ConnectionPool.jdbcPool.get(0);
            if (NamedCommon.uCon == null) return false;
            NamedCommon.uCon.setClientInfo("responseBuffering", "adaptive");
        }

        if (verbose) System.out.println(" ");
        if (verbose) uCommons.uSendMessage("Executing SQL ----------------------------------------");

        try {
            stmt = NamedCommon.uCon.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(cmd);
        } catch (SQLException e) {
            uCommons.uSendMessage(e.getMessage());
            return false;
        }
        return true;
    }

    private static boolean GenerateOutput() throws IOException, SQLException {
        if (verbose) System.out.println(" ");
        if (verbose) uCommons.uSendMessage("Generate Output --------------------------------------");

        if (cols == null) {
            GetAllColumns();
            if (cols == null) return false;
        }

        // clear the old file ----------------------------------------

        String loadDir = buildDir;
        if (!loadDir.startsWith(NamedCommon.BaseCamp)) {
            if (!NamedCommon.BaseCamp.endsWith(NamedCommon.slash)) {
                loadDir = NamedCommon.BaseCamp + NamedCommon.slash + loadDir;
            } else {
                loadDir = NamedCommon.BaseCamp + loadDir;
            }
        }
        if (!multiCmd) {
            // if multiCmd, the first cmd will clear old data.
            String dumper = loadDir + outfile;
            File directory = new File(loadDir);
            File[] datFilesArray = directory.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.contains(outfile);
                }
            });
            if (verbose)
                uCommons.uSendMessage("Remove [" + datFilesArray.length + "] old " + dumper + "*" + fileext + " files.");
            for (int f = 0; f < datFilesArray.length; f++) {
                uCommons.DeleteFile(datFilesArray[f].toString());
            }
            if (verbose) uCommons.uSendMessage("Generate new output to " + dumper + "*" + fileext);
        }

        // create the new file ----------------------------------------

        GetNewWriter();
        int cnt=0, cLen = cols.length;
        rs.setFetchSize(batchsize);
        String crlf="\r\n", value;
        StringBuilder sb = new StringBuilder();
        StringBuilder ln = new StringBuilder();

        int lx = 0;
        sb.setLength(0);
        ln.setLength(0);

        while (rs.next()) {
            if (cnt > 0) { sb.append(crlf); lx ++; }
            cnt++;
            if ((cnt % batchsize) == 0) { FlushData(sb.toString()); sb.setLength(0); }
            if ((cnt % showAt) == 0 && verbose) uCommons.uSendMessage(uCommons.oconvM(String.valueOf(cnt), "MD0,"));

            value = rs.getString(1);
            ln.append(value);                       // must do here to append FT between each value
            for (int i=2 ; i <= cLen ; i++) {
                value = rs.getString(i);
                if (value == null) value = "";
                ln.append(FT);
                ln.append(value);
            }

            if ((lx + ln.length()) > filesize) { FlushData(sb.toString()); sb.setLength(0); GetNewWriter(); lx=0;}
            nbrLines++;
            sb.append(ln);
            lx += ln.length();
            ln.setLength(0);
        }

        if (sb.length() > 0) FlushData(sb.toString());
        dataWriter.flush();
        dataWriter.close();
        dataWriter = null;
        stmt.close();
        rs.close();
        filenames.add(curfile);
        filelength.add(String.valueOf(nbrLines));
        newFile = new File(buildDir + curfile);
        filesizes.add(String.valueOf(newFile.length()));
        newFile = null;
        if (verbose) uCommons.uSendMessage("Data is now available.");
        if (verbose) uCommons.uSendMessage(uCommons.oconvM(String.valueOf(cnt), "MD0,") + " rows were generated.");
        if (verbose) System.out.println();
        return true;
    }

    private static void FlushData(String block) throws IOException {
        dataWriter.write(block);
    }
    
    private static void GetNewWriter() throws IOException {
        if (dataWriter != null) {
            filenames.add(curfile);
            filelength.add(String.valueOf(nbrLines));
            newFile = new File(buildDir + curfile);
            filesizes.add(String.valueOf(newFile.length()));
            newFile = null;
            curfile  = "newfile";
            dataWriter.flush();
            dataWriter.close();
            dataWriter = null;
        }
        String dumper = outfile;
        dumper = dumper + String.valueOf(fileNbr);
        dataWriter = uCommons.CreateFile(buildDir, dumper, fileext);
        if (dataWriter == null) throw new FileNotFoundException("Please check the directory tree & permissions.");
        curfile = dumper+fileext;
        fileNbr++;
        nbrLines = 0;
    }

    private static void GetAllColumns(){
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int nbrCols = metaData.getColumnCount();
            cols = new String[nbrCols];
            String cName;
            for (int i=1 ; i <= nbrCols ; i++) {
                cols[i-1] = metaData.getColumnName(i);
            }
        } catch (SQLException e) {
            uCommons.uSendMessage(fail);
            uCommons.uSendMessage(e.getMessage());
        }
    }

    private static boolean MoveOutput() {
        // outpath  :   e.g. /upl/data/external/

        if (verbose) System.out.println(" ");
        if (verbose) uCommons.uSendMessage("Output file statistics --------------------- ");
        int totRows=0;
        StringBuilder sbOut = new StringBuilder();
        String line = "";
        for (int i=0; i < filenames.size(); i++) {
            line = filenames.get(i) + "  " + filelength.get(i) + "  " + filesizes.get(i);
            sbOut.append(line + newLine);
            uCommons.uSendMessage(line);
            totRows += Integer.parseInt(filelength.get(i));
        }
        line = "";
        uCommons.WriteDiskRecord(buildDir + outfile + ".txt", sbOut.toString());
        sbOut.setLength(0);
        sbOut = null;

        if (verbose) {
            uCommons.uSendMessage("Total number of rows output ---------------- " + uCommons.oconvM(String.valueOf(totRows), "MD0,"));
        } else {
            uCommons.uSendMessage("TOTAL  " + uCommons.oconvM(String.valueOf(totRows), "MD0,"));
        }

        String loadDir = buildDir;

        String dumper = loadDir + outfile+"*";
        String mvCmd = "mv " + dumper + " " + outpath;

        if (verbose) uCommons.uSendMessage("Moving data ------------------------------------------");
        if (verbose) uCommons.uSendMessage(mvCmd);
        String[] mv;
        if (sudo) mvCmd = "sudo " + mvCmd;
        mv = new String[]{"/bin/bash", "-c", mvCmd};

        boolean okay = true;

        try {
            ProcessBuilder pb = new ProcessBuilder(mv);
            Process process = pb.start();
            if (sudo) {
                try (OutputStream os = process.getOutputStream()) {
                    os.write((password + "\n\n").getBytes());
                    os.flush();
                    os.close();
                }
            }
            process.waitFor();

            // Read the output and error streams

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            BufferedReader errors = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String pfx = "   ) ";
            try {
                while ((line = reader.readLine()) != null) {System.out.println(pfx + line);}
                while ((line = errors.readLine()) != null) {System.err.println(pfx + line);}
            } catch (IOException e) {
                uCommons.uSendMessage("Reader Error: " + e.getMessage());
            }
        } catch (IOException | InterruptedException | IllegalStateException e) {
            uCommons.uSendMessage("Error: " + e.getMessage());
            okay = false;
        }

        if (okay) {
            uCommons.uSendMessage(pass);
            if (verbose) {
                uCommons.uSendMessage("Process complete: data has been moved to;");
                uCommons.uSendMessage(outpath + outfile);
            }
            uCommons.WriteDiskRecord(outpath + readyFile, outfile+"*" + fileext);
        } else {
            uCommons.uSendMessage(fail);
            if (verbose) {
                uCommons.uSendMessage("   ) mv command failed: ");
                uCommons.uSendMessage("   ) Data is at " + dumper);
            }
            uCommons.WriteDiskRecord(buildDir + readyFile, outfile+"*" + fileext);
            return false;
        }

        return okay;
    }
}
