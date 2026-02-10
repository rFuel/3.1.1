package com.unilibre.dataworks;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

public class HDFSLoader {

//    private static int nbrFiles;
//    public static Runtime rt = Runtime.getRuntime();
//    public static String s;
//    public static String spcr = "------------------------------------------------";
//    public static String matchStr = "dat";
//    public static File dir = null;
//    private static ArrayList<String> DeleteFiles = new ArrayList<>();
//    private static int bufSize = 131072;
//    private static int bufChkr = 131072 * 2;
//    private static Path paf;
//
//    private static void usage() {
//        System.out.println("Usage : HDFSLoader(<source file>,<target file>)");
//        System.exit(1);
//    }
//
//    private static void printAndExit(String str) {
//        System.err.println(str);
//        System.exit(1);
//    }
//
//    public static void MoveFiles() {
//        uCommons.uSendMessage("HDFS.MoveFiles looking for " + matchStr + " in " + MoveData.ddir);
//        DeleteFiles.clear();
//        MoveData.ok2delete = true;
//        String theFile = "";
//        nbrFiles = MoveData.ListOfFiles.length;
//        for (int i = 0; i < nbrFiles; i++) {
//            try {
//                theFile = MoveData.ListOfFiles[i];
//                MoveData.baseFle = theFile;             // fully qualified name
//                Move(theFile);
//                if (MoveData.ok2delete) {
//                    DeleteFiles.add(theFile);
//                } else {
//                    MoveData.BadFiles.add(theFile);
//                }
//            } catch (IOException e) {
//                uCommons.uSendMessage("hdfsWrite(): " + e.getMessage());
//                e.printStackTrace();
//                MoveData.BadFiles.add(theFile);
//            }
//        }
//        RunControl.RemoveInsertedData();
//    }
//
//    private static void Move(String src) throws IOException {
//        if (src.equals("") || src.equals(null)) usage();
//        String srcFile = RunControl.GetFileOnly(src).replaceAll("\\[", "").replaceAll("\\]", "");
//        String fData = uCommons.ReadDiskRecord(src);
//        String[] lines = fData.split("\r?\n");
//        paf = new Path(MoveData.hdfsPath + srcFile);
//        int nbrLines = lines.length;
//        if (MoveData.hdFS) {
//            List<String> rows = new ArrayList<String>();
//            for (int i = 0; i < nbrLines; i++) {
//                rows.add(lines[i]);
//                if (rows.size() >= bufChkr) {
//                    writeBuffered(rows, bufSize);
//                    System.out.print(i + " ");
//                    rows.clear();
//                }
//            }
//            writeBuffered(rows, bufSize);
//        } else {
//            String dstPath = MoveData.datPath;
//            DoSimpleMove(srcFile, dstPath, src);
//        }
//        RunControl.BulkLoader(srcFile);
//    }
//
//    private static void writeBuffered(List<String> rows, int bufSize) throws IOException {
//
//        /* ------------------------------------------------------------------ */
//
//        try (FSDataOutputStream fsDataOutputStream = MoveData.fs.create(paf)) {
//            Writer writer = new OutputStreamWriter(fsDataOutputStream);
//            BufferedWriter bufferedWriter = new BufferedWriter(writer, bufSize);
//            System.out.println("Start    writing data");
//            write(rows, bufferedWriter);
//            System.out.println("Finished writing data");
//        }
//    }
//
//    private static void write(List<String> records, Writer writer) throws IOException {
//        for (String record : records) {
//            writer.write(record);
//        }
//        writer.flush();
//    }
//
//    private static void DoSimpleMove(String srcFile, String dstPath, String src) {
//        String rec = uCommons.ReadDiskRecord(src);
//        //uCommons.CreateFile(dstPath, srcFile);
//        try {
//            BufferedWriter bWriter = new BufferedWriter(new FileWriter(dstPath + srcFile));
//            bWriter.write(rec);
//            bWriter.flush();
//            bWriter.close();
//        } catch (IOException e) {
//            uCommons.eMessage = "[FATAL]   Cannot close " + dstPath + src;
//            uCommons.eMessage += "\n" + e.getMessage();
//            uCommons.eMessage += "\n" + MessageProtocol.messageText;
//            uCommons.ReportRunError(uCommons.eMessage);
//            uCommons.uSendMessage(uCommons.eMessage);
//            System.exit(0);
//        }
//    }
//
//    private static void DoHDFSMove(String srcFile, String dstFile) throws IOException {
//        // HDFS deals with Path
//        Path inpFile = new Path(srcFile);
//        Path outFile = new Path(dstFile);
//        // Check if input/output are valid
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//        if (!fs.exists(inpFile)) printAndExit("Input file not found");
//        if (!fs.isFile(inpFile)) printAndExit("Input should be a file");
//        if (fs.exists(outFile)) printAndExit("Output already exists");
//        // Read from and write to new file
//        FSDataInputStream in = fs.open(inpFile);
//        FSDataOutputStream out = fs.create(outFile);
//        byte buffer[] = new byte[256];
//        try {
//            int bytesRead = 0;
//            while ((bytesRead = in.read(buffer)) > 0) {
//                out.write(buffer, 0, bytesRead);
//            }
//        } catch (IOException e) {
//            System.out.println("Error while copying file");
//        } finally {
//            in.close();
//            out.close();
//        }
//    }
}