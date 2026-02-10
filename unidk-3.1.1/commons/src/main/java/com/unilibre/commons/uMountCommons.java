package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED  */

import java.io.*;


public class uMountCommons {

    public static BufferedWriter mntRequests = null;
    public static BufferedReader mntResponse = null;
    public static int timeout = 0;
    private static String extIn = ".temp";
    public static String extOut = ".send";
    public static final String slash = File.separator;

    public static String writeReq(String uuid, String inData) {
        String outVal = "";
        switch (NamedCommon.protocol) {
            case "real":
                outVal = u2Commons.MetaBasic(inData);
                break;
            case "u2mount":
                String fqname = NamedCommon.InMount + slash + uuid;
                if (mntRequests == null) AttachUniReader(NamedCommon.InMount, uuid);
                try {
                    mntRequests.write(inData);
                    mntRequests.newLine();
                    mntRequests.flush();
                    mntRequests.close();
                    mntRequests = null;
                } catch (IOException e) {
                    NamedCommon.ZERROR = true;
                    NamedCommon.Zmessage = "Write failure to " + fqname + extIn + " >> " + e.getMessage();
                    return "<<FAIL>>";
                }
                uCommons.RenameFile(fqname + extIn, fqname + extOut);
                outVal =  "<<PASS>>";
                break;
        }
        return outVal;
    }

    public static String readResp(String uuid) {
        String reply = "";
        String fqname = NamedCommon.OutMount + slash + uuid + extOut;
        boolean done = false;
        if (NamedCommon.debugging) uCommons.uSendMessage("Reading " + uuid);
        int fails = 0;
        while (!done) {
            reply = ReadRespRecord(fqname);
            if (reply.length() > 0) {
                if (mntResponse != null) {
                    try {
                        mntResponse.close();
                        mntResponse = null;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                File file = new File(fqname);
                if (file.delete()) {
                    done = true;
                } else {
                    fails++;
                    if (fails > 99) {
                        NamedCommon.ZERROR = true;
                        NamedCommon.Zmessage = "Cannot delete " + fqname;
                        done = true;
                    }
                }
            } else {
                fails++;
                if (fails > 100) {
                    done = true;
                }
                uCommons.Sleep(timeout);
            }
        }
        return reply;
    }

    private static String ReadRespRecord(String fqname) {
        String ans = "";
        String line = "";
        mntResponse = null;
        FileReader fr = null;
        BufferedReader br;
        boolean failed = true;
        while (failed) {
            try {
                fr = new FileReader(fqname);
                failed = false;
            } catch (FileNotFoundException e) {
                if (NamedCommon.debugging) uCommons.uSendMessage("File not ready. Wait a moment");
                uCommons.Sleep(0);
                timeout++;
            }
        }
        if (!failed) {
            //mntResponse = new BufferedReader(fr);
            br = new BufferedReader(fr);
            if (NamedCommon.debugging) uCommons.uSendMessage("Got it");
            line = "";
            while ((line) != null) {
                try {
                    //        line = mntResponse.readLine();
                    line = br.readLine();
                } catch (IOException e) {
                    line = null;
                }
                if (line != null) ans += line + "\n";
            }
            try {
                //mntResponse.close();
                br.close();
                fr.close();
                mntResponse = null;
            } catch (IOException e) {
                uCommons.uSendMessage("File Close FAIL on " + fqname);
                uCommons.uSendMessage(e.getMessage());
            }
        }
        return ans;
    }

    public static BufferedWriter AttachUniReader(String useMount, String uuid) {
        mntRequests = null;
        mntRequests = uCommons.CreateFile(useMount, uuid + extIn, "");
        if (mntRequests == null) {
            NamedCommon.ZERROR = true;
            NamedCommon.Zmessage = "Cannot attach to mount point for " + useMount;
        }
        return mntRequests;
    }

    public static BufferedReader AttachUniWriter(String useMount, String uuid) {
        mntResponse = null;
        String file = useMount + slash + uuid + extOut;
        try {
            mntResponse = new BufferedReader(new java.io.FileReader(file));
        } catch (FileNotFoundException e) {
            // do nothing here.
        }
        return mntResponse;
    }

}
