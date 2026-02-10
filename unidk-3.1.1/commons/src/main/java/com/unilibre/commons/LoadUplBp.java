package com.unilibre.commons;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */


import asjava.uniclientlibs.UniDynArray;
import asjava.uniclientlibs.UniString;
import asjava.uniobjects.UniFile;
import asjava.uniobjects.UniFileException;
import com.northgateis.reality.rsc.RSC;
import com.northgateis.reality.rsc.RSCException;
import com.unilibre.cipher.uCipher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static com.unilibre.commons.u2Commons.*;
import static com.unilibre.commons.u2Commons.uniExec;

public class LoadUplBp {
    public static UniFile uplHANDLE;
    public static String resp;
    private static String compiler="uDO ";
    private static int[] loadStats;
    private static ArrayList<String> realCplList = new ArrayList<>();


    public static String LoadCustPgms() {
        String status = "<<PASS>>", ext = "";
        boolean proceed = false;
        if (NamedCommon.sConnected) SourceDB.DisconnectSourceDB();
        SourceDB.ConnectSourceDB(); // so they can load into any account
        String bpFile = APImsg.APIget("file");
        if (!bpFile.trim().equals("")) {
            if (!custFiles.contains(bpFile)) custFiles += " "+bpFile;
            status = OpenCreate(bpFile);
            if (status.contains("<<FAIL>>")) {
                proceed = false;
            } else {
                uplBP = uplHANDLE;
                uCommons.uSendMessage("   .) "+bpFile+" is open");
                proceed = true;
            }
        } else {
            NamedCommon.ZERROR=true;
            NamedCommon.Zmessage="Your message did not supply a BP file name.";
            status = "<<FAIL>> "+NamedCommon.Zmessage;
            proceed=false;
        }
        if (proceed) {
            ext = ".cbp";
            compiler = APImsg.APIget("compiler");
            uCommons.uSendMessage("   .) Looking for *"+ ext + " in  "+ NamedCommon.gmods);
            String[] pgms = uCommons.ReadDiskFiles(NamedCommon.gmods, ext);
            if (pgms.length < 1) {
                uCommons.uSendMessage("      nothing to load");
                status = "<<PASS>> nothing to load.";
            } else {
                if (pgms[0].contains("<<FAIL>>")) {
                    status = pgms[0] + ". See properties for 'gmods'";
                    proceed = false;
                }
            }
            if (proceed && pgms.length > 0) proceed = (!LoadThesePrograms(pgms, ext).contains("<<FAIL>>"));
        }
        return status;
    }

    public static String LoadPgms() {
        String ans="";
        switch (NamedCommon.protocol) {
            case "u2cs":
                ans = u2cs_LoadPgms();
                break;
            case "real":
                ans = real_LoadPgms();
                break;
            case "rmi.u2cs":
                System.out.println("rmi:  is being developed - not ready yet");
        }
        return ans;
    }

    private static String real_LoadPgms() {
        RSC sub = new RSC(NamedCommon.rcon, "SLBP");
        String outVal="", success="{EOX}{ok}";
        try {
            sub.getParam(1).setValue("");
            sub.getParam(2).setValue("BP.UPL");
            sub.getParam(3).setValue("STOP");
            sub.getParam(4).setValue("");
            sub.execute();
            outVal = sub.getParam(1).getString();
            if (!outVal.equals(success)) NamedCommon.Zmessage = "ERROR: "+outVal;
        } catch (RSCException e) {
            NamedCommon.Zmessage = e.toString().replaceAll("\\r?\\n", " ").trim();
        }
        if (!NamedCommon.Zmessage.equals("")) {
            NamedCommon.ZERROR = true;
            uCommons.uSendMessage("RSC ERROR with initialisation");
            outVal = NamedCommon.Zmessage;
            return "<<FAIL>>";
        }
        sub = null;
        String status="", ext;
        loadStats = new int[4];
        loadStats[0]=0;
        loadStats[1]=0;
        loadStats[2]=0;
        loadStats[3]=0;
        String[] extn = new String[6];
        extn[0] = ".ins";
        extn[1] = ".ctl";
        extn[2] = ".gct";
        extn[3] = "uDO.bas";
        extn[4] = ".bas";
        extn[5] = ".csv";

        for (int p=0; p<extn.length; p++) {
            ext = extn[p];
            uCommons.uSendMessage("--------- Looking for *"+ ext + " in  "+ NamedCommon.gmods + "  ------------------------------");
            loadStats[0] = 0;
            loadStats[1] = 0;
            loadStats[2] = 0;
            loadStats[3] = 0;
            status = real_LoadPrograms(ext);
            if (loadStats[1] > 1) {
                uCommons.uSendMessage("<<LOAD>>  " + loadStats[1]);
                uCommons.uSendMessage("<<PASS>>  " + loadStats[0]);
                uCommons.uSendMessage("<<SKIP>>  " + loadStats[2]);
                uCommons.uSendMessage("<<FAIL>>  " + loadStats[3]);
            }
            if (NamedCommon.ZERROR) return "<<FAIL>>";
        }

        String cSTR = "{EXE}{exec=uLOAD.RETURN.CODES}";
        cSTR += "{file=}";
        cSTR += "{atr=}";
        cSTR += "{mv=}";
        cSTR += "{sv=}";
        String junk = u2Commons.MetaBasic(cSTR);
        return status;
    }

    private static String u2cs_LoadPgms() {
        boolean proceed = false;
        String status = "<<PASS>>", ext = "";
        if (!NamedCommon.IsAvailableU2) {
            if (SourceDB.ConnectSourceDB().contains("<<PASS>>")) {
                proceed = true;
            } else {
                status = "<<FAIL>> - SourceDB is unavailable";
            }
        } else {
            proceed = true;
        }
        if (proceed && NamedCommon.databaseType.equals("UNIVERSE")) {
            u2Commons.SetManaged(true);
            UniString usRec = uRead(NamedCommon.VOC, "LOGIM");
            u2Commons.SetManaged(false);
            if (usRec == null) {
                u2Commons.uWriter(NamedCommon.VOC, "LOGIN", "PA\nLONGNAMES ON\nPTERM -CASE NOINVERT");
            }
            u2Commons.SetManaged(true);
            DeleteCreate("&SAVEDLISTS&");
            DeleteCreate("&PH&");
            u2Commons.SetManaged(false);
        }
        uCommons.uSendMessage("OpenCreate Type 19 files: BP.UPL UPL.INSERTS OBJ.UPL uLOG uLISTS");
        if (proceed) {
            status = OpenCreate("BP.UPL");
            if (status.contains("<<FAIL>>")) {
                proceed = false;
            } else {
                uplBP = uOpen("BP.UPL");
                uCommons.uSendMessage("   .) BP.UPL is open");
                uWriter(uplBP, "STOP", "stop");
            }
        }
        if (proceed) {
            status = OpenCreate("UPL.INSERTS");
            if (status.contains("<<FAIL>>")) {
                proceed = false;
            } else {
                uplInserts = uOpen("UPL.INSERTS");;
                uCommons.uSendMessage("   .) UPL.INSERTS is open");
            }
        }
        if (proceed) {
            status = OpenCreate("OBJ.UPL");
            if (status.contains("<<FAIL>>")) {
                proceed = false;
            } else {
                uplOBJ = uOpen("OBJ.UPL");;
                uCommons.uSendMessage("   .) OBJ.UPL is open");
            }
        }
        if (proceed) {
            status = OpenCreate("uLOG");
            if (status.contains("<<FAIL>>")) proceed = false;
        }
        if (proceed) {
            status = OpenCreate("uLISTS");
            if (status.contains("<<FAIL>>")) proceed = false;
        }
        uCommons.uSendMessage("Done.");
        if (!proceed) uCommons.uSendMessage("Errors have occurred.");

        String[] pgms = new String[]{};

        if (proceed) {
            ext = ".ins";
            uCommons.uSendMessage("   .) Looking for *"+ ext + " in  "+ NamedCommon.gmods);
            pgms = uCommons.ReadDiskFiles(NamedCommon.gmods, ext);
            if (pgms.length < 1) {
                uCommons.uSendMessage("      nothing to load");
                status = "<<PASS>> nothing to load.";
            } else {
                if (pgms[0].contains("<<FAIL>>")) {
                    status = pgms[0] + ". See properties for 'gmods'";
                    proceed = false;
                }
            }
            if (proceed && pgms.length > 0) proceed = (!LoadThesePrograms(pgms, ext).contains("<<FAIL>>"));
        }

        if (proceed) {
            ext = "uDO.bas";
            uCommons.uSendMessage("   .) Looking for "+ ext + " in  "+ NamedCommon.gmods);
            pgms = uCommons.ReadDiskFiles(NamedCommon.gmods, ext);
            if (pgms.length < 1) {
                uCommons.uSendMessage("      nothing to load");
                status = "<<PASS>> nothing to load.";
            } else {
                if (pgms[0].contains("<<FAIL>>")) {
                    status = pgms[0] + ". See properties for 'gmods'";
                    proceed = false;
                }
            }
            if (proceed && pgms.length > 0) proceed = (!LoadThesePrograms(pgms, ext).contains("<<FAIL>>"));
        }

        if (proceed) {
            ext = ".gct";
            uCommons.uSendMessage("   .) Looking for *"+ ext + " in  "+ NamedCommon.gmods);
            pgms = uCommons.ReadDiskFiles(NamedCommon.gmods, ext);
            if (pgms.length < 1) {
                uCommons.uSendMessage("      nothing to load");
                status = "<<PASS>> nothing to load.";
            } else {
                if (pgms[0].contains("<<FAIL>>")) {
                    status = pgms[0] + ". See properties for 'gmods'";
                    proceed = false;
                }
            }
            if (proceed && pgms.length > 0) proceed = (!LoadThesePrograms(pgms, ext).contains("<<FAIL>>"));
        }

        if (proceed) {
            ext = ".bas";
            uCommons.uSendMessage("   .) Looking for *"+ ext + " in  "+ NamedCommon.gmods);
            pgms = uCommons.ReadDiskFiles(NamedCommon.gmods, ext);
            if (pgms.length < 1) {
                uCommons.uSendMessage("      nothing to load");
                status = "<<PASS>> nothing to load.";
            } else {
                if (pgms[0].contains("<<FAIL>>")) {
                    status = pgms[0] + ". See properties for 'gmods'";
                    proceed = false;
                }
            }
            if (proceed && pgms.length > 0) {
                proceed = (!LoadThesePrograms(pgms, ext).contains("<<FAIL>>"));
                uCommons.uSendMessage("      .) Pass: "+loadStats[0]);
                uCommons.uSendMessage("      .) Fail: "+loadStats[1]);
                uCommons.uSendMessage("      .) Skip: "+loadStats[2]);
            }
        }

        if (proceed) {
            ext = ".ctl";
            uCommons.uSendMessage("   .) Looking for *"+ ext + " in  "+ NamedCommon.gmods);
            pgms = uCommons.ReadDiskFiles(NamedCommon.gmods, ext);
            if (pgms.length < 1) {
                uCommons.uSendMessage("      nothing to load");
                status = "<<PASS>> nothing to load.";
            }
            if (proceed && pgms.length > 0) proceed = (!LoadThesePrograms(pgms, ext).contains("<<FAIL>>"));
        }
        return status;
    }

    private static String real_LoadPrograms(String ext) {
        String status = "<<PASS>>", cstatus = "<<PASS>>";
        boolean proceed, loadit = true;

        String[] pgms = uCommons.ReadDiskFiles(NamedCommon.gmods, ext);
        int nbrItems = pgms.length;
        int rHash = Integer.valueOf(String.valueOf(nbrItems).length());
        if (nbrItems < 1) {
            uCommons.uSendMessage("      nothing to load");
            status = "<<PASS>> nothing to load.";
        } else if (pgms[0].contains("<<FAIL>>")) {
                status = pgms[0] + ". See properties for 'gmods'";
                return status;
        } else {
            uCommons.uSendMessage("   .) Loading " + nbrItems + " items" );
        }

        if (NamedCommon.BaseCamp.contains("/")) {
            NamedCommon.slash = "/";
        } else {
            NamedCommon.slash = "\\";
        }
        String fqFileName, cStr;
        BufferedWriter bWriter = null;
        String key = "", atID, record, compile, catalog, decat, inVal, rCnt;
        String chker = "", skip = "_U", spc="   ";
        for (int ii = 0; ii < nbrItems; ii++) {
            spc = "   ";
            inVal = "";
            key = pgms[ii];
            fqFileName = NamedCommon.gmods + key;
            record = uCommons.ReadDiskRecord(fqFileName);
            while (record.endsWith("\n")) {
                record = record.substring(0, record.length() - 1);
            }
            if (record.startsWith("ENC(")) {
                record = record.substring(4, record.length());
                record = record.substring(0, record.length() - 1);
                record = uCipher.Decrypt(record);
            }
            while (record.contains("\n")) {
                record = record.replaceAll("\\r?\\n", "[[fm]]");
            }
            atID = key.substring(0, key.indexOf(ext));
            if (key.equals("uDO.bas")) atID = "uDO";
            if (atID.contains("\\/")) loadit = false;
            if (atID.endsWith(skip)) loadit = false;
            status = "";
            if (loadit) {
                if (fqFileName.endsWith(".ctl")) {
                    inVal = "LOAD BP.UPL " + atID;
                    status = real_LoadItem("BP.UPL", atID, record);
                }
                if (fqFileName.endsWith(".ins")) {
                    inVal = "LOAD UPL.INSERTS " + atID;
                    status = real_LoadItem("UPL.INSERTS", atID, record);
                }
                if (fqFileName.endsWith(".bas")) {
                    if (atID.equals("uDO") && ext.startsWith(atID)) {
                        inVal = "LOAD BP.UPL " + atID;
                        status = real_LoadItem("BP.UPL", atID, record);
                        // --------------------------------------------------------
                        //  SLBP will LOAD, DECATALOG, BASIC and CATALOG uDO
                        // --------------------------------------------------------
                    } else {
                        if (atID.equals("uDO")) {
                            status = "<<SKIP>>   " + atID;
                        } else {
                            inVal = "LOAD & uDO BP.UPL " + atID;
                            status = real_LoadItem("BP.UPL", "bas."+atID, record);
                            if (status.toLowerCase().equals("ok")) status = "<<PASS>>";
                            // --------------------------------------------------------
                            //  SLBP will LOAD, DECATALOG and uDO {bas.atID}
                            // --------------------------------------------------------
                        }
                    }
                }
                if (fqFileName.endsWith(".csv")) {
                    if (!atID.endsWith(".csv")) atID += ".csv";
                    inVal = "LOAD BP.UPL " + atID;
                    status = real_LoadItem("BP.UPL", atID, record);
                }
            } else {
                status = "<<SKIP>> " + atID;
                loadit = true;
            }

            if (status.contains("<<PASS>>")) loadStats[0]++;
            if (status.contains("<<SKIP>>")) loadStats[2]++;
            if (status.contains("<<FAIL>>")) {
                loadStats[3]++;
                spc = "___";
            } else {
                status = status.toLowerCase();
            }
            loadStats[1]++;
            rCnt = uCommons.RightHash(String.valueOf(ii+1), rHash);
            uCommons.uSendMessage("   .)    (" + rCnt + " of " +  nbrItems + ")" + spc + status + spc + inVal);

            if (NamedCommon.ZERROR) break;
        }
        return status;
    }

    private static String real_LoadItem(String file, String item, String record) {
        String cStr = "{LBP}{file="+file+"}{item=" + item + "}{data=" + record + "}";
        rCommons.setLoadFlag(true);
        String status = u2Commons.MetaBasic(cStr);
        rCommons.setLoadFlag(false);
        if (status.toLowerCase().equals("ok")) status = "<<PASS>>";
        return status;
    }

    public static String LoadThesePrograms(String[] pgms, String ext) {
        if (NamedCommon.BaseCamp.contains("/")) {
            NamedCommon.slash = "/";
        } else {
            NamedCommon.slash = "\\";
        }
        loadStats = new int[3];
        loadStats[0]=0;
        loadStats[1]=0;
        loadStats[2]=0;
        String fqFileName, status = "";
        BufferedWriter bWriter = null;
        boolean proceed, loadit = true;
        int nbrItems = pgms.length;
        String key = "", atID, record, compile, catalog, decat;
        uCommons.uSendMessage("   .) Loading " + nbrItems + " item(s)");
        String chker = "", skip = "";
        if (NamedCommon.databaseType.equals("UNIDATA")) {
            chker = "finish";
            skip = "_UV";
        } else {
            chker = "Complete";
            skip = "_UD";
        }

        for (int ii = 0; ii < nbrItems; ii++) {
            key = pgms[ii];
            fqFileName = NamedCommon.gmods + key;
            record = uCommons.ReadDiskRecord(fqFileName);
            if (record.startsWith("ENC(")) {
                record = record.substring(4, record.length());
                if (record.endsWith("\n")) {
                    while (record.endsWith("\n")) {
                        record = record.substring(0, record.length() - 1);
                    }
                }
                record = record.substring(0, record.length()-1);
                record = uCipher.Decrypt(record);
            }

            atID = key.substring(0, key.indexOf(ext));
            if (key.equals("uDO.bas")) atID = "uDO";
            if (atID.contains("\\/")) loadit = false;
            if (atID.endsWith(skip)) loadit = false;
            if (loadit) {
                if (fqFileName.endsWith(".ctl")) {
                    UniDynArray rec = new UniDynArray(uRead(uplBP, atID));
                    status = "<<SKIP> " + atID;
                    if (rec.equals("")|| rec.extract(1).equals("MT")) {
                        while (true) {
                            try {
                                uplBP.setRecordID(atID);
                                uplBP.setRecord(record);
                                uplBP.write();
                                status = "<<PASS> " + atID;
                                break;
                            } catch (UniFileException e) {
                                if (!u2Commons.TestAlive()) {
                                    SourceDB.ReconnectService();
                                } else {
                                    status = "<<FAIL>> Write FAILURE on " + uplBP.getFileName() + "  " + atID;
                                    uCommons.uSendMessage(status);
                                    uCommons.uSendMessage(e.getMessage());
                                    break;
                                }
                            }
                        }
                    }
                    rec = null;
                    uCommons.uSendMessage(status);
                    continue;
                }
                if (fqFileName.endsWith(".ins")) {
                    while (true) {
                        try {
                            uplInserts.setRecordID(atID);
                            uplInserts.setRecord(record);
                            uplInserts.write();
                            status = "<<PASS> " + atID;
                            break;
                        } catch (UniFileException e) {
                            if (!u2Commons.TestAlive()) {
                                SourceDB.ReconnectService();
                            } else {
                                status = "<<FAIL>> Write FAILURE on " + uplInserts.getFileName() + "  " + atID;
                                uCommons.uSendMessage(status);
                                uCommons.uSendMessage(e.getMessage());
                                break;
                            }
                        }
                    }
                } else {
                    status = "";
                    while (true) {
                        try {
                            uplBP.setRecordID(atID);
                            uplBP.setRecord(record);
                            uplBP.write();
                            if (NamedCommon.databaseType.equals("UNIDATA")) {
                                decat = "DELETE.CATALOG " + atID;
                            } else {
                                decat = "DECATALOG OBJ.UPL " + atID;
                                if (fqFileName.endsWith(".gct")) decat = "DELETE.CATALOG *" + atID;
                            }
                            if (compiler.equals("")) compiler = "uDO ";
                            compile = compiler;
                            if (NamedCommon.uplSite.equals(NamedCommon.UniLibre)) compile += "-H -K ";
                            compile += atID;
                            status = "<<PASS>> " + atID; // + " loaded, compiled & cataloged.";
                            proceed = uniExec(decat);
                            resp = NamedCommon.databaseType + " says " + resp;
                            if (proceed) {
                                if (!atID.equals("uDO")) {
                                    resp = "";
                                    proceed = uniExec(compile);
                                    if (!resp.contains(chker) && !resp.equals("")) {
                                        proceed = false;
                                    } else {
                                        UniString tst = uRead(NamedCommon.VOC, atID);
                                        if (tst.toString().equals("")) {
                                            resp = " Did Not Compile";
                                            proceed = false;
                                        } else {
                                            if (fqFileName.endsWith(".gct")) {
                                                proceed = uniExec("CATALOG OBJ.UPL *" + atID + " " + atID + " FORCE");
                                            }
                                        }
                                    }
                                } else {
                                    proceed = uniExec("BASIC BP.UPL uDO");
                                    proceed = uniExec("CATALOG BP.UPL uDO LOCAL");
                                }
                                if (!proceed) {
                                    status = "<<FAIL>> *************** " + compile + "  " + resp;
                                }
                            } else {
                                status = "<<FAIL>> *************** " + decat;
                            }

                            if (!NamedCommon.uplSite.equals(NamedCommon.UniLibre)) {
                                uplBP.setRecordID(atID);
                                uplBP.setRecord("MT");
                                uplBP.write();
                            }
                            break;
                        } catch (UniFileException e) {
                            if (!u2Commons.TestAlive()) {
                                SourceDB.ReconnectService();
                            } else {
                                status = "<<FAIL>> Write FAILURE on " + uplBP.getFileName() + "  " + atID;
                                status += "  " + e.getMessage();
                                break;
                            }
                        }
                    }
                }
            } else {
                status = "<<SKIP>> " + atID;
                loadit = true;
            }

            uCommons.uSendMessage(status);
            if (status.contains("<<PASS>>")) loadStats[0]++;
            if (status.contains("<<FAIL>>")) loadStats[1]++;
            if (status.contains("<<SKIP>>")) loadStats[2]++;

            if (!status.contains("<<FAIL>>")) {
                fqFileName = NamedCommon.gmods + key;
                record = uCommons.ReadDiskRecord(fqFileName);
                String fqLoaded = NamedCommon.gmods;
                if (!fqLoaded.endsWith(NamedCommon.slash)) fqLoaded += NamedCommon.slash;
                fqLoaded += "loaded";
                BufferedWriter bw = null;
                bw = uCommons.GetOSFileHandle(fqLoaded+NamedCommon.slash+key);
                if (bw != null) {
                    try {
                        bw.write(record);
                        bw.close();
                        bw = null;
                        File progfile = new File(fqFileName);
                        progfile.delete();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return status;
    }

    public static String DeleteCreate(String file) {
        boolean proceed = true;
        String cBP;
        String status = "<<PASS>>";
        uplHANDLE = uClose(uplHANDLE);
        uplHANDLE = uOpen(file);
        if (NamedCommon.ZERROR) {
            NamedCommon.Zmessage = "   .) "+file+" cannot open. Will create it now.";
            uCommons.uSendMessage(NamedCommon.Zmessage);
//            return "<<FAIL>>";
        }

        if (uplHANDLE != null) {
            cBP = "DELETE.FILE " + file;
            proceed = uniExec(cBP);
            if (proceed) uplHANDLE = uClose(uplHANDLE);
        } else {
            uplHANDLE = uClose(uplHANDLE);
        }

        if (uplHANDLE == null) {
            uCommons.uSendMessage("   .) Deleted  " + file + " in " + NamedCommon.dbpath);
            uCommons.uSendMessage("   .) Creating " + file + " in " + NamedCommon.dbpath);
            if (NamedCommon.databaseType.equals("UNIVERSE")) {
                cBP = "CREATE.FILE " + file + " 19";
            } else {
                cBP = "CREATE.FILE DIR " + file;
            }
            proceed = uniExec(cBP);
            if (!proceed) {
                status = "<<FAIL>> - " + u2Commons.uniExecResp;
            } else {
                if (uOpenFile(file, "2")) {
                    uplHANDLE = NamedCommon.U2File;
                } else {
                    status = "<<FAIL>> - " + file + " must be created by hand :: " + cBP;
                }
            }
        }
        return status;
    }

    public static String OpenCreate(String file) {
        boolean proceed = true;
        String status = "<<PASS>>";
        uplHANDLE = uClose(uplHANDLE);
        u2Commons.SetManaged(true);
        uplHANDLE = uOpen(file);
        u2Commons.SetManaged(false);
        if (NamedCommon.ZERROR && (!rfuelFiles.contains(file) || !custFiles.contains(file))) {
            NamedCommon.Zmessage = file+" cannot be openned or created.";
            return "<<FAIL>>";
        }

        if (uplHANDLE == null) {
            uCommons.uSendMessage("   .) Creating " + file + " in " + NamedCommon.dbpath);
            String cBP;
            if (NamedCommon.databaseType.equals("UNIVERSE")) {
                cBP = "CREATE.FILE " + file + " 19";
            } else {
                cBP = "CREATE.FILE DIR " + file;
            }
            proceed = uniExec(cBP);
            if (!proceed) {
                status = "<<FAIL>> - " + resp;
            } else {
                if (uOpenFile(file, "2")) {
                    uplHANDLE = NamedCommon.U2File;
                    uCommons.uSendMessage("   .) "+uplHANDLE.getFileName() + " has been created.");
                } else {
                    status = "<<FAIL>> - " + file + " must be created by hand :: " + cBP;
                }
            }
        }

        if (uplHANDLE.getFileName().equals("BP.UPL")) {
            uWriter(uplHANDLE, "DBT", "UV");
            uCommons.uSendMessage("   .) "+uplHANDLE.getFileName() + " DBT set to UV.");
            UniString uvChkrec = uRead(uplHANDLE, "properties");
            if (uvChkrec == null || uvChkrec.equals("")) {
                uWriter(uplHANDLE, "properties", "inf.logging=1\nupl.logging=0\n" +
                        "ulog.size=1048576\nulog.max=10\njlogs=false");
                uCommons.uSendMessage("   .) "+uplHANDLE.getFileName() + " properties created.");
                uWriter(uplHANDLE, "RQM", "99999");
                uCommons.uSendMessage("   .) "+uplHANDLE.getFileName() + " RQM created.");
            }
        }
        return status;
    }
}
