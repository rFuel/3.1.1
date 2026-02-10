package com.unilibre.commons;

import com.northgateis.reality.rsc.RSC;
import com.northgateis.reality.rsc.RSCException;

import java.util.ArrayList;
import java.util.Arrays;

public class rCommons {

    // Common methods for the Reality DB.

    private static boolean LoadingBP = false;
    private static ArrayList<String> mKeys = new ArrayList<>();
    private static ArrayList<String> mVals = new ArrayList<>();


    public static void setLoadFlag(boolean flag) {
        LoadingBP = flag;
    }

    public static String realSub(String inVal) {
        String outVal="";
        RSC sub;
        if (LoadingBP) {
            if (inVal.startsWith("{LBP}")) {
                String vParts = inVal.replaceAll("\\}\\{", "}\n{");
                ArrayList<String> msgArray = new ArrayList<String>(Arrays.asList(vParts.split("\n")));
                mKeys.clear();
                mVals.clear();
                int lx = msgArray.size(), lxx;
                String tmp, tmp1, tmp2;
                for (int m=0; m<lx; m++) {
                    tmp = msgArray.get(0);
                    lxx = tmp.indexOf("=");
                    if (lxx > 0) {
                        tmp = tmp.substring(1, tmp.length() - 1);
                        tmp1 = tmp.substring(0, lxx - 1);
                        tmp2 = tmp.substring(lxx, tmp.length());
                        mKeys.add(tmp1);
                        mVals.add(tmp2);
                    }
                    msgArray.remove(0);
                }
                String file = rMessagePart(mKeys, mVals, "file");
                String item = rMessagePart(mKeys, mVals, "item");
                String data = rMessagePart(mKeys, mVals, "data");

                sub = new RSC(NamedCommon.rcon, "SLBP");
                try {
                    sub.getParam(1).setValue("");
                    sub.getParam(2).setValue(file);
                    sub.getParam(3).setValue(item);
                    sub.getParam(4).setValue(data);
                    sub.execute();
                    outVal = sub.getParam(1).getString();
                } catch (RSCException e) {
                    NamedCommon.Zmessage = e.toString().replaceAll("\\r?\\n", " ").trim();
                    NamedCommon.ZERROR = true;
                    uCommons.uSendMessage("ERROR with message: " + inVal);
                    outVal = NamedCommon.Zmessage;
                }
            }
        } else {
            sub = new RSC(NamedCommon.rcon, "SR.METABASIC");
            try {
                sub.getParam(1).setValue("");
                sub.getParam(2).setValue(inVal);
                sub.execute();
                outVal = sub.getParam(1).getString();
            } catch (RSCException e) {
                NamedCommon.Zmessage = e.toString().replaceAll("\\r?\\n", " ").trim();
                NamedCommon.ZERROR = true;
                uCommons.uSendMessage("ERROR with message: " + inVal);
                uCommons.uSendMessage(NamedCommon.Zmessage);
            }
        }
        outVal = outVal.replace("{EOX}", "");
        if (outVal.startsWith("{")) {
            outVal = outVal.substring(1, outVal.length());
            outVal = outVal.substring(0, outVal.length() - 1);
            if (outVal.toUpperCase().startsWith("ANS=")) outVal = outVal.substring(4, outVal.length());
        }
        sub = null;
        return outVal;
    }

    private static String rMessagePart(ArrayList<String> mKeys, ArrayList<String> mVals, String key) {
        String answer = "";
        int lx = mKeys.indexOf(key);
        if (lx > -1) {
            answer = mVals.get(lx);
        }
        return answer;
    }

    public static void CleanVoc(String pid) {
        String srtn = "{SAR}";
        String sCmd = "";
        String file = "{file=MD}";
        String item = "{ITEM=}";
        String atr  = "{atr=-1}";
        String hush = "{hush=true}";
        if (NamedCommon.debugging) uCommons.uSendMessage("uRest: Q-Files by \"upl\" and PID: " + NamedCommon.pid);
        sCmd = "{cmD=SELECT MD WITH F0 = \"upl]\"}";
        if (!pid.equals("")) sCmd += " AND WITH F0 = [\"" + NamedCommon.pid + "]\"";
        String cSel = srtn + sCmd + file;
        String line = u2Commons.MetaBasic(cSel);
        String del  = "{DEL}{file=MD}";
        String recId, response;
        if (!line.equals("")) {
            ArrayList<String> items = new ArrayList<>(Arrays.asList(line.split("<im>")));
            int nbrItems = items.size();
            for (int i=0; i < nbrItems; i++) {
                recId = items.get(i).split("<km>")[0];
                if (recId.equals("")) continue;
                response = u2Commons.MetaBasic(del + "{item="+recId+"}");
                if (NamedCommon.ZERROR) {
                    uCommons.uSendMessage(NamedCommon.Zmessage);
                    return;
                }
            }
            uCommons.uSendMessage(nbrItems + " Q-Pointers removed from MD");
            items.clear();
            line = "";
        }
    }

    public static void Startup(boolean shutdown) {
        String cmd, file, atr, mv, sv, rtnString, cStr, sDown="";

        if (shutdown) sDown = "stop";

        uCommons.uSendMessage("Clearing uLOG");
        cmd = "{CLF}";
        file = "{file=uLOG}{item=}{data=}";
        atr = "{atr=}";
        mv = "{mv=}";
        sv = "{sv=}";
        cStr = cmd + file + atr + mv + sv;
        rtnString = realSub(cStr);

        uCommons.uSendMessage("Resetting STOP switch in BP.UPL");
        cmd = "{WRI}";
        file = "{file=BP.UPL}{item=STOP}{data="+ sDown + "}";
        atr = "{atr=}";
        mv = "{mv=}";
        sv = "{sv=}";
        cStr = cmd + file + atr + mv + sv;
        rtnString = realSub(cStr);

        if (!shutdown) {
            uCommons.uSendMessage("STOP switch has been turned OFF");
        } else {
            uCommons.uSendMessage("STOP switch has been turned ON");
        }

        cmd = "{WRI}";
        file = "{file=BP.UPL}{item=RQM}{data=" + NamedCommon.RQM + "}";
        atr = "{atr=}";
        mv = "{mv=}";
        sv = "{sv=}";
        cStr = cmd + file + atr + mv + sv;
        rtnString = realSub(cStr);
        if (NamedCommon.ZERROR) return;
    }

    public static String ReadAnItem(String file, String item, String a, String m, String s) {
        String ans="";
        String cmd = "{RDI}";
        String fle = "{file="+file+"}{item="+item+"}";
        String atr = "{atr="+a+"}";
        String mv = "{mv="+m+"}";
        String sv = "{sv="+s+"}";
        String cStr = cmd + fle + a + m + s;
        ans = realSub(cStr);
        return ans;
    }
}
