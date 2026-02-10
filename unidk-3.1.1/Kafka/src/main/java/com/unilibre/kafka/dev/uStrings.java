package com.unilibre.kafka.dev;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.unilibre.commons.uCommons;

import java.util.List;

public class uStrings {

    public static List<String> gSplit2List(String inStr, String chr) {
        return Lists.newArrayList(Splitter.on(chr).trimResults().split(inStr));
    }

    public static String[] gSplit2Array(String inStr, String chr) {
        List ans = gSplit2List(inStr, chr);
        String[] array = new String[ans.size()];
        return (String[]) ans.toArray(array);
    }

    public static int gDcount(String inStr, char chr) {
        int ans;
        ans = CharMatcher.is(chr).countIn(inStr);
        return (ans + 1);
    }

    public static String gReplace(String inStr, String fChr, String tChr) {
        if (fChr.length() > 1) {
            uCommons.uSendMessage("ERROR >>> gReplace() cannot change more than 1 char "
                    + fChr + " is too big !!");
            uCommons.uSendMessage("ERROR >>> no change made.");
            return fChr;
        } else {
            char chr = fChr.charAt(0);
            return CharMatcher.is(chr).replaceFrom(inStr, tChr);
        }
    }

    public static String gTrimALL(String inStr, String chr) {
        return CharMatcher.anyOf(chr).trimFrom(inStr);
    }

}
