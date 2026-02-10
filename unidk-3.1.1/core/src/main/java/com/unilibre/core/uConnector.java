package com.unilibre.core;
/* Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.commons.APImap;
import com.unilibre.commons.APImsg;
import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

import static org.apache.commons.lang3.StringEscapeUtils.escapeXml11;

public class uConnector {

    public static String Format(String input, String fmt) {
        String output = input;
        if (input.length() > 0) {
            if (fmt.toUpperCase().equals("XML")  && input.startsWith("<")) return output;
            if (fmt.toUpperCase().equals("JSON") && input.startsWith("{")) return output;
            if (fmt.toUpperCase().equals("JSON") && input.startsWith("[")) return output;
        }

        if (fmt.equals("")) {
            fmt = APImsg.APIget("esbfmt");
            if (fmt.equals("")) {
                fmt = uCommons.APIGetter("esbfmt");
                if (fmt.equals("")) fmt = APImsg.APIget("format");
                  if (fmt.equals("")) return input;
            }
        }

        switch (fmt.toUpperCase()) {
            case "XML":
                if (!input.equals("")) {
                    if (input.substring(0, 1).equals("{")) {
                        output = json2xml(input);
                    } else {
                        uCommons.uSendMessage("        : response data is expected in JSON format. NO conversion made.");
                    }
                }
                break;
            case "JSON":
                if (!input.equals("")) {
                    if (input.substring(0, 1).equals("<")) {
                        output = xml2json(input);
                    } else {
                        uCommons.uSendMessage("        : response data is expected in XML format. NO conversion made.");
                    }
                }
                break;
            default:
                output = input;
        }
        return output;
    }

    public static String xml2json(String xml) {
        String ans = xml;
        try {
            JSONObject jsnObj = XML.toJSONObject(xml);
            ans = jsnObj.toString();
        } catch (JSONException je) {
            uCommons.uSendMessage("***");
            uCommons.uSendMessage("rFuel xml2json() exception: " + je.getMessage());
            uCommons.uSendMessage("rFuel will return the string unchanged.");
            uCommons.uSendMessage("***");
            ans = xml;
        }
        return ans;
    }

    public static String json2xml(String payload) {
        String xml = payload;
        try {
            JSONObject j2xObject = new JSONObject(payload);
            xml = XML.toString(j2xObject);
            if (NamedCommon.escXML) xml = escapeXml11(xml);
            j2xObject = null;
        } catch (JSONException je) {
            uCommons.uSendMessage("***");
            uCommons.uSendMessage("rFuel json2xml() exception: " + je.getMessage());
            uCommons.uSendMessage("rFuel will return the string unchanged.");
            uCommons.uSendMessage("***");
            xml = payload;
        }
        return xml;
    }

}
