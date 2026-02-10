package com.unilibre.tester.tester;

        //  Useage: datahub_jsonBuilder.jar {source} {target}
        //
        //  e.g. datahub_jsonBuilder.jar KB/v2/CLIENT.map /home/unilibre/data/CLIENT_dsd.json
        // -----------------------------------------------------------------------------------


import com.unilibre.commons.uCommons;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.Properties;

public class datahub_jsonBuilder {

    public static void main(String[] args) {
        String src="", tgt="", base="", map="", dsd="";
        if (args.length == 2) {
            src = args[0];
            tgt = args[1];
        } else {
            System.out.println("");
            System.out.println("-------------------------------------------------------");
            System.out.println("Useage: datahub_jsonBuilder {source} {target}");
            System.out.println("        you MUST supply both - source AND target");
            System.out.println("-------------------------------------------------------");
            System.out.println("");
            return;
        }
        //
        while (src.contains("\\")) { src = src.replace("\\", "/"); }
        while (tgt.contains("\\")) { tgt = tgt.replace("\\", "/"); }
        String[] sParts = src.split("/");
        map = sParts[sParts.length-1];
        base = src.substring(0, src.length() - map.length());
        if (!base.endsWith("/")) base += "/";

        System.out.println("");
        System.out.println("Action: Operate on   : " + src);

        Properties mProps = LoadProperties(base + map);
        String u2File = mProps.getProperty("u2File");
        dsd = mProps.getProperty("list");

        String dsdBase="";
        for (int i = 0; i < sParts.length; i++) {
            if (i > 0) dsdBase += "/";
            if (sParts[i].equals("maps")) { dsdBase += "maps/"; break; }
            dsdBase += sParts[i];
        }

        System.out.println("      : Source file  : " + u2File);
        System.out.println("      : Described by : " + dsdBase + dsd);
        System.out.println("      : Send JSON to : " + tgt);

        String contents = uCommons.ReadDiskRecord(dsdBase + dsd);
        String[] lines = contents.split("\\r?\\n");
        String[] parts;
        String us = "_", cm = ",", ultras = "Ultracs",
                fName, description, av, mv, sv;
        int eol = lines.length;

        JSONObject dataType = new JSONObject();
        JSONObject empty = new JSONObject();
        JSONObject dType = new JSONObject();
        dType.put("com.linkedin.pegasus2avro.schema.StringType", empty);
        dataType.put("type", dType);

        JSONObject uvSchema  = new JSONObject();
        uvSchema.put("tableSchema", u2File);

        JSONObject platform = new JSONObject();
        platform.put("com.linkedin.pegasus2avro.schema.universe", uvSchema);

        JSONObject master   = new JSONObject();
        JSONObject proposed = new JSONObject();
        JSONObject snapshot = new JSONObject();
        JSONObject aspects  = new JSONObject();
        JSONArray aspectarr = new JSONArray();
        JSONObject schema   = new JSONObject();
        JSONObject field    = new JSONObject();
        JSONArray fieldarr  = new JSONArray();

        for (int l=0 ; l < eol ; l++) {
            if (lines[l].startsWith("#")) continue;
            field = new JSONObject();
            parts = lines[l].split("\\,");
            av = parts[1];
            mv = parts[2];
            sv = parts[3];
            if (av.equals("")) av = "1";
            if (mv.equals("")) mv = "1";
            if (sv.equals("")) sv = "1";
            fName = "F"+av+us+mv+us+sv;
            description = parts[5];
//            field.put("comment", "---------[ " + fName + " ]--------------");
            field.put("fieldPath", fName);
            if (av.equals("0")) {
                field.put("nullable", false);
                field.put("isPartOfKey", true);
            } else {
                field.put("nullable", true);
                field.put("isPartOfKey", false);
            }
            field.put("description", description);
            field.put("nativeDataType", "String()");
            field.put("recursive", false);
            field.put("type", dataType);
            fieldarr.put(field);
        }

        schema.put("schemaName", u2File);
        schema.put("platform", "urn:li:dataPlatform:universe");
        schema.put("version", 0);
        schema.put("hash", "");
        schema.put("platformSchema", "{com.linkedin.pegasus2avro.schema.universe: {tableSchema: " + u2File + "}}");
        schema.put("platformSchema", platform);
        schema.put("fields", fieldarr);
        aspects.put("com.linkedin.pegasus2avro.schema.SchemaMetadata", schema);
        aspectarr.put(0, aspects);
        snapshot.put("urn", "urn:li:dataset:(urn:li:dataPlatform:universe," + u2File + ",PROD)");
        snapshot.put("aspects", aspectarr);
        proposed.put("com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot", snapshot);
        master.put("proposedSnapshot", proposed);
        WriteJsonFile(master.toString(), tgt);
        System.out.println("Done.");
        System.out.println("");
    }

    private static void WriteJsonFile(String json, String jFile) {
        try {
            BufferedWriter bWriter = new BufferedWriter(new FileWriter(jFile));
            bWriter.write(json);
            bWriter.flush();
            bWriter.close();
            bWriter = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties LoadProperties(String fname) {
        Properties lProps = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(fname);
        } catch (FileNotFoundException e) {
            is = null;
        }
        if (is != null) {
            try {
                lProps.load(is);
            } catch (IOException e) {
                System.out.println(e.getMessage());
                return null;
            } catch (IllegalArgumentException iae) {
                System.out.println(iae.getMessage());
                return null;
            }
            try {
                is.close();
                is = null;
            } catch (IOException e) {
                System.out.println(e.getMessage());
                return null;
            }
        } else {
            System.out.println("Please load '" + fname + "'");
        }
        return lProps;
    }
}
