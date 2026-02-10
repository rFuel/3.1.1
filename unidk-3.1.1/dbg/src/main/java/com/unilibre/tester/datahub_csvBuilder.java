package com.unilibre.tester.tester;

import com.unilibre.commons.uCommons;

public class datahub_csvBuilder {

    public static void main(String[] args) {
        String resource = "\"urn:li:dataset:(urn:li:dataPlatform:universe,CoreBanking,PROD)\"";
        String tags = "[urn:li:tag:Ultracs]";
        String map  = "CLIENT.dsd";
        String contents = uCommons.ReadDiskRecord("R:\\upl\\maps\\KB\\v2\\" + map);
        String[] lines = contents.split("\\r?\\n");
        String[] parts;
        String us = "_", cm = ",", glossary = "", owners = "", ownertype = "", ultras = "Ultracs",
                subresource, description, av, mv, sv, csvLn;
        int eol = lines.length;
        System.out.println("resource,subresource,glossary_terms,tags,owners,ownership_type,description");
        for (int l=0 ; l < eol ; l++) {
            if (lines[l].startsWith("#")) continue;
            parts = lines[l].split("\\,");
            av = parts[1];
            mv = parts[2];
            sv = parts[3];
            if (av.equals("")) av = "1";
            if (mv.equals("")) mv = "1";
            if (sv.equals("")) sv = "1";
            subresource = "F"+av+us+mv+us+sv;
            description = parts[5];
            csvLn = resource + cm + subresource + cm + glossary + cm + tags + owners + cm + owners + cm + ownertype + cm + description + cm + ultras;
            System.out.println(csvLn);
        }
    }
}
