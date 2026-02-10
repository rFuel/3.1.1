package com.unilibre.rfuel;

import com.unilibre.commons.NamedCommon;
import com.unilibre.commons.uCommons;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class PropertyTest {
    public static void main(String[] args) {
        String fname = args[0];
        System.out.println("Manually:");
        System.out.println("---------");
        Properties p1 = DoManual(fname);
        System.out.println("[preuni] "+p1.get("preuni"));
        System.out.println("[presql] "+p1.get("presql"));
        System.out.println(" ");
        System.out.println("----------------------------------------------------------");
        System.out.println(" ");
        System.out.println("uCommons.LoadProperties:");
        System.out.println("------------------------");
        Properties p2 = uCommons.LoadProperties(fname);
        System.out.println("[preuni] "+p2.get("preuni"));
        System.out.println("[presql] "+p2.get("presql"));
    }

    private static Properties DoManual(String fname) {
        Properties lProps = new Properties();
        List<String> plines;
        try {
            plines = Files.readAllLines(Paths.get(fname), StandardCharsets.ISO_8859_1); // safer encoding
            StringBuilder sb = new StringBuilder();
            StringBuilder logicalLine = new StringBuilder();
            String trimmed;
            for (String line : plines) {
                if (line.startsWith("*")) continue;
                if (line.startsWith("#")) continue;
                trimmed = line.trim();
                if (trimmed.endsWith("\\")) {
                    logicalLine.append(trimmed, 0, trimmed.length() - 1);
                    continue; // wait for more lines
                } else {
                    logicalLine.append(trimmed);
                    sb.append(logicalLine.toString()).append("\n");
                    logicalLine.setLength(0); // reset for next logical line
                }
            }
            try (Reader reader = new StringReader(sb.toString())) {
                lProps.load(reader);
            } catch (IOException | IllegalArgumentException e) {
                NamedCommon.StopNow = "<<FAIL>>";
                NamedCommon.ZERROR = true;
                NamedCommon.Zmessage = "Error loading '" + fname + "' – " + e.getMessage();
                uCommons.uSendMessage(NamedCommon.Zmessage);
            }
        } catch (IOException e) {
            System.out.println("Crap !!");
        }
        return lProps;
    }
}
