package com.unilibre.dataserver;

import java.util.*;

public class JstatOutputParser {

    public static Map<String, List<Object>> parse(String rawOutput) {
        Map<String, List<Object>> result = new HashMap<>();
        List<Object> rfuelList = new ArrayList<>();
        List<Object> backgroundList = new ArrayList<>();

        String[] sections = rawOutput.split("(?m)^### SECTION:");
        for (String section : sections) {
            if (section.contains("rFuel")) {
                rfuelList.addAll(parseRFuelLines(section));
            } else if (section.contains("Background")) {
                backgroundList.addAll(parseBackgroundLines(section));
            }
        }

        result.put("rfuel", rfuelList);
        result.put("background", backgroundList);
        return result;
    }

    private static List<RFuelProcess> parseRFuelLines(String section) {
        List<RFuelProcess> parsed = new ArrayList<>();
        String[] lines = section.split("\n");
        for (String line : lines) {
            if (line.toLowerCase(Locale.ROOT).startsWith("rfuel:")) continue;
            //
            // E.G. unilibre 4195 CPU:0.4% MEM:1.2% -D64 -Dbkr=bulk.bkr -Dtask=014 -Dque=1 com.unilibre.rfuel.StartUp
            //
            try {
                String[] lParts = line.split(" ");
                String user="", pid="", cpu="", mem="", bkr="", task="", que="", clazz="";
                for (int i=0; i< lParts.length; i++) {
                    switch (i) {
                        case 0:
                            user = lParts[i];
                            break;
                        case 1:
                            pid = lParts[i];
                            break;
                        case 2:
                            cpu = lParts[i];
                            cpu = cpu.split(":")[1].split("%")[0];
                            break;
                        case 3:
                            mem = lParts[i];
                            mem = mem.split(":")[1].split("%")[0];
                            break;
                        case 4:
                            break;
                        case 5:
                            bkr = lParts[i].split("=")[1];
                            break;
                        case 6:
                            task = lParts[i].split("=")[1];
                            break;
                        case 7:
                            que = lParts[i].split("=")[1];
                            break;
                        case 8:
                            clazz = lParts[i];
                            break;
                    }
                }

                parsed.add(new RFuelProcess(user, Integer.parseInt(pid), Double.parseDouble(cpu), Double.parseDouble(mem), bkr, task, que, clazz));

            } catch (Exception ignored) {}
        }
        return parsed;
    }

    private static List<BackgroundProcess> parseBackgroundLines(String section) {
        List<BackgroundProcess> parsed = new ArrayList<>();
        String[] lines = section.split("\n");
        for (String line : lines) {
            if (line.toLowerCase(Locale.ROOT).startsWith("background:")) continue;
            try {
                String[] parts = line.split("\\s+");
                String cpu = line.split("CPU:")[1].split("%")[0].trim();
                String mem = line.split("MEM:")[1].split("%")[0].trim();
                String clazz = line.replaceFirst("^\\S+\\s+\\d+\\s+", "").replaceAll("CPU:.*", "").trim();

                parsed.add(new BackgroundProcess(
                        parts[0],
                        Integer.parseInt(parts[1]),
                        clazz,
                        Double.parseDouble(cpu),
                        Double.parseDouble(mem)
                ));
            } catch (Exception ignored) {}
        }
        return parsed;
    }
}
