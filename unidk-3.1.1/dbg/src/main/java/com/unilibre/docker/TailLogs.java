package com.unilibre.docker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TailLogs {

    public static void main(String[] args) {

        String cmd = "docker container ls";
        String answer = nixExecCmd(cmd, 999999);
        System.out.println(answer);
    }

    public static String nixExecCmd(String cmd, int limit) {
        StringBuilder ans = new StringBuilder();
        String line = "";

        try {
            Runtime rt = Runtime.getRuntime();
            Process p = rt.exec(cmd);
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = stdInput.readLine()) != null) {
                if (line.equals("") || line.contains("null")) continue;
                ans.append(line).append("\n");
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return ans.toString();
    }

}
