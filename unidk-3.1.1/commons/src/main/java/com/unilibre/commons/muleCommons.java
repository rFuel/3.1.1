package com.unilibre.commons;


import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

public class muleCommons {

    private static HttpURLConnection con;
    private static HttpsURLConnection scon;
    private static String prevURL = "";
    private static String type = "application/json", uAgent = "Mozilla/5.0", auth = "No Auth";


    public static String sendToEndpoint(String url, String message) {

        String answer = "";

        byte[] postData = message.getBytes(StandardCharsets.UTF_8);

        boolean okay = false;
        BufferedReader buf = null;
        DataOutputStream dos = null;
        BufferedReader errorStream = null;
        int tries=0;

        while (!okay) {
            try {
                URL myurl = new URL(url);
                if (myurl.getProtocol().toLowerCase().equals("https")) {
                    try {
                        // Create a trust manager that does not validate certificate chains
                        TrustManager[] trustAllCerts = new TrustManager[]{
                                new X509TrustManager() {
                                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                                        return null;
                                    }

                                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                                    }

                                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                                    }
                                }
                        };

                        // Install the all-trusting trust manager
                        SSLContext sc = SSLContext.getInstance("SSL");
                        sc.init(null, trustAllCerts, new java.security.SecureRandom());
                        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

                        // Create all-trusting host name verifier
                        HostnameVerifier allHostsValid = new HostnameVerifier() {
                            public boolean verify(String hostname, SSLSession session) {
                                return true;
                            }
                        };

                        // Install the all-trusting host verifier
                        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    } catch (KeyManagementException e) {
                        e.printStackTrace();
                    }
                    scon = null;
                    scon = (HttpsURLConnection) myurl.openConnection();
                    scon.setDoOutput(true);
                    scon.setRequestMethod("POST");
                    scon.setRequestProperty("User-Agent", uAgent);
                    scon.setRequestProperty("Content-Type", type);
                    scon.setRequestProperty("Authorization", auth);
//                scon.connect();

                    // the IBM way works BEST --------------------------------------
                    OutputStream out = scon.getOutputStream();
                    out.write(postData, 0, postData.length);
                    out.close();
                    out = null;

                    // execute HTTPS request
                    int returnCode = scon.getResponseCode();

                    // get result from stream input or erro stream
                    InputStream connectionIn = null;
                    if (returnCode == 200) {
                        connectionIn = scon.getInputStream();
                    } else {
                        connectionIn = scon.getErrorStream();
                    }
                    buf = new BufferedReader(new InputStreamReader(connectionIn));
                    String inputLine;
                    answer = "";
                    while (answer.equals("")) {
                        if (tries > 20) break;
                        while ((inputLine = buf.readLine()) != null) {
                            answer += inputLine;
                        }
                        tries++;
                    }

                    connectionIn.close();
                    connectionIn = null;
                    inputLine = "";
                    buf.close();
                    buf = null;
                    // ----------------------------------------------------
                    scon.disconnect();
                    scon = null;
                } else {
                    con = null;
                    con = (HttpURLConnection) myurl.openConnection();
                    con.setDoOutput(true);
                    con.setRequestMethod("POST");
                    con.setRequestProperty("User-Agent", "Java client");
                    con.setRequestProperty("Content-Type", type);
                    con.setRequestProperty("x-rfuel-api", "the-quick-brown-fox-got-decoded");
                    prevURL = url;
                    myurl = null;
                    dos = null;
                    dos = new DataOutputStream(con.getOutputStream());
                    dos.write(postData);
                    dos.flush();
                    buf = null;
                    buf = new BufferedReader(new InputStreamReader(con.getInputStream()));
                    StringBuilder content = new StringBuilder();
                    String line;
                    answer = "";
                    while ((line = buf.readLine()) != null) {
                        content.append(line);
                        content.append(System.lineSeparator());
                    }
                    answer = content.toString();
                    if (answer.equals("")) {
                        errorStream = new BufferedReader(new InputStreamReader(con.getErrorStream()));
                        while ((line = errorStream.readLine()) != null) {
                            content.append(line);
                            content.append(System.lineSeparator());
                        }
                        answer = content.toString();
                    }
                    content = null;
                }
                myurl = null;
                okay = true;
            } catch (MalformedURLException e) {
                answer = e.getMessage();
                System.out.println("MalformedURLException: " + answer);
                break;
            } catch (ProtocolException e) {
                System.out.println("ProtocolException: " + e.getMessage());
                break;
            } catch (IOException e) {
                System.out.println("HTTP IOException: " + e.getMessage());
                break;
            } finally {
                con.disconnect();
                try {
                    if (buf != null) buf.close();
                    if (dos != null) dos.close();
                } catch (IOException e) {
                    System.out.println("IOException in sendToEndpoint - " + e.getMessage());
                }
                buf = null;
                con = null;
                dos = null;
            }
        }
        postData = null;
        return answer;
    }

    public static String TryAgain(String url) {
        String answer = "";
        int tries=0;
        BufferedReader buf = null;
        try {
            URL myurl = new URL(url);
            con = null;
            con = (HttpURLConnection) myurl.openConnection();
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("User-Agent", "Java client");
            con.setRequestProperty("Content-Type", type);
            con.setRequestProperty("x-rfuel-api", "the-quick-brown-fox-got-decoded");
            buf = new BufferedReader(new InputStreamReader(con.getInputStream()));
            StringBuilder content = new StringBuilder();
            String line;
            answer = "";
            while (answer.equals("")) {
                if (tries > 20) break;
                while ((line = buf.readLine()) != null) {
                    content.append(line);
                    content.append(System.lineSeparator());
                }
                tries++;
            }
            answer = content.toString();
        } catch (MalformedURLException e) {
            System.out.println("MalformedURLException for URL "+url+" - " + e.getMessage());
        } catch (ProtocolException e) {
            System.out.println("ProtocolException for URL "+url+" - " + e.getMessage());
        } catch (IOException e) {
            System.out.println("ProtocolException for URL "+url+" - " + e.getMessage());
        } finally {
            con.disconnect();
            try {
                if (buf != null) buf.close();
            } catch (IOException e) {
                System.out.println("IOException on BufferedReader - " + e.getMessage());
            }
            con = null;
            buf = null;
        }
        return answer;
    }

}
