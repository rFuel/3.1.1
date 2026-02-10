package com.unilibre.tester.tester;

import com.unilibre.commons.NamedCommon;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.util.TrustManagerUtils;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.concurrent.Callable;

public class remote_UV_file_reader  implements Callable {

    String contents;
    final boolean SSL = false;

    @Override
    public Object call() throws Exception {
        this.contents = "";
        String unc = "\\\\13.211.203.239\\data\\uv\\RFUEL\\uDELTA.LOG\\20569.15863.99972.1671.176602.ulog";
        String host = "13.211.203.239";
        String user = "rfuel";
        String pass = "un1l1br3";
        String dir  = "/data/uv/RFUEL/uDELTA.LOG/";
        String file = "20569.15863.99972.1671.176602.ulog";
        String conStr = "ftp://"+user+":"+pass+"@"+host+dir+file;
        FTPConnector fcon = new FTPConnector();
        fcon.server = host;
        fcon.user   = user;
        fcon.password  = pass;
        fcon.directory = dir;
        fcon.CreateClient(SSL);
        if (fcon.connect()) {
            ArrayList<String> files = fcon.listFiles();
            InputStream is  = fcon.GetInputStream(file);
            InputStreamReader sr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(sr);
            contents = br.lines().toString();
        }
        return contents;
    }

    class FTPConnector {

        private FTPClient f;
        public String server, user, password, directory;

        private void CreateClient(boolean isSSL) {
            f = new FTPClient();
        }

        private boolean connect() {
            try {
//                f.setDefaultTimeout(5000);
//                f.setTrustManager(TrustManagerUtils.getAcceptAllTrustManager());
                f.connect(server, 22);
            } catch (RuntimeException re) {
                System.out.println("RuntimeException:   " + re.getMessage());
            } catch (SocketException e) {
                System.out.println("SocketException :   " + e.getMessage());
            } catch (IOException e) {
                System.out.println("IOException     :   " + e.getMessage());
            }
            int reply = f.getReplyCode();
            boolean ok = false;
            if (FTPReply.isPositiveCompletion(reply)) {
                ok = true;
            }
            ok = true;
            if (ok) {
                try {
//                    f.execPROT("P");                // private
//                    System.out.println(f.getEnabledProtocols());
                    f.login(user, password);
                    f.enterLocalPassiveMode();
                    f.setBufferSize(1024 * 1024);
                    f.setFileType(FTPClient.BINARY_FILE_TYPE);
                    f.setKeepAlive(true);
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
            return ok;
        }

        private ArrayList<String> listFiles() throws IOException {
            FTPFile[] ftpfiles = f.listFiles(directory);
            ArrayList<String> files = new ArrayList<>();
            int nbrFiles = ftpfiles.length;
            for (int i=0 ; i < nbrFiles ; i++) {
                files.add(ftpfiles[i].toString());
            }
            return files;
        }

        private InputStream GetInputStream(String filename) throws IOException {
            return f.retrieveFileStream(filename);
        }
    }
}
