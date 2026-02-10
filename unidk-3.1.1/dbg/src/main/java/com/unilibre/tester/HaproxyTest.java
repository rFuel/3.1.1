package com.unilibre.tester.tester;

import asjava.uniobjects.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import static org.apache.commons.lang.StringUtils.substring;

public class HaproxyTest {

    // on rfuel22  : haproxy -f /upl/conf/aws-uv-pxy.cfg
    // redirects from rfuel22:31400  to  aws-uv:31438

    private static int pxPort = 31400;
    private static String pxHost = "";
    private static String mode = "";
    private static UniJava uvJava = null;
    private static UniSession uvSession = null;


    public static void main(String[] args) throws UniSessionException, IOException, InterruptedException {
        uvJava = new UniJava();

        String[] modes = {"src", "direct"};

        for (int i = 0 ; i < modes.length ; i++) {
            uvSession = uvJava.openSession(0);
            mode = modes[i];
            switch (mode) {
                case "direct":
                    DirectTest();
                    break;
                case "src":
                    SrcHostTest();
                    break;
            }
            uvSession.disconnect();
        }
    }

    private static void DirectTest() throws InterruptedException {

        // must know the target UV credentials.

        pxHost = "192.168.48.107";
        pxPort = 31400;

        uvSession.setHostName( pxHost );
        uvSession.setHostPort( pxPort );
        uvSession.setUserName( "rfuel" );
        uvSession.setPassword( "un1l1br3" );
        uvSession.setConnectionString( "uvcs" );
        uvSession.setAccountPath( "/data/uv/RFUEL" );

        try {
            uvSession.connect();
            UniFile VOC = uvSession.openFile("VOC");
            if (VOC.isOpen()) ShowSuccess();
            VOC.close();
            uvSession.disconnect();
            Thread.sleep(1000);
        } catch (UniSessionException | UniFileException usx) {
            System.out.println(usx.getMessage());
        }
    }

    private static void SrcHostTest() throws IOException {

        // target uv host credentials are held in connection config file

        String cfg = "R:/upl/conf/functiontest.shost";
        InputStream is = new FileInputStream( cfg );
        Properties lProps = new Properties();
        lProps.load(is);

        uvSession.setHostName( lProps.getProperty("host") );
        uvSession.setHostPort( Integer.parseInt(lProps.getProperty("port")) );
        uvSession.setUserName( lProps.getProperty("user") );
        uvSession.setPassword( lProps.getProperty("pword") );
        uvSession.setConnectionString( lProps.getProperty("protocol") );
        uvSession.setAccountPath( lProps.getProperty("path") );

        try {
            uvSession.connect();
            UniFile VOC = uvSession.openFile("VOC");
            if (VOC.isOpen()) ShowSuccess();
            uvSession.disconnect();
        } catch (UniSessionException usx) {
            System.out.println(usx.getMessage());
        } catch (NullPointerException npe) {
            System.out.println(npe.getMessage());
        }
    }

    private static void ShowSuccess() {
        System.out.println(substring("PASS: (" + mode + ") ----------------------------------------------------", 0,40));
        System.out.println("      host : " + uvSession.getHostName());
        System.out.println("      port : " + uvSession.getHostPort());
        System.out.println("      user : " + uvSession.getUserName());
        System.out.println("      path : " + uvSession.getAccountPath());
    }

}
