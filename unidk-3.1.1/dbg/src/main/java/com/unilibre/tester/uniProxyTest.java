package com.unilibre.tester.tester;

import asjava.uniobjects.UniJava;
import asjava.uniobjects.UniSession;
import asjava.uniobjects.UniSessionException;
import asjava.unirpc.UniRPCTokens;

public class uniProxyTest {

    public static void main(String[] args) throws UniSessionException {
        int pxPort = UniRPCTokens.UNIRPC_DEFAULT_PROXY_PORT;                                // or other, as directed
        UniJava uvJava = new UniJava();
        UniSession uvSession = uvJava.openSession(0);

        // set uvcs connection params as per usual.

        String host = "ec2-54-153-142-191.ap-southeast-2.compute.amazonaws.com";
//        uvSession.setHostPort(UniRPCTokens.UNIRPC_DEFAULT_PORT );                           // 31438
        uvSession.setHostName(host );                                                       // MUST be hostname
        uvSession.setUserName( "rfuel" );
        uvSession.setPassword( "un1l1br3" );
        uvSession.setConnectionString( "uvcs" );
        uvSession.setAccountPath( "/data/uv/RFUEL" );

        // 1.   The proxy token (below) is used to make sure the host is in your access list (uniproxy.config)
        // 2.   The rest of the creds are needed for the actual connection

        uvSession.setProxyHost("192.168.48.107");
        uvSession.setProxyPort(pxPort);
        uvSession.setProxyToken("uv-aws");

        try {
            uvSession.connect();
            System.out.println("Passed");
            uvSession.disconnect();
        } catch (UniSessionException usx) {
            System.out.println(usx.getMessage());
        }
    }

}
