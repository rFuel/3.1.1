package com.unilibre.rfsockets;
/*Copyright UniLibre on 2015. ALL RIGHTS RESERVED */

import com.unilibre.commons.uCommons;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

public class Main {

    // -----------------------------------------------------------------------
    //                   This service runs on the database host.
    // -----------------------------------------------------------------------

    private static int oneKbyte = 1024;
    private Map<SocketChannel, List<byte[]>> keepDataTrack = new HashMap<>();
    private ByteBuffer buffer = ByteBuffer.allocate(8 * oneKbyte);

    private void startEchoServer() {

        final int STARTUP_PORT = 58555;

        Selector selector = null;
        ServerSocketChannel serverSocketChannel = null;

        //
        // Use Selector because this object manages the state of each channel.  
        //     * could use one thread per channel BUT threads are OS expensive  
        //     * threads can die unexpectedly and the reasons are lost          
        //     * one socket server can serve all the channels via the Selector  
        //

        try {
            selector = Selector.open();
        } catch (IOException e) {
            uCommons.uSendMessage("*********************************************************************");
            uCommons.uSendMessage("Cannot create a Selector object");
            uCommons.uSendMessage(e.getMessage());
            uCommons.uSendMessage("*********************************************************************");
            System.exit(0);
        }

        try {
            serverSocketChannel = ServerSocketChannel.open();
        } catch (IOException e) {
            uCommons.uSendMessage("*********************************************************************");
            uCommons.uSendMessage("Cannot create a ServerSocketChannel object");
            uCommons.uSendMessage(e.getMessage());
            uCommons.uSendMessage("*********************************************************************");
            System.exit(0);
        }

        try {
            if ((serverSocketChannel.isOpen()) && (selector.isOpen())) {
                serverSocketChannel.configureBlocking(false);
                serverSocketChannel.setOption(StandardSocketOptions.SO_RCVBUF, 256 * oneKbyte);
                serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                serverSocketChannel.bind(new InetSocketAddress(STARTUP_PORT));

                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

                System.out.println("Waiting for connections on ... " + STARTUP_PORT);

                while (true) {
                    selector.select();
                    Iterator keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        SelectionKey key = (SelectionKey) keys.next();
                        System.out.println("Have a key: " + key);
                        keys.remove();
                        if (!key.isValid()) continue;
                        if (key.isAcceptable()) {
                            System.out.println("isAcceptable");
                            acceptOP(key, selector);
                        }
                        if (key.isReadable()) {
                            System.out.println("isReadable");
                            this.readOP(key);
                        }
                        if (key.isWritable()) {
                            System.out.println("isWritable");
                            this.writeOP(key);
                        }
                    }
                }
            } else {
                System.out.println("The server socket channel or selector cannot be opened!");
            }
        } catch (IOException e) {
            uCommons.uSendMessage("*********************************************************************");
            uCommons.uSendMessage("ServerSocketChannel object error :");
            uCommons.uSendMessage(e.getMessage());
            uCommons.uSendMessage("*********************************************************************");
            System.exit(0);
        }

    }

    // isAcceptable is true

    private void acceptOP(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverChannel.accept();
        socketChannel.configureBlocking(false);
        System.out.println("Incoming connection from: " + socketChannel.getRemoteAddress());
        // send a welcome message
        socketChannel.write(ByteBuffer.wrap("Hello!\n".getBytes("UTF-8")));
        System.out.println("   said Hello!");
        // register the channel with selector for further I/O
        keepDataTrack.put(socketChannel, new ArrayList<byte[]>());
        socketChannel.register(selector, SelectionKey.OP_READ);
        System.out.println("    connection accepted ");
    }

    // isReadable is true

    private void readOP(SelectionKey key) {
        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            buffer.clear();
            int numRead = -1;
            try {
                numRead = socketChannel.read(buffer);
            } catch (IOException e) {
                System.err.println("Cannot read error!");
            }
            if (numRead == -1) {
                this.keepDataTrack.remove(socketChannel);
                System.out.println("Connection closed by: " + socketChannel.getRemoteAddress());
                socketChannel.close();
                key.cancel();
                return;
            }
            byte[] data = new byte[numRead];
            System.arraycopy(buffer.array(), 0, data, 0, numRead);
            System.out.println(new String(data, "UTF-8") + " from " + socketChannel.getRemoteAddress());
            // write back to client
            doEchoJob(key, data);
        } catch (IOException ex) {
            System.err.println(ex);
        }
    }

    //isWritable is true

    private void writeOP(SelectionKey key) throws IOException {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        List<byte[]> channelData = keepDataTrack.get(socketChannel);
        Iterator<byte[]> its = channelData.iterator();
        while (its.hasNext()) {
            byte[] it = its.next();
            its.remove();
            socketChannel.write(ByteBuffer.wrap(it));
        }
        key.interestOps(SelectionKey.OP_READ);
    }

    private void doEchoJob(SelectionKey key, byte[] data) {
        SocketChannel socketChannel = (SocketChannel) key.channel();
        List<byte[]> channelData = keepDataTrack.get(socketChannel);
        channelData.add(data);
        key.interestOps(SelectionKey.OP_WRITE);
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.startEchoServer();
    }

}