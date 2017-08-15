package edu.monash;

import java.net.ServerSocket;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        start();
        // Tests.stopWatchTest();
    }

    public static void start() {
        try {
            StreamingServer streamingServer = new StreamingServer();
            ServerSocket serverSocket = new ServerSocket(4000);
            while (true) {
                streamingServer.startService(serverSocket);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
