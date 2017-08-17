package edu.monash;

import java.net.ServerSocket;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        if (args.length < 2)
        {
            System.out.println("Usage: <Algorithm> <Probe Sequence>");
            System.exit(1);
        }
        Algorithm algorithm = Algorithm.valueOf(args[0]);
        ProbeSequence probeSequence = ProbeSequence.valueOf(args[1]);
        start(algorithm, probeSequence);
        // Tests.stopWatchTest();
        //Tests.findWrongProbeSequenceTest();
    }

    public static void start(Algorithm algorithm, ProbeSequence probeSequence) {
        try {
            StreamingServer streamingServer = new StreamingServer(algorithm, probeSequence);
            ServerSocket serverSocket = new ServerSocket(4000);
            while (true) {
                streamingServer.startService(serverSocket);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
