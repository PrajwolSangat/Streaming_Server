package edu.monash;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by psangats on 7/07/2017.
 */
public class StreamingServer {
    private StreamingAlgorithms streamingAlgorithms = new StreamingAlgorithms();

    public void startService(ServerSocket serverSocket) {
        Socket connectionSocket;
        try {
            //System.out.println("Streaming Server is listening ...");
            connectionSocket = serverSocket.accept();

            //START THREAD
            Thread thread = new Thread(() -> receiveTuples(connectionSocket));
            thread.start();
        } catch (Exception ex) {

        }
    }

    private void receiveTuples(Socket socket) {
        try {
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String messageRead;
            while ((messageRead = inFromClient.readLine()) != null) {
                String streamName = messageRead.split(":")[0];
                String key = messageRead.split(":")[1];
                String value = messageRead.split(":")[2];
                Algorithm algorithm = Algorithm.valueOf(messageRead.split(":")[3]);
               // System.out.println(messageRead);
                switch (streamName) {
                    case "R":
                        executeJoins(algorithm, key, value, streamName);
                        break;
                    case "S":
                        executeJoins(algorithm, key, value, streamName);
                        break;
                    case "T":
                        executeJoins(algorithm, key, value, streamName);
                        break;
                    case "U":
                        executeJoins(algorithm, key, value, streamName);
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void executeJoins(Algorithm algorithm, String key, String value, String streamName) {
        switch (algorithm) {
            case AMJOIN:
                streamingAlgorithms.amJoin(key, value, "CA", streamName);
                break;
            case EHJOIN:
                if (key.equals("CLEANUP")) {
                    System.out.println("Received Cleanup Request.");
//                    Thread thread = new Thread(() -> streamingAlgorithms.earlyHashJoinCleanUp());
//                    thread.start();
                } else {
                    streamingAlgorithms.earlyHashJoin(key, value, "1M", streamName);
                }
                break;
            case SLICEJOIN:
                streamingAlgorithms.sliceJoin(key, value, "CA", streamName);
                break;
            case XJOIN:
                streamingAlgorithms.xJoin(key, value, "CA", streamName);
                break;
            case MJOIN:
                streamingAlgorithms.mJoin(key, value, "CA", streamName);
                break;
        }

    }
}
