package edu.monash;

import java.io.BufferedReader;
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
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            String messageRead = inFromClient.readLine();
            String streamName = messageRead.split(":")[0];

            String key = messageRead.split(":")[1];
            String value = messageRead.split(":")[2];
            Algorithm algorithm = Algorithm.valueOf(messageRead.split(":")[3]);
            switch (streamName) {
                case "R":
                    System.out.println("=========================" + streamName);
                    executeJoins(algorithm, key, value, streamName);
                    break;
                case "S":
                    System.out.println(streamName);
                    executeJoins(algorithm, key, value, streamName);
                    break;
                case "T":
                    executeJoins(algorithm, key, value, streamName);
                    break;
                case "U":
                    executeJoins(algorithm, key, value, streamName);
                    break;

            }
        } catch (Exception ex) {

        }
    }

    private void executeJoins(Algorithm algorithm, String key, String value, String streamName) {
        switch (algorithm) {
            case AMJOIN:
                break;
            case EHJOIN:
                streamingAlgorithms.earlyHashJoin(key, value, "1M", streamName);
                break;
            case SLICEJOIN:
                streamingAlgorithms.sliceJoin(key, value, "CA", streamName);
                break;
            case XJOIN:
                break;
            case MJOIN:
                break;
        }

    }
}
