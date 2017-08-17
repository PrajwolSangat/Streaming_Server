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
    private StreamingAlgorithms streamingAlgorithms;

    public StreamingServer(Algorithm algorithm, ProbeSequence probeSequence) {
        streamingAlgorithms = new StreamingAlgorithms(algorithm, probeSequence);
    }

    public void startService(ServerSocket serverSocket) {
        Socket connectionSocket;
        try {
            connectionSocket = serverSocket.accept();
            Thread thread = new Thread(() -> receiveTuples(connectionSocket));
            thread.start();
        } catch (Exception ex) {
            ex.printStackTrace();
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
                switch (streamName) {
                    case "R":
                        executeJoins(key, value, streamName);
                        break;
                    case "S":
                        executeJoins(key, value, streamName);
                        break;
                    case "T":
                        executeJoins(key, value, streamName);
                        break;
                    case "U":
                        executeJoins(key, value, streamName);
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void executeJoins(String key, String value, String streamName) {
        switch (streamingAlgorithms.getAlgorithm()) {
            case AMJOIN:
                streamingAlgorithms.amJoin(key, value, streamName);
                break;
            case EHJOIN:
                streamingAlgorithms.earlyHashJoinModified(key, value, streamName);
                break;
            case SLICEJOIN:
                streamingAlgorithms.sliceJoin(key, value, "CA", streamName);
                break;
            case XJOIN:
                streamingAlgorithms.xJoin(key, value, streamName);
                break;
            case MJOIN:
                streamingAlgorithms.mJoin(key, value, streamName);
                break;
        }

    }
}
