package edu.monash;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.TreeMap;

public class Main {

    public static void main(String[] args) {
        //test2();
        start();
    }

    public static void start() {
        try {
            StreamingServer streamingServer = new StreamingServer();
            ServerSocket serverSocket = new ServerSocket(4000);
            while (true) {
                streamingServer.startService(serverSocket);
            }

        } catch (Exception ex) {

            System.out.println(ex.toString());
        }
    }

    public void test1() {
        ArrayList<String> a1 = new ArrayList<>();
        a1.add("1");

        ArrayList<String> a2 = new ArrayList<>();
        a2.add("1");
        a2.add("2");

        ArrayList<String> a3 = new ArrayList<>();
        a3.add("1");
        a3.add("2");
        a3.add("3");


        TreeMap<Integer, ArrayList<String>> sortedMap =
                Utils.findJoinOrder(a2, a1, a3);

        for (Integer itemSize : sortedMap.keySet()
                ) {
            System.out.println(sortedMap.get(itemSize));
        }
    }

    public static void test2() {
        HashMap<String, String> a1 = new HashMap<>();
        a1.put("1", "1");

        HashMap<String, String> a2 = new HashMap<>();
        a2.put("1", "1");
        a2.put("2", "1");

        HashMap<String, String> a3 = new HashMap<>();
        a3.put("1", "1");
        a3.put("2", "1");
        a3.put("3", "1");


        HashMap[] sortedMap =
                Utils.findJoinOrder(a2, a1, a3);


        if (sortedMap[0].containsKey("1") && sortedMap[1].containsKey("1") && sortedMap[2].containsKey("1")) {
            System.out.println(sortedMap[1].get("2"));
        }
    }
}
