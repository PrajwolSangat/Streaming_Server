package edu.monash;

import java.net.ServerSocket;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        //findOptimalJoinOrderTest();
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

    public static void findOptimalJoinOrderTest(){
        HashMap<String, String> a1 = new HashMap<>();
        HashMap<String, String> a2 = new HashMap<>();
        HashMap<String, String> a3 = new HashMap<>();

        // Test case 1: All hash map is empty

        // Test case 2: All hash map has different size
        a1.put("1", "1");
        a2.put("1", "1");
        a2.put("2", "1");
        a3.put("1", "1");
        a3.put("2", "1");
        a3.put("3", "1");

        // Test case 2: Some of them have same size
        a3.remove("2");
        a3.remove("3");

        List<Pair<Integer, HashMap>> sortedMap =
                Utils.findOptimalJoinOrder(a2, a1, a3);


        if (sortedMap.get(0)._2().containsKey("1") && sortedMap.get(1)._2().containsKey("1") && sortedMap.get(2)._2().containsKey("1")) {
            System.out.println(sortedMap.get(1)._2().get("1"));
        }
    }
    public static void test3(){
        MultiValueMap mvm = new MultiValueMap();
        mvm.putValue("1", "First");
        mvm.putValue("2", "Second");
        mvm.putValue("1", "Third");

        System.out.println(mvm.getValues("1"));

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
