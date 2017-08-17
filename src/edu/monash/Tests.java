package edu.monash;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by psangats on 7/08/2017.
 */
public  class Tests {

    public static void findOptimalProbeSequenceTest() {
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

        List<Tuple<Integer, HashMap>> sortedMap = Utils.getOptimalProbeSequence(a2, a1, a3);

        if (sortedMap.get(0).getSecond().containsKey("1") && sortedMap.get(1).getSecond().containsKey("1") && sortedMap.get(2).getSecond().containsKey("1")) {
            System.out.println(sortedMap.get(1).getSecond().get("1"));
        }
    }

    public static void findWrongProbeSequenceTest() {
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

        List<Tuple<Integer, HashMap>> sortedMap = Utils.getWrongProbeSequence(a2, a1, a3);

        if (sortedMap.get(0).getSecond().containsKey("1") && sortedMap.get(1).getSecond().containsKey("1") && sortedMap.get(2).getSecond().containsKey("1")) {
            System.out.println(sortedMap.get(1).getSecond().get("1"));
        }
    }
    public static void stopWatchTest() {
        try {
            StopWatch sw = new StopWatch();
            sw.start();
            Thread.sleep(1000 * 10);
            sw.stop();
            System.out.println("Elapsed nano time: " + sw.elaspsedTime());
            System.out.println("Elapsed nano time in ms: " + sw.elaspsedTime(TimeUnit.MILLISECONDS));
            System.out.println("Elapsed nano time in secs: " + sw.elaspsedTime(TimeUnit.SECONDS));
        } catch (Exception ex) {
        }


    }
}
