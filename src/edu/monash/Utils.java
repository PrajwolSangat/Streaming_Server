package edu.monash;

import java.util.*;

/**
 * Created by psangats on 12/07/2017.
 */
public final class Utils {
    private static final long MEGABYTE = 1024L * 1024L;

    public static BitSet getAllBits() {
        BitSet allBits = new BitSet(4);
        allBits.set(0, 4);
        return allBits;
    }

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    public static List<Tuple<Integer, HashMap>> getOptimalProbeSequence(HashMap... lists) {
        List<Tuple<Integer, HashMap>> orderedList = new ArrayList<>();
        for (HashMap list : lists
                ) {
            orderedList.add(new Tuple<>(list.size(), list));
        }
        Collections.sort(orderedList, Comparator.comparing(p -> p.getFirst()));
        return orderedList;
    }
    public static List<Tuple<Integer, HashMap>> getWrongProbeSequence(HashMap... lists) {
        List<Tuple<Integer, HashMap>> orderedList = new ArrayList<>();
        for (HashMap list : lists
                ) {
            orderedList.add(new Tuple<>(list.size(), list));
        }
        Collections.sort(orderedList, Comparator.comparing(p -> p.getFirst(), Collections.reverseOrder()));
        return orderedList;
    }
    public static List<Tuple<Integer, HashMap>> getFixedProbeSequence(String stream, HashMap... lists) {
        List<Tuple<Integer, HashMap>> orderedList = new ArrayList<>();
        switch (stream) {
            case "R":
                orderedList.add(new Tuple<>(0, lists[1]));
                orderedList.add(new Tuple<>(1, lists[2]));
                orderedList.add(new Tuple<>(2, lists[3]));
                orderedList.add(new Tuple<>(3, lists[4]));
                break;
            case "S":
                orderedList.add(new Tuple<>(0, lists[0]));
                orderedList.add(new Tuple<>(1, lists[2]));
                orderedList.add(new Tuple<>(2, lists[3]));
                orderedList.add(new Tuple<>(3, lists[4]));
                break;
            case "T":
                orderedList.add(new Tuple<>(0, lists[0]));
                orderedList.add(new Tuple<>(1, lists[1]));
                orderedList.add(new Tuple<>(2, lists[3]));
                orderedList.add(new Tuple<>(3, lists[4]));
                break;
            case "U":
                orderedList.add(new Tuple<>(0, lists[0]));
                orderedList.add(new Tuple<>(1, lists[1]));
                orderedList.add(new Tuple<>(2, lists[2]));
                orderedList.add(new Tuple<>(3, lists[4]));
                break;
            case "V":
                orderedList.add(new Tuple<>(0, lists[0]));
                orderedList.add(new Tuple<>(1, lists[1]));
                orderedList.add(new Tuple<>(2, lists[2]));
                orderedList.add(new Tuple<>(3, lists[3]));
                break;
        }
        return orderedList;
    }
}
