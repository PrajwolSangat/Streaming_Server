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

    public static TreeMap<Integer, ArrayList<String>> findJoinOrder(ArrayList<String>... lists) {
        TreeMap<Integer, ArrayList<String>> orderedList = new TreeMap<>();
        for (ArrayList<String> list : lists
                ) {
            orderedList.put(list.size(), list);
        }
        return orderedList;
    }

    public static long bytesToMegabytes(long bytes) {
        return bytes / MEGABYTE;
    }

    public static List<Pair<Integer, HashMap>> findOptimalJoinOrder(HashMap... lists) {
        List<Pair<Integer, HashMap>> orderedList = new ArrayList<>();
        for (HashMap list : lists
                ) {
            orderedList.add(new Pair<>(list.size(), list));
        }
        Collections.sort(orderedList, Comparator.comparing(p -> p._1()));
        return orderedList;
    }

    public static HashMap[] findJoinOrder(HashMap... lists) {
        TreeMap<Integer, HashMap> orderedList = new TreeMap<>();
        int counter = 0;
        ArrayList<Integer> previousListSizes = new ArrayList<>();
        for (HashMap list : lists
                ) {
            if (list.size() == 0) {
                orderedList.put(counter++, list);
            } else {
                if (previousListSizes.contains(list.size())) {
                    orderedList.put(list.size() + 1, list);
                    previousListSizes.add(list.size() + 1);
                } else {
                    orderedList.put(list.size(), list);
                    previousListSizes.add(list.size());
                }
            }
        }
        return orderedList.values().toArray(lists);
    }

    public static HashMap[] getFixedOrder(String stream, HashMap... lists) {
        HashMap[] hmap = new HashMap[3];
        if (stream.equals("R")) {
            hmap[0] = lists[1];
            hmap[1] = lists[2];
            hmap[2] = lists[3];
        } else if (stream.equals("S")) {
            hmap[0] = lists[0];
            hmap[1] = lists[2];
            hmap[2] = lists[3];
        } else if (stream.equals("T")) {
            hmap[0] = lists[0];
            hmap[1] = lists[1];
            hmap[2] = lists[3];
        } else if (stream.equals("U")) {
            hmap[0] = lists[0];
            hmap[1] = lists[1];
            hmap[2] = lists[2];
        }
        return hmap;
    }
}
