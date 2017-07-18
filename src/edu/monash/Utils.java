package edu.monash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by psangats on 12/07/2017.
 */
public final class Utils {
    private static final long MEGABYTE = 1024L * 1024L;

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

    public static HashMap[] findJoinOrder(HashMap... lists) {
        TreeMap<Integer, HashMap> orderedList = new TreeMap<>();
        for (HashMap list : lists
                ) {
            orderedList.put(list.size(), list);
        }
        return orderedList.values().toArray(lists);
    }
}
