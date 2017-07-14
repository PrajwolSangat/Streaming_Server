package edu.monash;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by psangats on 7/07/2017.
 */
public class StreamingAlgorithms {
    HashMap hashTableR = new HashMap();
    HashMap hashTableS = new HashMap();
    HashMap hashTableT = new HashMap();
    HashMap hashTableU = new HashMap();
    HashMap indirectPartitionMapper = new HashMap();

    HashMap hashTableRS = new HashMap();
    HashMap hashTableRST = new HashMap();

    Integer integerTimeStamp = 0;

    public void xJoin(String key, String value, String joinType, String whichStream) {
        if (joinType.equals("CA")) {
            switch (whichStream) {
                // Common Attribute Join [Key is the common attribute]
                case "R":
                    integerTimeStamp += 1;
                    ArrayList<Triplet> arrayList = new ArrayList<>();
                    if (hashTableR.containsKey(key)) {
                        hashTableR.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableR.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableR.put(key, arrayList);
                    }
                    //TODO
                    break;
                case "S":
                    break;
                case "T":
                    break;
                case "U":
                    break;
            }
        }
    }

    public void earlyHashJoin(String key, String value, String joinType, String whichStream) {

        // One to Many Join
        if (joinType.equals("1M")) {
            switch (whichStream) {
                case "R":

                    if (hashTableS.containsKey(key)) {
                        System.out.println(String.format("%s, %s, %s", key, value, hashTableS.get(key)));
                        ArrayList<String> al = new ArrayList<>();
                        al.add(value);
                        hashTableR.put(key, al);
                        hashTableS.remove(key);
                    } else {
                        ArrayList<String> al = new ArrayList<>();
                        al.add(value);
                        hashTableR.put(key, al);
                    }
                    break;
                case "S":
                    if (hashTableR.containsKey(key)) {
                        System.out.println(String.format("%s, %s, %s", key, value, hashTableR.get(key)));
                    } else {
                        ArrayList<String> al = new ArrayList<>();
                        al.add(value);
                        hashTableS.put(key, al);
                    }
                    break;
            }
        } else {
            // Many to Many
            switch (whichStream) {
                case "R":
                    if (hashTableS.containsKey(key)) {
                        System.out.println(String.format("%s, %s, %s", key, value, hashTableS.get(key)));
                        if (hashTableR.containsKey(key)) {
                            hashTableR.put(key, ((ArrayList<String>) hashTableR.get(key)).add(value));

                        } else {
                            hashTableR.put(key, new ArrayList<String>().add(value));
                        }
                    } else {
                        hashTableR.put(key, new ArrayList<String>().add(value));        //WHY INSERT IN R
                    }
                    break;

                case "S":
                    if (hashTableR.containsKey(key)) {
                        System.out.println(String.format("%s, %s, %s", key, value, hashTableR.get(key)));
                        if (hashTableS.containsKey(key)) {
                            hashTableS.put(key, ((ArrayList<String>) hashTableR.get(key)).add(value));
                        } else {
                            hashTableS.put(key, new ArrayList<String>().add(value));
                        }
                    } else {
                        hashTableS.put(key, new ArrayList<String>().add(value));
                    }
                    break;
            }
        }
    }

    public void sliceJoin(String key, String value, String joinType, String whichStream) {
        if (joinType.equals("CA")) {
            switch (whichStream) {
                // Common Attribute Join [Key is the common attribute]
                case "R":
                    integerTimeStamp += 1;
                    if (hashTableR.containsKey(key)) {
                        ArrayList<Triplet<Integer, String,String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableR.get(key);
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableR.put(key, arrayList);
                    } else {
                        ArrayList<Triplet> arrayList = new ArrayList<>();
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableR.put(key, arrayList);
                    }

                    HashMap[] orderedMapsR = Utils.findJoinOrder(hashTableS, hashTableT, hashTableU);
                    if (orderedMapsR[0].containsKey(key) && orderedMapsR[1].containsKey(key) && orderedMapsR[2].containsKey(key)) {
                        System.out.println(String.format("[Output R]: %s, %s, %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                    }
                    break;
                case "S":
                    integerTimeStamp += 1;
                    if (hashTableS.containsKey(key)) {
                        ArrayList<Triplet<Integer, String,String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableS.get(key);
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableS.put(key, arrayList);
                    } else {
                        ArrayList<Triplet> arrayList = new ArrayList<>();
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableS.put(key, arrayList);
                    }

                    HashMap[] orderedMapsS = Utils.findJoinOrder(hashTableR, hashTableT, hashTableU);
                    if (orderedMapsS[0].containsKey(key) && orderedMapsS[1].containsKey(key) && orderedMapsS[2].containsKey(key)) {
                        System.out.println(String.format("[Output S]: %s, %s, %s, %s, %s", hashTableR.get(key), key, value, hashTableT.get(key), hashTableU.get(key)));
                    }
                    break;
                case "T":
                    integerTimeStamp += 1;
                    if (hashTableT.containsKey(key)) {
                        ArrayList<Triplet<Integer, String,String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableT.get(key);
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableT.put(key, arrayList);
                    } else {
                        ArrayList<Triplet> arrayList = new ArrayList<>();
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableT.put(key, arrayList);
                    }

                    HashMap[] orderedMapsT = Utils.findJoinOrder(hashTableR, hashTableS, hashTableU);
                    if (orderedMapsT[0].containsKey(key) && orderedMapsT[1].containsKey(key) && orderedMapsT[2].containsKey(key)) {
                        System.out.println(String.format("[Output T]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), key, value, hashTableU.get(key)));
                    }
                    break;
                case "U":
                    integerTimeStamp += 1;
                    if (hashTableU.containsKey(key)) {
                        ArrayList<Triplet<Integer, String,String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableU.get(key);
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableU.put(key, arrayList);
                    } else {
                        ArrayList<Triplet> arrayList = new ArrayList<>();
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableU.put(key, arrayList);
                    }

                    HashMap[] orderedMapsU = Utils.findJoinOrder(hashTableR, hashTableT, hashTableS);
                    if (orderedMapsU[0].containsKey(key) && orderedMapsU[1].containsKey(key) && orderedMapsU[2].containsKey(key)) {
                        System.out.println(String.format("[Output U]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), hashTableT.get(key), key, value));
                    }
                    break;
            }
        } else {
            // Distinct Attribute Join using mapping function
            // R -> (a,b)
            // S -> (a,c)
            // T -> (c,d)
            // U -> (a,e)

            switch (whichStream) {
                // Common Attribute Join [Key is the common attribute]
                case "R":
                    integerTimeStamp += 1;
                    if (hashTableR.containsKey(key)) {
                        ArrayList<Triplet<Integer, String,String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableR.get(key);
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableR.put(key, arrayList);
                    } else {
                        ArrayList<Triplet> arrayList = new ArrayList<>();
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableR.put(key, arrayList);
                    }

                    // Using Simple Heuristics to find the join order
                    HashMap[] orderedMapsR = Utils.findJoinOrder(hashTableS, hashTableU);
                    if (orderedMapsR[0].containsKey(key) && orderedMapsR[1].containsKey(key)) {
                        // implementation of slice mapping
                        ArrayList<Triplet<Integer, String, String>> mappingList = (ArrayList<Triplet<Integer, String, String>>) hashTableS.get(key); // list buffer of key values
                        for (Triplet triplet :
                                mappingList) {
                            if (hashTableT.containsKey(triplet.getSecond())) {
                                System.out.println(String.format("[Output R]: %s, %s, %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                            }
                        }
                    }
                    // implementation of slice mapping complete
                    break;
                case "S":
                    integerTimeStamp += 1;
                    if (hashTableS.containsKey(key)) {
                        ArrayList<Triplet<Integer, String,String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableS.get(key);
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableS.put(key, arrayList);
                    } else {
                        ArrayList<Triplet> arrayList = new ArrayList<>();
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableS.put(key, arrayList);
                    }

                    if (indirectPartitionMapper.containsKey(value)) {
                        // used for mapping in stream T
                        ArrayList<Pair<String,String>> arrayList = (ArrayList<Pair<String,String>>) indirectPartitionMapper.get(value);
                        Pair<String, String> pair = new Pair<>(value, key);
                        arrayList.add(pair);
                        indirectPartitionMapper.put(value, arrayList);
                    } else {
                        ArrayList<Pair> arrayList = new ArrayList<>();
                        Pair<String, String> pair = new Pair<>(value, key);
                        arrayList.add(pair);
                        indirectPartitionMapper.put(value, arrayList);
                    }
                    HashMap[] orderedMapsS = Utils.findJoinOrder(hashTableR, hashTableT, hashTableU);
                    if (orderedMapsS[0].containsKey(key) && orderedMapsS[1].containsKey(key) && orderedMapsS[2].containsKey(key)) {
                        System.out.println(String.format("[Output S]: %s, %s, %s, %s, %s", hashTableR.get(key), key, value, hashTableT.get(key), hashTableU.get(key)));
                    }
                    break;
                case "T":
                    integerTimeStamp += 1;
                    if (hashTableT.containsKey(key)) {
                        ArrayList<Triplet<Integer, String,String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableT.get(key);
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableT.put(key, arrayList);
                    } else {
                        ArrayList<Triplet> arrayList = new ArrayList<>();
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableT.put(key, arrayList);
                    }
                    if (indirectPartitionMapper.containsKey(key)) {
                        ArrayList<Pair<String, String>> mappingList = (ArrayList<Pair<String, String>>) indirectPartitionMapper.get(key);
                        HashMap[] orderedMapsT = Utils.findJoinOrder(hashTableR, hashTableU);
                        for (Pair pair : mappingList) {
                            if (orderedMapsT[0].containsKey(pair._2()) && orderedMapsT[1].containsKey(pair._2())) {
                                System.out.println(String.format("[Output T]:  %s, %s, %s, %s, %s", hashTableR.get(pair._2()), hashTableS.get(pair._2()), pair._1(), pair._2(), hashTableU.get(pair._2())));
                            }
                        }
                    }
                    break;
                case "U":
                    integerTimeStamp += 1;
                    if (hashTableU.containsKey(key)) {
                        ArrayList<Triplet<Integer, String,String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableU.get(key);
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableU.put(key, arrayList);
                    } else {
                        ArrayList<Triplet> arrayList = new ArrayList<>();
                        Triplet<Integer, String, String> triplet = new Triplet<>(integerTimeStamp, key, value);
                        arrayList.add(triplet);
                        hashTableU.put(key, arrayList);
                    }
                    HashMap[] orderedMapsU = Utils.findJoinOrder(hashTableR, hashTableS);
                    if (orderedMapsU[0].containsKey(key) && orderedMapsU[1].containsKey(key)) {
                        // implementation of slice mapping
                        ArrayList<Triplet<Integer, String, String>> mappingList = (ArrayList<Triplet<Integer, String, String>>) hashTableS.get(key);
                        for (Triplet triplet : mappingList
                                ) {
                            if (hashTableT.containsKey(triplet.getThird())) {
                                System.out.println(String.format("[Output U]: %s, %s, %s, %s, %s", hashTableR.get(key), mappingList, hashTableT.get(triplet.getThird()), key, value));
                            }
                        }
                        // implementation of slice mapping complete
                    }
                    break;
            }
        }
    }
}

