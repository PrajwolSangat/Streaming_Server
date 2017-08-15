package edu.monash;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by psangats on 7/07/2017.
 */
public class StreamingAlgorithms {
    private HashMap hashTableR = new HashMap();
    private HashMap hashTableS = new HashMap();
    private HashMap hashTableT = new HashMap();
    private HashMap hashTableU = new HashMap();
    private HashMap indirectPartitionMapper = new HashMap();

    // Used for Biased Flushing Policy in Early Hash Join
    private Integer MAX_HASH_TABLE_SIZE = 30;
    private HashMap hashTableCollectionR = new HashMap();
    private HashMap hashTableCollectionS = new HashMap();

    private HashMap<String, LinkedList<QuadTuple>[]> hashTableRS = new HashMap();
    private HashMap<String, LinkedList<QuadTuple>[]> hashTableRST = new HashMap();
    private HashMap<String, BitSet> hashTableVector = new HashMap();

    private Integer integerTimeStamp = 0;

    private boolean isFirst = true;
    private boolean isInitialResponse = true;
    private long finalArrivalTimeStamp;
    private long initialResponseTimeStamp;
    private StopWatch sw = new StopWatch();
    private int totalBits = 3000000;
    private BitSet bitSet = new BitSet(totalBits);

    public void initialiseBitSetToTrue() {
        for (int i = 0; i < totalBits; i++
                ) {
            bitSet.set(i, true);
        }
    }

    public void xJoin(String key, String value, String joinType, String whichStream) {
        if (isFirst) {
            sw.start();
            isFirst = false;
        }
        if (joinType.equals("CA")) {
            LinkedList<QuadTuple<String, String, Integer, Integer>> ll_quad = new LinkedList<>();
            QuadTuple<String, String, Integer, Integer> quadTuple = new QuadTuple<>(key, value, integerTimeStamp, -1);
            integerTimeStamp++;
            switch (whichStream) {
                case "R":
                    if (key.equals("COMPLETE")) {
                        sw.stop();
                        finalArrivalTimeStamp = sw.elaspsedTime(TimeUnit.SECONDS);
                        System.out.println("[XJOIN] Execution Time: " + finalArrivalTimeStamp + " Secs");
                        System.out.println("[XJOIN] Initial Response Time: " + initialResponseTimeStamp + " nano Secs");
                        return;
                    }
                    if (hashTableR.containsKey(key)) {
                        ll_quad = (LinkedList<QuadTuple<String, String, Integer, Integer>>) hashTableR.get(key);
                    }
                    ll_quad.add(quadTuple);
                    hashTableR.put(key, ll_quad);
                    if (hashTableS.containsKey(key)) {
                        if (hashTableRS.containsKey(key)) {
                            hashTableRS.get(key)[0].add(quadTuple);
                        } else {
                            hashTableRS.put(key, new LinkedList[]{ll_quad, new LinkedList<>()});
                        }
                        if (hashTableT.containsKey(key)) {
                            if (hashTableRST.containsKey(key)) {
                                hashTableRST.get(key)[0].add(quadTuple);
                            } else {
                                hashTableRST.put(key, new LinkedList[]{ll_quad, new LinkedList<>(), new LinkedList<>()});
                            }
                            if (hashTableU.containsKey(key)) {
                                if (isInitialResponse) {
                                    sw.stop();
                                    initialResponseTimeStamp = sw.elaspsedTime();
                                    isInitialResponse = false;
                                }
                                System.out.println(String.format("[XOutput R]: %s, %s, %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                            }
                        }
                    }
                    break;
                case "S":
                    if (hashTableS.containsKey(key)) {
                        ll_quad = (LinkedList<QuadTuple<String, String, Integer, Integer>>) hashTableS.get(key);
                    }
                    ll_quad.add(quadTuple);
                    hashTableS.put(key, ll_quad);
                    if (hashTableR.containsKey(key)) {
                        if (hashTableRS.containsKey(key)) {
                            hashTableRS.get(key)[1].add(quadTuple);
                        } else {
                            hashTableRS.put(key, new LinkedList[]{new LinkedList<>(), ll_quad});
                        }
                        if (hashTableT.containsKey(key)) {
                            if (hashTableRST.containsKey(key)) {
                                hashTableRST.get(key)[1].add(quadTuple);
                            } else {
                                hashTableRST.put(key, new LinkedList[]{new LinkedList<>(), ll_quad, new LinkedList<>()});
                            }
                            if (hashTableU.containsKey(key)) {
                                if (isInitialResponse) {
                                    sw.stop();
                                    initialResponseTimeStamp = sw.elaspsedTime();
                                    isInitialResponse = false;
                                }
                                System.out.println(String.format("[XOutput S]: %s, %s, %s, %s, %s", hashTableR.get(key), key, value, hashTableT.get(key), hashTableU.get(key)));
                            }
                        }
                    }
                    break;
                case "T":
                    if (hashTableT.containsKey(key)) {
                        ll_quad = (LinkedList<QuadTuple<String, String, Integer, Integer>>) hashTableT.get(key);
                    }
                    ll_quad.add(quadTuple);
                    hashTableT.put(key, ll_quad);
                    if (hashTableRS.containsKey(key)) {
                        if (hashTableRST.containsKey(key)) {
                            hashTableRST.get(key)[2].add(quadTuple);
                        } else {
                            hashTableRST.put(key, new LinkedList[]{new LinkedList<>(), new LinkedList<>(), ll_quad});
                        }
                        if (hashTableU.containsKey(key)) {
                            if (isInitialResponse) {
                                sw.stop();
                                initialResponseTimeStamp = sw.elaspsedTime();
                                isInitialResponse = false;
                            }
                            System.out.println(String.format("[XOutput T]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), key, value, hashTableU.get(key)));
                        }
                    }
                    break;
                case "U":
                    if (hashTableU.containsKey(key)) {
                        ll_quad = (LinkedList<QuadTuple<String, String, Integer, Integer>>) hashTableU.get(key);
                    }
                    ll_quad.add(quadTuple);
                    hashTableU.put(key, ll_quad);
                    if (hashTableRST.containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[XOutput U]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), hashTableT.get(key), key, value));
                    }
                    break;
            }
        }
    }

    public void mJoin(String key, String value, String joinType, String whichStream, boolean isJoinOrdered) {
        if (isFirst) {
            sw.start();
            isFirst = false;
        }
        if (joinType.equals("CA")) {
            QuadTuple<String, String, Integer, Integer> quadTuple = new QuadTuple<>(key, value, integerTimeStamp, -1);
            LinkedList<QuadTuple<String, String, Integer, Integer>> ll_quad = new LinkedList<>();
            integerTimeStamp++;
            switch (whichStream) {
                case "R":
                    if (key.equals("COMPLETE")) {
                        sw.stop();
                        finalArrivalTimeStamp = sw.elaspsedTime(TimeUnit.SECONDS);
                        System.out.println("[MJOIN] Execution Time: " + finalArrivalTimeStamp + " Secs");
                        System.out.println("[MJOIN] Initial Response Time: " + initialResponseTimeStamp + " nano Secs");
                        return;
                    }
                    if (hashTableR.containsKey(key)) {
                        ll_quad = (LinkedList<QuadTuple<String, String, Integer, Integer>>) hashTableR.get(key);
                    }
                    ll_quad.add(quadTuple);
                    hashTableR.put(key, ll_quad);
                    List<Tuple<Integer, HashMap>> orderedMapsR;
                    if (isJoinOrdered) {
                        orderedMapsR = Utils.getOptimalJoinOrder(hashTableS, hashTableT, hashTableU);
                    } else {
                        orderedMapsR = Utils.getFixedJoinOrder("R", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsR.get(0).getSecond().containsKey(key) && orderedMapsR.get(1).getSecond().containsKey(key) && orderedMapsR.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[MOutput R]: %s, %s, %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                    }
                    break;
                case "S":
                    if (hashTableS.containsKey(key)) {
                        ll_quad = (LinkedList<QuadTuple<String, String, Integer, Integer>>) hashTableS.get(key);
                    }
                    ll_quad.add(quadTuple);
                    hashTableS.put(key, ll_quad);
                    List<Tuple<Integer, HashMap>> orderedMapsS;
                    if (isJoinOrdered) {
                        orderedMapsS = Utils.getOptimalJoinOrder(hashTableR, hashTableT, hashTableU);
                    } else {
                        orderedMapsS = Utils.getFixedJoinOrder("S", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsS.get(0).getSecond().containsKey(key) && orderedMapsS.get(1).getSecond().containsKey(key) && orderedMapsS.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[MOutput S]: %s, %s, %s, %s, %s", hashTableR.get(key), key, value, hashTableT.get(key), hashTableU.get(key)));
                    }
                    break;
                case "T":
                    if (hashTableT.containsKey(key)) {
                        ll_quad = (LinkedList<QuadTuple<String, String, Integer, Integer>>) hashTableT.get(key);
                    }
                    ll_quad.add(quadTuple);
                    hashTableT.put(key, ll_quad);
                    List<Tuple<Integer, HashMap>> orderedMapsT;
                    if (isJoinOrdered) {
                        orderedMapsT = Utils.getOptimalJoinOrder(hashTableR, hashTableS, hashTableU);
                    } else {
                        orderedMapsT = Utils.getFixedJoinOrder("T", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsT.get(0).getSecond().containsKey(key) && orderedMapsT.get(1).getSecond().containsKey(key) && orderedMapsT.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[MOutput T]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), key, value, hashTableU.get(key)));
                    }
                    break;
                case "U":
                    if (hashTableU.containsKey(key)) {
                        ll_quad = (LinkedList<QuadTuple<String, String, Integer, Integer>>) hashTableU.get(key);
                    }
                    ll_quad.add(quadTuple);
                    hashTableU.put(key, ll_quad);
                    List<Tuple<Integer, HashMap>> orderedMapsU;
                    if (isJoinOrdered) {
                        orderedMapsU = Utils.getOptimalJoinOrder(hashTableR, hashTableS, hashTableT);
                    } else {
                        orderedMapsU = Utils.getFixedJoinOrder("U", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsU.get(0).getSecond().containsKey(key) && orderedMapsU.get(1).getSecond().containsKey(key) && orderedMapsU.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[MOutput U]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), hashTableT.get(key), key, value));
                    }
                    break;
            }
        }
    }

    public void amJoin(String key, String value, String joinType, String whichStream) {
        if (isFirst) {
            sw.start();
            isFirst = false;
        }
        if (joinType.equals("CA")) {
            TriTuple<String, String, Integer> triTuple = new TriTuple<>(key, value, integerTimeStamp);
            LinkedList<TriTuple<String, String, Integer>> ll_tri = new LinkedList<>();
            integerTimeStamp++;
            switch (whichStream) {
                case "R":
                    if (key.equals("COMPLETE")) {
                        sw.stop();
                        finalArrivalTimeStamp = sw.elaspsedTime(TimeUnit.SECONDS);
                        System.out.println("[AMJOIN] Execution Time: " + finalArrivalTimeStamp + " Secs");
                        System.out.println("[AMJOIN] Initial Response Time: " + initialResponseTimeStamp + " nano Secs");
                        return;
                    }
                    if (hashTableR.containsKey(key)) {
                        ll_tri = (LinkedList<TriTuple<String, String, Integer>>) hashTableR.get(key);
                    }
                    ll_tri.add(triTuple);
                    hashTableR.put(key, ll_tri);
                    // PROBE WITH BIT VECTOR HT
                    if (hashTableVector.containsKey(key)) {
                        BitSet vector = hashTableVector.get(key);
                        // IF THERE IS NO ENTRY -> UPDATE VECTOR
                        if (!vector.get(0))
                            vector.set(0);
                        // IF XOR 1111 -> PROBE WITH OTHER STREAMS
                        if (vector.equals(Utils.getAllBits())) {
                            if (isInitialResponse) {
                                sw.stop();
                                initialResponseTimeStamp = sw.elaspsedTime();
                                isInitialResponse = false;
                            }
                            System.out.println(String.format("[AMOutput R]: %s, %s, %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                        }
                    } else {
                        // INSERT INDEX WITH VECTOR
                        BitSet bs = new BitSet(4);
                        bs.set(0);
                        hashTableVector.put(key, bs);
                    }
                    break;
                case "S":
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableS.containsKey(key)) {
                        ll_tri = (LinkedList<TriTuple<String, String, Integer>>) hashTableS.get(key);
                    }
                    ll_tri.add(triTuple);
                    hashTableS.put(key, ll_tri);
                    // PROBE WITH BIT VECTOR HT
                    if (hashTableVector.containsKey(key)) {
                        BitSet vector = hashTableVector.get(key);
                        // IF THERE IS NO ENTRY -> UPDATE VECTOR
                        if (!vector.get(1))
                            vector.set(1);
                        // IF XOR 1111 -> PROBE WITH OTHER STREAMS
                        if (vector.equals(Utils.getAllBits())) {
                            if (isInitialResponse) {
                                sw.stop();
                                initialResponseTimeStamp = sw.elaspsedTime();
                                isInitialResponse = false;
                            }
                            System.out.println(String.format("[AMOutput S]: %s, %s, %s, %s, %s", hashTableR.get(key), key, value, hashTableT.get(key), hashTableU.get(key)));
                        }
                    } else {
                        // INSERT INDEX WITH VECTOR
                        BitSet bs = new BitSet(4);
                        bs.set(1);
                        hashTableVector.put(key, bs);
                    }
                    break;
                case "T":
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableT.containsKey(key)) {
                        ll_tri = (LinkedList<TriTuple<String, String, Integer>>) hashTableT.get(key);
                    }
                    ll_tri.add(triTuple);
                    hashTableT.put(key, ll_tri);
                    // PROBE WITH BIT VECTOR HT
                    if (hashTableVector.containsKey(key)) {
                        BitSet vector = hashTableVector.get(key);
                        // IF THERE IS NO ENTRY -> UPDATE VECTOR
                        if (!vector.get(2))
                            vector.set(2);
                        // IF XOR 1111 -> PROBE WITH OTHER STREAMS
                        if (vector.equals(Utils.getAllBits())) {
                            if (isInitialResponse) {
                                sw.stop();
                                initialResponseTimeStamp = sw.elaspsedTime();
                                isInitialResponse = false;
                            }
                            System.out.println(String.format("[AMOutput T]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), key, value, hashTableU.get(key)));
                        }
                    } else {
                        // INSERT INDEX WITH VECTOR
                        BitSet bs = new BitSet(4);
                        bs.set(2);
                        hashTableVector.put(key, bs);
                    }
                    break;
                case "U":
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableU.containsKey(key)) {
                        ll_tri = (LinkedList<TriTuple<String, String, Integer>>) hashTableU.get(key);
                    }
                    ll_tri.add(triTuple);
                    hashTableU.put(key, ll_tri);
                    // PROBE WITH BIT VECTOR HT
                    if (hashTableVector.containsKey(key)) {
                        BitSet vector = hashTableVector.get(key);
                        // IF THERE IS NO ENTRY -> UPDATE VECTOR
                        if (!vector.get(3))
                            vector.set(3);
                        // IF XOR 1111 -> PROBE WITH OTHER STREAMS
                        if (vector.equals(Utils.getAllBits())) {
                            if (isInitialResponse) {
                                sw.stop();
                                initialResponseTimeStamp = sw.elaspsedTime();
                                isInitialResponse = false;
                            }
                            System.out.println(String.format("[AMOutput U]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), hashTableT.get(key), key, value));
                        }
                    } else {
                        // INSERT INDEX WITH VECTOR
                        BitSet bs = new BitSet(4);
                        bs.set(3);
                        hashTableVector.put(key, bs);
                    }
                    break;
            }
        }
    }

    public void earlyHashJoin(String key, String value, String joinType, String whichStream) {
        if (isFirst) {
            sw.start();
            isFirst = false;
        }
        // One to Many Join
        if (joinType.equals("1M")) {
            switch (whichStream) {
                case "R":
                    if (key.equals("COMPLETE")) {
                        sw.stop();
                        finalArrivalTimeStamp = sw.elaspsedTime(TimeUnit.SECONDS);
                        System.out.println("[EHJOIN] Execution Time: " + finalArrivalTimeStamp + " Secs");
                        System.out.println("[EHJOIN] Initial Response Time: " + initialResponseTimeStamp + " nano Secs");
                        return;
                    }
                    integerTimeStamp++;
                    if (hashTableS.containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output R]: %s, %s, %s", key, value, hashTableS.get(key).toString()));
                        LinkedList<String> al = new LinkedList<>();
                        al.add(value);
                        hashTableR.put(key, al);
                        hashTableS.remove(key);
                    } else {
                        LinkedList<String> al = new LinkedList<>();
                        al.add(value);
                        hashTableR.put(key, al);
                    }
                    break;
                case "S":
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key)) {
                        if (isInitialResponse) {
                            initialResponseTimeStamp = System.currentTimeMillis();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output S]: %s, %s, %s", key, hashTableR.get(key).toString(), value));
                    } else {
                        LinkedList<String> al = new LinkedList<>();
                        al.add(value);
                        hashTableS.put(key, al);
                    }
                    break;
            }
        } else if (joinType.equals("MM")) {
            // Many to Many
            switch (whichStream) {
                case "R":
                    if (key.equals("COMPLETE")) {
                        sw.stop();
                        finalArrivalTimeStamp = sw.elaspsedTime(TimeUnit.SECONDS);
                        System.out.println("[EHJOIN] Execution Time: " + finalArrivalTimeStamp + " Secs");
                        System.out.println("[EHJOIN] Initial Response Time: " + initialResponseTimeStamp + " nano Secs");
                        return;
                    }
                    integerTimeStamp++;
                    if (hashTableS.containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output R]: %s, %s, %s", key, "[" + integerTimeStamp + ", " + value + "]", hashTableS.get(key).toString()));
                    }
                    if (hashTableR.containsKey(key)) {
                        LinkedList<Tuple<Integer, String>> linkedList = (LinkedList<Tuple<Integer, String>>) hashTableR.get(key);
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableR.put(key, linkedList);

                    } else {
                        LinkedList<Tuple<Integer, String>> linkedList = new LinkedList<>();
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableR.put(key, linkedList);
                    }

                    break;

                case "S":
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key)) {
                        if (isInitialResponse) {
                            initialResponseTimeStamp = System.currentTimeMillis();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output S]: %s, %s, %s", key, hashTableR.get(key).toString(), "[" + integerTimeStamp + ", " + value + "]"));
                    }
                    if (hashTableS.containsKey(key)) {
                        LinkedList<Tuple<Integer, String>> linkedList = (LinkedList<Tuple<Integer, String>>) hashTableS.get(key);
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableS.put(key, linkedList);
                    } else {
                        LinkedList<Tuple<Integer, String>> linkedList = new LinkedList<>();
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableS.put(key, linkedList);
                    }
                    break;
            }
        }

        // Biased Flushing Policy
        // Integer hashTableSize = 0;
        //Long freeMemory = Utils.bytesToMegabytes(Runtime.getRuntime().freeMemory());
        //System.out.println("Free Memory: " + freeMemory);
        //if (freeMemory <= 150) {
//        if (hashTableS.size() > MAX_HASH_TABLE_SIZE) { // Using it for simulation
//            HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableRClone = (HashMap<String, LinkedList<Tuple<Integer, String>>>) hashTableR.clone();
//            hashTableCollectionR.put(integerTimeStamp, hashTableRClone);
//            try {
//                FileOutputStream fileOutputStream = new FileOutputStream("FlushOut\\S\\" + integerTimeStamp + ".ser");
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
//                objectOutputStream.writeObject(hashTableS.clone());
//                objectOutputStream.close();
//                hashTableR.clear();
//                hashTableS.clear();
//            } catch (Exception ex) {
//                System.out.println(ex.toString());
//            }
//        } else if (hashTableR.size() > MAX_HASH_TABLE_SIZE) {
//            HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableSClone = (HashMap<String, LinkedList<Tuple<Integer, String>>>) hashTableS.clone();
//            hashTableCollectionS.put(integerTimeStamp, hashTableSClone);
//            try {
//                FileOutputStream fileOutputStream = new FileOutputStream("FlushOut\\R\\" + integerTimeStamp + ".ser");
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
//                objectOutputStream.writeObject(hashTableR.clone());
//                objectOutputStream.close();
//                hashTableR.clear();
//                hashTableS.clear();
//            } catch (Exception ex) {
//                System.out.println(ex.toString());
//            }
//        }
    }

    public void earlyHashJoinModified(String key, String value, String joinType, String whichStream, boolean isJoinOrdered) {
        if (isFirst) {
            sw.start();
            isFirst = false;
        }
        // One to Many Join
        if (joinType.equals("1M")) {
            switch (whichStream) {
                case "R":
                    if (key.equals("COMPLETE")) {
                        sw.stop();
                        finalArrivalTimeStamp = sw.elaspsedTime(TimeUnit.SECONDS);
                        System.out.println("[EHJOIN] Execution Time: " + finalArrivalTimeStamp + " Secs");
                        System.out.println("[EHJOIN] Initial Response Time: " + initialResponseTimeStamp + " nano Secs");
                        return;
                    }
                    integerTimeStamp++;
                    if (hashTableS.containsKey(key) && hashTableT.containsKey(key) && hashTableU.containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output R]: %s, %s, %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                        LinkedList<String> al = new LinkedList<>();
                        hashTableR.put(key, al);
                        hashTableS.remove(key);
                        hashTableT.remove(key);
                        hashTableU.remove(key);
                    } else {
                        LinkedList<String> al = new LinkedList<>();
                        al.add(value);
                        hashTableR.put(key, al);
                    }
                    break;
                case "S":
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key) && hashTableT.containsKey(key) && hashTableU.containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output S]: %s, %s, %s, %s, %s", hashTableR.get(key), key, value, hashTableT.get(key), hashTableU.get(key)));
                    } else {
                        LinkedList<String> al = new LinkedList<>();
                        al.add(value);
                        hashTableS.put(key, al);
                    }
                    break;
                case "T":
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key) && hashTableS.containsKey(key) && hashTableU.containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output T]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), key, value, hashTableU.get(key)));
                    } else {
                        LinkedList<String> al = new LinkedList<>();
                        al.add(value);
                        hashTableT.put(key, al);
                    }
                    break;
                case "U":
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key) && hashTableS.containsKey(key) && hashTableT.containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output T]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), hashTableT.get(key), key, value));
                    } else {
                        LinkedList<String> al = new LinkedList<>();
                        al.add(value);
                        hashTableU.put(key, al);
                    }
                    break;
            }
        } else if (joinType.equals("MM")) {
            // Many to Many
            switch (whichStream) {
                case "R":
                    if (key.equals("COMPLETE")) {
                        sw.stop();
                        finalArrivalTimeStamp = sw.elaspsedTime(TimeUnit.SECONDS);
                        System.out.println("[EHJOIN] Execution Time: " + finalArrivalTimeStamp + " Secs");
                        System.out.println("[EHJOIN] Initial Response Time: " + initialResponseTimeStamp + " micro Secs");
                        return;
                    }
                    integerTimeStamp++;

                    List<Tuple<Integer, HashMap>> orderedMapsR;
                    if (isJoinOrdered) {
                        orderedMapsR = Utils.getOptimalJoinOrder(hashTableS, hashTableT, hashTableU);
                    } else {
                        orderedMapsR = Utils.getFixedJoinOrder("R", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsR.get(0).getSecond().containsKey(key) && orderedMapsR.get(1).getSecond().containsKey(key) && orderedMapsR.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime(TimeUnit.MILLISECONDS);
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output R]: %s, [%s], %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                    }
                    if (hashTableR.containsKey(key)) {
                        LinkedList<Tuple<Integer, String>> linkedList = (LinkedList<Tuple<Integer, String>>) hashTableR.get(key);
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableR.put(key, linkedList);

                    } else {
                        LinkedList<Tuple<Integer, String>> linkedList = new LinkedList<>();
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableR.put(key, linkedList);
                    }
                    break;

                case "S":
                    integerTimeStamp++;
                    List<Tuple<Integer, HashMap>> orderedMapsS;
                    if (isJoinOrdered) {
                        orderedMapsS = Utils.getOptimalJoinOrder(hashTableR, hashTableT, hashTableU);
                    } else {
                        orderedMapsS = Utils.getFixedJoinOrder("S", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsS.get(0).getSecond().containsKey(key) && orderedMapsS.get(1).getSecond().containsKey(key) && orderedMapsS.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime(TimeUnit.MILLISECONDS);
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output S]: %s, %s, [%s], %s, %s",  key, hashTableR.get(key), value, hashTableT.get(key), hashTableU.get(key)));
                    }
                    if (hashTableS.containsKey(key)) {
                        LinkedList<Tuple<Integer, String>> linkedList = (LinkedList<Tuple<Integer, String>>) hashTableS.get(key);
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableS.put(key, linkedList);
                    } else {
                        LinkedList<Tuple<Integer, String>> linkedList = new LinkedList<>();
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableS.put(key, linkedList);
                    }
                    break;
                case "T":
                    integerTimeStamp++;
                    List<Tuple<Integer, HashMap>> orderedMapsT;
                    if (isJoinOrdered) {
                        orderedMapsT = Utils.getOptimalJoinOrder(hashTableR, hashTableS, hashTableU);
                    } else {
                        orderedMapsT = Utils.getFixedJoinOrder("T", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsT.get(0).getSecond().containsKey(key) && orderedMapsT.get(1).getSecond().containsKey(key) && orderedMapsT.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime(TimeUnit.MILLISECONDS);
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output S]: %s, %s, %s, [%s], %s",  key, hashTableR.get(key), hashTableS.get(key), value, hashTableU.get(key)));
                    }
                    if (hashTableT.containsKey(key)) {
                        LinkedList<Tuple<Integer, String>> linkedList = (LinkedList<Tuple<Integer, String>>) hashTableS.get(key);
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableT.put(key, linkedList);
                    } else {
                        LinkedList<Tuple<Integer, String>> linkedList = new LinkedList<>();
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableT.put(key, linkedList);
                    }
                    break;
                case "U":
                    integerTimeStamp++;
                    List<Tuple<Integer, HashMap>> orderedMapsU;
                    if (isJoinOrdered) {
                        orderedMapsU = Utils.getOptimalJoinOrder(hashTableR, hashTableS, hashTableT);
                    } else {
                        orderedMapsU = Utils.getFixedJoinOrder("U", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsU.get(0).getSecond().containsKey(key) && orderedMapsU.get(1).getSecond().containsKey(key) && orderedMapsU.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime(TimeUnit.MILLISECONDS);
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[EHJ Output S]: %s, %s, %s, %s, [%s]",  key, hashTableR.get(key), hashTableS.get(key), hashTableT.get(key), value));
                    }
                    if (hashTableU.containsKey(key)) {
                        LinkedList<Tuple<Integer, String>> linkedList = (LinkedList<Tuple<Integer, String>>) hashTableS.get(key);
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableU.put(key, linkedList);
                    } else {
                        LinkedList<Tuple<Integer, String>> linkedList = new LinkedList<>();
                        Tuple<Integer, String> tuple = new Tuple<>(integerTimeStamp, value);
                        linkedList.add(tuple);
                        hashTableU.put(key, linkedList);
                    }
                    break;
            }
        }

        // Biased Flushing Policy
        // Integer hashTableSize = 0;
        //Long freeMemory = Utils.bytesToMegabytes(Runtime.getRuntime().freeMemory());
        //System.out.println("Free Memory: " + freeMemory);
        //if (freeMemory <= 150) {
//        if (hashTableS.size() > MAX_HASH_TABLE_SIZE) { // Using it for simulation
//            HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableRClone = (HashMap<String, LinkedList<Tuple<Integer, String>>>) hashTableR.clone();
//            hashTableCollectionR.put(integerTimeStamp, hashTableRClone);
//            try {
//                FileOutputStream fileOutputStream = new FileOutputStream("FlushOut\\S\\" + integerTimeStamp + ".ser");
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
//                objectOutputStream.writeObject(hashTableS.clone());
//                objectOutputStream.close();
//                hashTableR.clear();
//                hashTableS.clear();
//            } catch (Exception ex) {
//                System.out.println(ex.toString());
//            }
//        } else if (hashTableR.size() > MAX_HASH_TABLE_SIZE) {
//            HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableSClone = (HashMap<String, LinkedList<Tuple<Integer, String>>>) hashTableS.clone();
//            hashTableCollectionS.put(integerTimeStamp, hashTableSClone);
//            try {
//                FileOutputStream fileOutputStream = new FileOutputStream("FlushOut\\R\\" + integerTimeStamp + ".ser");
//                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
//                objectOutputStream.writeObject(hashTableR.clone());
//                objectOutputStream.close();
//                hashTableR.clear();
//                hashTableS.clear();
//            } catch (Exception ex) {
//                System.out.println(ex.toString());
//            }
//        }
    }

    public void earlyHashJoinCleanUp() {
        HashMap<Integer, HashMap<String, LinkedList<Tuple<Integer, String>>>> hashTableCollectionRTemp = (HashMap<Integer, HashMap<String, LinkedList<Tuple<Integer, String>>>>) hashTableCollectionR;
        HashMap<Integer, HashMap<String, LinkedList<Tuple<Integer, String>>>> hashTableCollectionSTemp = (HashMap<Integer, HashMap<String, LinkedList<Tuple<Integer, String>>>>) hashTableCollectionS;
        try {
            File[] rStreamFiles = new File("FlushOut\\R\\").listFiles();
            File[] sStreamFiles = new File("FlushOut\\S\\").listFiles();
            for (File rStreamFile : rStreamFiles
                    ) {
                FileInputStream fileInputStream = new FileInputStream(rStreamFile);
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableR = (HashMap<String, LinkedList<Tuple<Integer, String>>>) objectInputStream.readObject();
                objectInputStream.close();
                hashTableCollectionRTemp.put(Integer.parseInt(rStreamFile.getName().substring(0, rStreamFile.getName().indexOf('.'))), hashTableR);
                if (!rStreamFile.isDirectory()) {
                    rStreamFile.delete();
                }
            }
            for (File sStreamFile : sStreamFiles
                    ) {
                FileInputStream fileInputStream = new FileInputStream(sStreamFile);
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableS = (HashMap<String, LinkedList<Tuple<Integer, String>>>) objectInputStream.readObject();
                objectInputStream.close();
                hashTableCollectionSTemp.put(Integer.parseInt(sStreamFile.getName().substring(0, sStreamFile.getName().indexOf('.'))), hashTableS);
                if (!sStreamFile.isDirectory()) {
                    sStreamFile.delete();
                }
            }

            for (Integer sKey : hashTableCollectionSTemp.keySet()
                    ) {
                for (Integer rKey : hashTableCollectionRTemp.keySet()
                        ) {
                    if (!sKey.equals(rKey)) {
                        HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableS = hashTableCollectionSTemp.get(sKey);
                        HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableR = hashTableCollectionRTemp.get(rKey);
                        for (String key : hashTableS.keySet()
                                ) {
                            if (hashTableR.containsKey(key)) {
                                System.out.println("EHJ CleanUP: " + key + ", " + hashTableR.get(key) + ", " + hashTableS.get(key));
                            }
                        }
                    } else {
                        HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableS = hashTableCollectionSTemp.get(sKey);
                        HashMap<String, LinkedList<Tuple<Integer, String>>> hashTableR = hashTableCollectionRTemp.get(rKey);
                    }
                }
            }

        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }

    public void sliceJoin(String key, String value, String joinType, String whichStream, boolean isJoinOrdered) {
        if (joinType.equals("CA")) {
            if (isFirst) {
                sw.start();
                isFirst = false;
            }
            switch (whichStream) {
                // Common Attribute Join [Key is the common attribute]
                case "R":
                    if (key.equals("COMPLETE")) {
                        sw.stop();
                        finalArrivalTimeStamp = sw.elaspsedTime(TimeUnit.SECONDS);
                        System.out.println("[SLICE JOIN] Execution Time: " + finalArrivalTimeStamp + " nano Secs");
                        System.out.println("[SLICE JOIN] Initial Response Time: " + initialResponseTimeStamp + " Secs");
                        return;
                    }
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key)) {
                        LinkedList<TriTuple<Integer, String, String>> linkedList = (LinkedList<TriTuple<Integer, String, String>>) hashTableR.get(key);
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableR.put(key, linkedList);
                    } else {
                        LinkedList<TriTuple> linkedList = new LinkedList<>();
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableR.put(key, linkedList);
                    }
                    List<Tuple<Integer, HashMap>> orderedMapsR;
                    if (isJoinOrdered) {
                        orderedMapsR = Utils.getOptimalJoinOrder(hashTableS, hashTableT, hashTableU);
                    } else {
                        orderedMapsR = Utils.getFixedJoinOrder("R", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsR.get(0).getSecond().containsKey(key) && orderedMapsR.get(1).getSecond().containsKey(key) && orderedMapsR.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[Slice Output R]: %s, %s, %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                    }
                    break;
                case "S":
                    integerTimeStamp++;
                    if (hashTableS.containsKey(key)) {
                        LinkedList<TriTuple<Integer, String, String>> linkedList = (LinkedList<TriTuple<Integer, String, String>>) hashTableS.get(key);
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableS.put(key, linkedList);
                    } else {
                        LinkedList<TriTuple> linkedList = new LinkedList<>();
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableS.put(key, linkedList);
                    }
                    List<Tuple<Integer, HashMap>> orderedMapsS;
                    if (isJoinOrdered) {
                        orderedMapsS = Utils.getOptimalJoinOrder(hashTableR, hashTableT, hashTableU);
                    } else {
                        orderedMapsS = Utils.getFixedJoinOrder("S", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsS.get(0).getSecond().containsKey(key) && orderedMapsS.get(1).getSecond().containsKey(key) && orderedMapsS.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[Slice Output S]: %s, %s, %s, %s, %s", hashTableR.get(key), key, value, hashTableT.get(key), hashTableU.get(key)));
                    }
                    break;
                case "T":
                    integerTimeStamp++;
                    if (hashTableT.containsKey(key)) {
                        LinkedList<TriTuple<Integer, String, String>> linkedList = (LinkedList<TriTuple<Integer, String, String>>) hashTableT.get(key);
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableT.put(key, linkedList);
                    } else {
                        LinkedList<TriTuple> linkedList = new LinkedList<>();
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableT.put(key, linkedList);
                    }
                    List<Tuple<Integer, HashMap>> orderedMapsT;
                    if (isJoinOrdered) {
                        orderedMapsT = Utils.getOptimalJoinOrder(hashTableR, hashTableS, hashTableU);
                    } else {
                        orderedMapsT = Utils.getFixedJoinOrder("T", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsT.get(0).getSecond().containsKey(key) && orderedMapsT.get(1).getSecond().containsKey(key) && orderedMapsT.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[Slice Output T]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), key, value, hashTableU.get(key)));
                    }
                    break;
                case "U":
                    integerTimeStamp++;
                    if (hashTableU.containsKey(key)) {
                        LinkedList<TriTuple<Integer, String, String>> linkedList = (LinkedList<TriTuple<Integer, String, String>>) hashTableU.get(key);
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableU.put(key, linkedList);
                    } else {
                        LinkedList<TriTuple> linkedList = new LinkedList<>();
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableU.put(key, linkedList);
                    }
                    List<Tuple<Integer, HashMap>> orderedMapsU;
                    if (isJoinOrdered) {
                        orderedMapsU = Utils.getOptimalJoinOrder(hashTableR, hashTableS, hashTableT);
                    } else {
                        orderedMapsU = Utils.getFixedJoinOrder("U", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsU.get(0).getSecond().containsKey(key) && orderedMapsU.get(1).getSecond().containsKey(key) && orderedMapsU.get(2).getSecond().containsKey(key)) {
                        if (isInitialResponse) {
                            sw.stop();
                            initialResponseTimeStamp = sw.elaspsedTime();
                            isInitialResponse = false;
                        }
                        System.out.println(String.format("[Slice Output U]: %s, %s, %s, %s, %s", hashTableR.get(key), hashTableS.get(key), hashTableT.get(key), key, value));
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
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key)) {
                        LinkedList<TriTuple<Integer, String, String>> linkedList = (LinkedList<TriTuple<Integer, String, String>>) hashTableR.get(key);
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableR.put(key, linkedList);
                    } else {
                        LinkedList<TriTuple> linkedList = new LinkedList<>();
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableR.put(key, linkedList);
                    }
                    List<Tuple<Integer, HashMap>> orderedMapsR;
                    if (isJoinOrdered) {
                        orderedMapsR = Utils.getOptimalJoinOrder(hashTableS, hashTableU);

                    } else {
                        orderedMapsR = Utils.getFixedJoinOrder("R", hashTableR, hashTableS, hashTableT, hashTableU);

                    }
                    if (orderedMapsR.get(0).getSecond().containsKey(key) && orderedMapsR.get(1).getSecond().containsKey(key)) {
                        // implementation of slice mapping
                        LinkedList<TriTuple<Integer, String, String>> mappingList = (LinkedList<TriTuple<Integer, String, String>>) hashTableS.get(key); // list buffer of key values
                        for (TriTuple triTuple :
                                mappingList) {
                            if (hashTableT.containsKey(triTuple.getSecond())) {
                                System.out.println(String.format("[Slice Output R]: %s, %s, %s, %s, %s", key, value, hashTableS.get(key), hashTableT.get(key), hashTableU.get(key)));
                            }
                        }
                    }
                    // implementation of slice mapping complete
                    break;
                case "S":
                    integerTimeStamp++;
                    if (hashTableS.containsKey(key)) {
                        LinkedList<TriTuple<Integer, String, String>> linkedList = (LinkedList<TriTuple<Integer, String, String>>) hashTableS.get(key);
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableS.put(key, linkedList);
                    } else {
                        LinkedList<TriTuple> linkedList = new LinkedList<>();
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableS.put(key, linkedList);
                    }

                    if (indirectPartitionMapper.containsKey(value)) {
                        // used for mapping in stream T
                        LinkedList<Tuple<String, String>> linkedList = (LinkedList<Tuple<String, String>>) indirectPartitionMapper.get(value);
                        Tuple<String, String> tuple = new Tuple<>(value, key);
                        linkedList.add(tuple);
                        indirectPartitionMapper.put(value, linkedList);
                    } else {
                        LinkedList<Tuple> linkedList = new LinkedList<>();
                        Tuple<String, String> tuple = new Tuple<>(value, key);
                        linkedList.add(tuple);
                        indirectPartitionMapper.put(value, linkedList);
                    }
                    List<Tuple<Integer, HashMap>> orderedMapsS;
                    if (isJoinOrdered) {
                        orderedMapsS = Utils.getOptimalJoinOrder(hashTableR, hashTableT, hashTableU);

                    } else {
                        orderedMapsS = Utils.getFixedJoinOrder("S", hashTableR, hashTableS, hashTableT, hashTableU);

                    }
                    if (orderedMapsS.get(0).getSecond().containsKey(key) && orderedMapsS.get(1).getSecond().containsKey(key) && orderedMapsS.get(2).getSecond().containsKey(key)) {
                        System.out.println(String.format("[Slice Output S]: %s, %s, %s, %s, %s", hashTableR.get(key), key, value, hashTableT.get(key), hashTableU.get(key)));
                    }
                    break;
                case "T":
                    integerTimeStamp++;
                    if (hashTableT.containsKey(key)) {
                        LinkedList<TriTuple<Integer, String, String>> linkedList = (LinkedList<TriTuple<Integer, String, String>>) hashTableT.get(key);
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableT.put(key, linkedList);
                    } else {
                        LinkedList<TriTuple> linkedList = new LinkedList<>();
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableT.put(key, linkedList);
                    }
                    if (indirectPartitionMapper.containsKey(key)) {
                        LinkedList<Tuple<String, String>> mappingList = (LinkedList<Tuple<String, String>>) indirectPartitionMapper.get(key);
                        List<Tuple<Integer, HashMap>> orderedMapsT;
                        if (isJoinOrdered) {
                            orderedMapsT = Utils.getOptimalJoinOrder(hashTableR, hashTableU);
                        } else {
                            orderedMapsT = Utils.getFixedJoinOrder("T", hashTableR, hashTableS, hashTableT, hashTableU);
                        }
                        for (Tuple tuple : mappingList) {
                            if (orderedMapsT.get(0).getSecond().containsKey(tuple.getSecond()) && orderedMapsT.get(1).getSecond().containsKey(tuple.getSecond())) {
                                System.out.println(String.format("[Slice Output T]:  %s, %s, %s, %s, %s", hashTableR.get(tuple.getSecond()), hashTableS.get(tuple.getSecond()), tuple.getFirst(), tuple.getSecond(), hashTableU.get(tuple.getSecond())));
                            }
                        }
                    }
                    break;
                case "U":
                    integerTimeStamp++;
                    if (hashTableU.containsKey(key)) {
                        LinkedList<TriTuple<Integer, String, String>> linkedList = (LinkedList<TriTuple<Integer, String, String>>) hashTableU.get(key);
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableU.put(key, linkedList);
                    } else {
                        LinkedList<TriTuple> linkedList = new LinkedList<>();
                        TriTuple<Integer, String, String> triTuple = new TriTuple<>(integerTimeStamp, key, value);
                        linkedList.add(triTuple);
                        hashTableU.put(key, linkedList);
                    }
                    List<Tuple<Integer, HashMap>> orderedMapsU;
                    if (isJoinOrdered) {
                        orderedMapsU = Utils.getOptimalJoinOrder(hashTableR, hashTableS);
                    } else {
                        orderedMapsU = Utils.getFixedJoinOrder("U", hashTableR, hashTableS, hashTableT, hashTableU);
                    }
                    if (orderedMapsU.get(0).getSecond().containsKey(key) && orderedMapsU.get(0).getSecond().containsKey(key)) {
                        // implementation of slice mapping
                        LinkedList<TriTuple<Integer, String, String>> mappingList = (LinkedList<TriTuple<Integer, String, String>>) hashTableS.get(key);
                        for (TriTuple triTuple : mappingList
                                ) {
                            if (hashTableT.containsKey(triTuple.getThird())) {
                                System.out.println(String.format("[Slice Output U]: %s, %s, %s, %s, %s", hashTableR.get(key), mappingList, hashTableT.get(triTuple.getThird()), key, value));
                            }
                        }
                        // implementation of slice mapping complete
                    }
                    break;
            }
        }
    }
}

