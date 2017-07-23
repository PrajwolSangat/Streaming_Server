package edu.monash;

import java.io.*;
import java.util.*;

/**
 * Created by psangats on 7/07/2017.
 */
public class StreamingAlgorithms {
    HashMap hashTableR = new HashMap();
    HashMap hashTableS = new HashMap();
    HashMap hashTableT = new HashMap();
    HashMap hashTableU = new HashMap();
    HashMap indirectPartitionMapper = new HashMap();

    // Used for Biased Flushing Policy in Early Hash Join
    Integer MAX_HASH_TABLE_SIZE = 30;
    HashMap hashTableCollectionR = new HashMap();
    HashMap hashTableCollectionS = new HashMap();

    HashMap hashTableRS = new HashMap();
    HashMap hashTableRST = new HashMap();

    Integer integerTimeStamp = 0;

    public void xJoin(String key, String value, String joinType, String whichStream) {
        if (joinType.equals("CA")) {

            ArrayList<Triplet> arrayList = new ArrayList<>();
            switch (whichStream) {
                // Common Attribute Join [Key is the common attribute]
                case "R":
                    integerTimeStamp += 1;
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableR.containsKey(key)) {
                        hashTableR.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableR.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        arrayList.add(new Triplet<>(integerTimeStamp, key, value));
                        hashTableR.put(key, arrayList);
                    }
                    //PROBE WITH S STREAM HT
                    if (hashTableS.containsKey(key)) {
                        //INSERT INTO hashTableRS

                        //PROBE WITH T STREAM HT
                        if (hashTableT.containsKey(key)) {
                            //INSERT INTO hashTableRST

                            //PROBE WITH U STREAM HT
                            if (hashTableU.containsKey(key)) {
                                // PRINT RESULT
                            }
                        }
                    }
                    break;
                case "S":
                    integerTimeStamp += 1;
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableS.containsKey(key)) {
                        hashTableS.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableS.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        arrayList.add(new Triplet<>(integerTimeStamp, key, value));
                        hashTableS.put(key, arrayList);
                    }
                    //PROBE WITH R STREAM HT
                    if (hashTableR.containsKey(key)) {
                        //INSERT INTO hashTableRS

                        //PROBE WITH T STREAM HT
                        if (hashTableT.containsKey(key)) {
                            //INSERT INTO hashTableRST

                            //PROBE WITH U STREAM HT
                            if (hashTableU.containsKey(key)) {
                                // PRINT RESULT
                            }
                        }
                    }
                    break;
                case "T":
                    integerTimeStamp += 1;
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableT.containsKey(key)) {
                        hashTableT.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableT.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        arrayList.add(new Triplet<>(integerTimeStamp, key, value));
                        hashTableT.put(key, arrayList);
                    }
                    //PROBE WITH RS STREAM HT
                    if (hashTableRS.containsKey(key)) {
                        //INSERT INTO hashTableRST

                        //PROBE WITH U STREAM HT
                        if (hashTableU.containsKey(key)) {
                            // PRINT RESULT
                        }
                    }
                    break;
                case "U":
                    integerTimeStamp += 1;
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableU.containsKey(key)) {
                        hashTableU.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableU.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        arrayList.add(new Triplet<>(integerTimeStamp, key, value));
                        hashTableU.put(key, arrayList);
                    }
                    //PROBE WITH RS STREAM HT
                    if (hashTableRST.containsKey(key)) {
                        // PRINT RESULT

                    }
                    break;
            }
        }
    }

    public void mJoin(String key, String value, String joinType, String whichStream) {
        if (joinType.equals("CA")) {
            ArrayList<Triplet<Integer, String, String>> arrayList = new ArrayList<>();
            switch (whichStream) {
                // Common Attribute Join [Key is the common attribute]


                case "R":
                    integerTimeStamp += 1;
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableR.containsKey(key)) {
                        hashTableR.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableR.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        arrayList.add(new Triplet<>(integerTimeStamp, key, value));
                        hashTableR.put(key, arrayList);
                    }
                    break;
                case "S":
                    integerTimeStamp += 1;
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableS.containsKey(key)) {
                        hashTableS.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableS.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        arrayList.add(new Triplet<>(integerTimeStamp, key, value));
                        hashTableS.put(key, arrayList);
                    }
                    break;
                case "T":
                    integerTimeStamp += 1;
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableT.containsKey(key)) {
                        hashTableT.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableT.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        arrayList.add(new Triplet<>(integerTimeStamp, key, value));
                        hashTableT.put(key, arrayList);
                    }
                    break;
                case "U":
                    integerTimeStamp += 1;
                    // INSERT INTO STREAM HASH TABLE
                    if (hashTableU.containsKey(key)) {
                        hashTableU.put(key, ((ArrayList<Triplet<Integer, String, String>>) hashTableU.get(key)).add(new Triplet<>(integerTimeStamp, key, value)));
                    } else {
                        arrayList.add(new Triplet<>(integerTimeStamp, key, value));
                        hashTableU.put(key, arrayList);
                    }
                    break;
            }
        }
    }

    public void earlyHashJoin(String key, String value, String joinType, String whichStream) {

        // One to Many Join
        if (joinType.equals("1M")) {
            switch (whichStream) {
                case "R":
                    integerTimeStamp++;
                    if (hashTableS.containsKey(key)) {
                        System.out.println(String.format("R: %s, %s, %s", key, value, hashTableS.get(key).toString()));
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
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key)) {
                        System.out.println(String.format("S: %s, %s, %s", key, hashTableR.get(key).toString(), value));
                    } else {
                        ArrayList<String> al = new ArrayList<>();
                        al.add(value);
                        hashTableS.put(key, al);
                    }
                    break;
            }
        } else if (joinType.equals("MM")) {
            // Many to Many
            switch (whichStream) {
                case "R":
                    integerTimeStamp++;
                    if (hashTableS.containsKey(key)) {
                        System.out.println(String.format("R: %s, %s, %s", key, "[" + integerTimeStamp + ", " + value + "]", hashTableS.get(key).toString()));
                    }
                    if (hashTableR.containsKey(key)) {
                        ArrayList<Pair<Integer, String>> arrayList = (ArrayList<Pair<Integer, String>>) hashTableR.get(key);
                        Pair<Integer, String> pair = new Pair<>(integerTimeStamp, value);
                        arrayList.add(pair);
                        hashTableR.put(key, arrayList);

                    } else {
                        ArrayList<Pair<Integer, String>> arrayList = new ArrayList<>();
                        Pair<Integer, String> pair = new Pair<>(integerTimeStamp, value);
                        arrayList.add(pair);
                        hashTableR.put(key, arrayList);
                    }

                    break;

                case "S":
                    integerTimeStamp++;
                    if (hashTableR.containsKey(key)) {
                        System.out.println(String.format("S: %s, %s, %s", key, hashTableR.get(key).toString(), "[" + integerTimeStamp + ", " + value + "]"));
                    }
                    if (hashTableS.containsKey(key)) {
                        ArrayList<Pair<Integer, String>> arrayList = (ArrayList<Pair<Integer, String>>) hashTableS.get(key);
                        Pair<Integer, String> pair = new Pair<>(integerTimeStamp, value);
                        arrayList.add(pair);
                        hashTableS.put(key, arrayList);
                    } else {
                        ArrayList<Pair<Integer, String>> arrayList = new ArrayList<>();
                        Pair<Integer, String> pair = new Pair<>(integerTimeStamp, value);
                        arrayList.add(pair);
                        hashTableS.put(key, arrayList);
                    }
                    break;
            }
        }

        // Biased Flushing Policy
        // Integer hashTableSize = 0;
        //Long freeMemory = Utils.bytesToMegabytes(Runtime.getRuntime().freeMemory());
        //System.out.println("Free Memory: " + freeMemory);
        //if (freeMemory <= 150) {
        if (hashTableS.size() > MAX_HASH_TABLE_SIZE) { // Using it for simulation
            HashMap<String, ArrayList<Pair<Integer, String>>> hashTableRClone = (HashMap<String, ArrayList<Pair<Integer, String>>>) hashTableR.clone();
            hashTableCollectionR.put(integerTimeStamp, hashTableRClone);
            try {
                FileOutputStream fileOutputStream = new FileOutputStream("FlushOut\\S\\" + integerTimeStamp + ".ser");
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
                objectOutputStream.writeObject(hashTableS.clone());
                objectOutputStream.close();
                hashTableR.clear();
                hashTableS.clear();
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        } else if (hashTableR.size() > MAX_HASH_TABLE_SIZE) {
            HashMap<String, ArrayList<Pair<Integer, String>>> hashTableSClone = (HashMap<String, ArrayList<Pair<Integer, String>>>) hashTableS.clone();
            hashTableCollectionS.put(integerTimeStamp, hashTableSClone);
            try {
                FileOutputStream fileOutputStream = new FileOutputStream("FlushOut\\R\\" + integerTimeStamp + ".ser");
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
                objectOutputStream.writeObject(hashTableR.clone());
                objectOutputStream.close();
                hashTableR.clear();
                hashTableS.clear();
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        }
    }

    // CleanUp Phase
//        if (cleanUp[0].equals("Y")) {
//            try {
//                System.out.println(whichStream + " CleanUP");
//                Thread cleanUpThread = new Thread(() -> earlyHashJoinCleanUp(hashTableCollectionR));
//                cleanUpThread.start();
//            } catch (Exception ex) {
//                System.out.println(ex.toString());
//            }
//        }
    //}

    public void earlyHashJoinCleanUp() {
        HashMap<Integer, HashMap<String, ArrayList<Pair<Integer, String>>>> hashTableCollectionRTemp = (HashMap<Integer, HashMap<String, ArrayList<Pair<Integer, String>>>>) hashTableCollectionR;
        HashMap<Integer, HashMap<String, ArrayList<Pair<Integer, String>>>> hashTableCollectionSTemp = (HashMap<Integer, HashMap<String, ArrayList<Pair<Integer, String>>>>) hashTableCollectionS;
        try {
            File[] rStreamFiles = new File("FlushOut\\R\\").listFiles();
            File[] sStreamFiles = new File("FlushOut\\S\\").listFiles();
            for (File rStreamFile : rStreamFiles
                    ) {
                FileInputStream fileInputStream = new FileInputStream(rStreamFile);
                ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                HashMap<String, ArrayList<Pair<Integer, String>>> hashTableR = (HashMap<String, ArrayList<Pair<Integer, String>>>) objectInputStream.readObject();
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
                HashMap<String, ArrayList<Pair<Integer, String>>> hashTableS = (HashMap<String, ArrayList<Pair<Integer, String>>>) objectInputStream.readObject();
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
                        HashMap<String, ArrayList<Pair<Integer, String>>> hashTableS = hashTableCollectionSTemp.get(sKey);
                        HashMap<String, ArrayList<Pair<Integer, String>>> hashTableR = hashTableCollectionRTemp.get(rKey);
                        for (String key : hashTableS.keySet()
                                ) {
                            if (hashTableR.containsKey(key)) {
                                System.out.println("EHJ CleanUP: " + key + ", " + hashTableR.get(key) + ", " + hashTableS.get(key));
                            }
                        }
                    } else {
                        HashMap<String, ArrayList<Pair<Integer, String>>> hashTableS = hashTableCollectionSTemp.get(sKey);
                        HashMap<String, ArrayList<Pair<Integer, String>>> hashTableR = hashTableCollectionRTemp.get(rKey);
                    }
                }
            }

        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }


    public void sliceJoin(String key, String value, String joinType, String whichStream) {
        if (joinType.equals("CA")) {
            switch (whichStream) {
                // Common Attribute Join [Key is the common attribute]
                case "R":
                    integerTimeStamp += 1;
                    if (hashTableR.containsKey(key)) {
                        ArrayList<Triplet<Integer, String, String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableR.get(key);
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
                        ArrayList<Triplet<Integer, String, String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableS.get(key);
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
                        ArrayList<Triplet<Integer, String, String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableT.get(key);
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
                        ArrayList<Triplet<Integer, String, String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableU.get(key);
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
                        ArrayList<Triplet<Integer, String, String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableR.get(key);
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
                        ArrayList<Triplet<Integer, String, String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableS.get(key);
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
                        ArrayList<Pair<String, String>> arrayList = (ArrayList<Pair<String, String>>) indirectPartitionMapper.get(value);
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
                        ArrayList<Triplet<Integer, String, String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableT.get(key);
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
                        ArrayList<Triplet<Integer, String, String>> arrayList = (ArrayList<Triplet<Integer, String, String>>) hashTableU.get(key);
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

