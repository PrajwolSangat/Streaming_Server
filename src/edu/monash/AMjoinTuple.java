package edu.monash;

import java.io.Serializable;

/**
 * Created by psangats on 7/07/2017.
 */
public class AMjoinTuple<K, V, ATS> implements Serializable{

    private final K key;
    private final V value;
    private final ATS ats;

    public AMjoinTuple(K key, V value, ATS ats) {
        this.key = key;
        this.value = value;
        this.ats = ats;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public ATS getAts() {
        return ats;
    }


    @Override
    public String toString() {
        return  getKey() + ", " + getValue() + ", " + getAts();
    }
}
