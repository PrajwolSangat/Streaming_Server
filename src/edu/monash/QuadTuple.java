package edu.monash;

import java.io.Serializable;

/**
 * Created by psangats on 7/07/2017.
 */
public class QuadTuple<K, V, ATS, DTS> implements Serializable{

    private final K key;
    private final V value;
    private final ATS ats;
    private final DTS dts;

    public QuadTuple(K key, V value, ATS ats, DTS dts) {
        this.key = key;
        this.value = value;
        this.ats = ats;
        this.dts = dts;
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

    public DTS getDts() {
        return dts;
    }


    @Override
    public String toString() {
        return  getKey() + ", " + getValue() + ", " + getAts() + ", " + getDts();
    }
}
