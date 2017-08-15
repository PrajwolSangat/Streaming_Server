package edu.monash;

import java.io.Serializable;

/**
 * Created by psangats on 7/07/2017.
 */


public class Tuple<X, Y> implements Serializable {
    private final X first;
    private final Y second;

    public Tuple(X first, Y second) {
        this.first = first;
        this.second = second;
    }

    public X getFirst() {
        return first;
    }

    public Y getSecond() {
        return second;
    }

    @Override
    public String toString() {
        return getFirst() + ", " + getSecond();
    }
}

