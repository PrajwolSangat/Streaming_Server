package edu.monash;

import java.io.Serializable;

/**
 * Created by psangats on 7/07/2017.
 */


public class Pair<X, Y> implements Serializable {
    private final X x;
    private final Y y;

    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public X _1() {
        return x;
    }

    public Y _2() {
        return y;
    }

    @Override
    public String toString() {
        return _1() + ", " + _2();
    }
}

