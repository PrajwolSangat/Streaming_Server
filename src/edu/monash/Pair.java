package edu.monash;

/**
 * Created by psangats on 7/07/2017.
 */


public class Pair<X, Y> {
    private final X x;
    private final Y y;

    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public X _1(){
        return x;
    }
    public  Y _2(){
        return y;
    }
}

