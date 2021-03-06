package edu.monash;

import java.io.Serializable;

/**
 * Created by psangats on 7/07/2017.
 */
public class TriTuple<X, Y, Z> implements Serializable{

    private final X first;
    private final Y second;
    private final Z third;

    public TriTuple(X first, Y second, Z third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public X getFirst() {
        return first;
    }

    public Y getSecond() {
        return second;
    }

    public Z getThird() {
        return third;
    }

    @Override
    public String toString() {
        return  getFirst() + ", " + getSecond()+ ", " + getThird();
    }
}
