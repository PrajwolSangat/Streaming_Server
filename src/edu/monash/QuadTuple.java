package edu.monash;

import java.io.Serializable;

/**
 * Created by psangats on 7/07/2017.
 */
public class QuadTuple<W, X, Y, Z> implements Serializable {

    private final W first;
    private final X second;
    private final Y third;
    private final Z fourth;

    public QuadTuple(W first, X second, Y third, Z fourth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
    }

    public W getFirst() {
        return first;
    }

    public X getSecond() {
        return second;
    }

    public Y getThird() {
        return third;
    }

    public Z getFourth() {
        return fourth;
    }

    @Override
    public String toString() {
        return getFirst() + ", " + getSecond() + ", " + getThird() + ", " + getFourth();
    }
}
