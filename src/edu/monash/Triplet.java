package edu.monash;

/**
 * Created by psangats on 7/07/2017.
 */
public class Triplet<T, U, V> {

    private final T first;
    private final U second;
    private final V third;

    public Triplet(T first, U second, V third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public T getFirst() {
        return first;
    }

    public U getSecond() {
        return second;
    }

    public V getThird() {
        return third;
    }

    public String toString() {
        return "<" + getFirst().toString() + "," + getSecond().toString() + "," + getThird().toString() + ">";
    }
}
