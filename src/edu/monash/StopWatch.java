package edu.monash;

import java.util.concurrent.TimeUnit;

/**
 * Created by psangats on 7/08/2017.
 */
public class StopWatch {
    private long starts;
    private long ends;

    public StopWatch start() {
        starts = System.nanoTime();
        return this;
    }

    public void reset() {
        start();
    }

    public StopWatch stop(){
        ends = System.nanoTime();
        return this;
    }

    /*
     * Returns nano seconds time
     */
    public long elaspsedTime() {
        return ends - starts;
    }

    /*
     * Returns time in TimeUnit
     */
    public long elaspsedTime(TimeUnit unit) {
        return unit.convert(elaspsedTime(), TimeUnit.NANOSECONDS);
    }

    public String toMinuteSeconds() {
        return String.format("%d min, %d sec", elaspsedTime(TimeUnit.MINUTES),
                elaspsedTime(TimeUnit.SECONDS) - elaspsedTime(TimeUnit.MINUTES));
    }
}
