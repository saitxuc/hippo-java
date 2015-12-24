package com.hippo.common.util;

/**
 * 
 * @author saitxuc
 * 2015-3-26
 */
public final class StopWatch {
	
	private long start;
    private long stop;

    /**
     * Starts the stop watch
     */
    public StopWatch() {
        this(true);
    }

    /**
     * Creates the stop watch
     *
     * @param started whether it should start immediately
     */
    public StopWatch(boolean started) {
        if (started) {
            restart();
        }
    }

    /**
     * Starts or restarts the stop watch
     */
    public void restart() {
        start = System.currentTimeMillis();
        stop = 0;
    }

    /**
     * Stops the stop watch
     *
     * @return the time taken in millis.
     */
    public long stop() {
        stop = System.currentTimeMillis();
        return taken();
    }

    /**
     * Returns the time taken in millis.
     *
     * @return time in millis
     */
    public long taken() {
        if (start > 0 && stop > 0) {
            return stop - start;
        } else if (start > 0) {
            return System.currentTimeMillis() - start;
        } else {
            return 0;
        }
    }
	
}
