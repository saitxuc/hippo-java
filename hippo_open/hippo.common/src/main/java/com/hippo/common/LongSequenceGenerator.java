package com.hippo.common;

/**
 * 
 * @author saitxuc
 * write 2014-7-14
 */
public class LongSequenceGenerator {

    private long lastSequenceId;

    public synchronized long getNextSequenceId() {
        return ++lastSequenceId;
    }

    public synchronized long getLastSequenceId() {
        return lastSequenceId;
    }

    public synchronized void setLastSequenceId(long l) {
        lastSequenceId = l;
    }

}
