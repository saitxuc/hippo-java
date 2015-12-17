package com.hippo.manager;

/**
 * 
 * @author saitxuc
 * write 2014-8-8
 */
import java.util.concurrent.atomic.AtomicLong;

import javax.management.j2ee.statistics.CountStatistic;

/**
 * A count statistic implementation
 * 
 * 
 */
public class CountStatisticImpl extends StatisticImpl implements CountStatistic {

    private final AtomicLong counter = new AtomicLong(0);
    private CountStatisticImpl parent;

    public CountStatisticImpl(CountStatisticImpl parent, String name, String description) {
        this(name, description);
        this.parent = parent;
    }

    public CountStatisticImpl(String name, String description) {
        this(name, "count", description);
    }

    public CountStatisticImpl(String name, String unit, String description) {
        super(name, unit, description);
    }

    public void reset() {
        if (isDoReset()) {
            super.reset();
            counter.set(0);
        }
    }

    public long getCount() {
        return counter.get();
    }

    public void setCount(long count) {
        if (isEnabled()) {
            counter.set(count);
        }
    }

    public void add(long amount) {
        if (isEnabled()) {
            counter.addAndGet(amount);
            updateSampleTime();
            if (parent != null) {
                parent.add(amount);
            }
        }
    }

    public void increment() {
        if (isEnabled()) {
            counter.incrementAndGet();
            updateSampleTime();
            if (parent != null) {
                parent.increment();
            }
        }
    }

    public void subtract(long amount) {
        if (isEnabled()) {
            counter.addAndGet(-amount);
            updateSampleTime();
            if (parent != null) {
                parent.subtract(amount);
            }
        }
    }

    public void decrement() {
        if (isEnabled()) {
            counter.decrementAndGet();
            updateSampleTime();
            if (parent != null) {
                parent.decrement();
            }
        }
    }

    public CountStatisticImpl getParent() {
        return parent;
    }

    public void setParent(CountStatisticImpl parent) {
        this.parent = parent;
    }

    protected void appendFieldDescription(StringBuffer buffer) {
        buffer.append(" count: ");
        buffer.append(Long.toString(counter.get()));
        super.appendFieldDescription(buffer);
    }

    /**
     * @return the average time period that elapses between counter increments
     *         since the last reset.
     */
    public double getPeriod() {
        double count = counter.get();
        if (count == 0) {
            return 0;
        }
        double time = System.currentTimeMillis() - getStartTime();
        return time / (count * 1000.0);
    }

    /**
     * @return the number of times per second that the counter is incrementing
     *         since the last reset.
     */
    public double getFrequency() {
        double count = counter.get();
        double time = System.currentTimeMillis() - getStartTime();
        return count * 1000.0 / time;
    }

}
