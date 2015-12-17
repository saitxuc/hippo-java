package com.hippo.manager;

/**
 * 
 * @author saitxuc
 * 2015-3-17
 */
public class ConnectionStatistics extends StatsImpl {
	
	private CountStatisticImpl enqueues;
    private CountStatisticImpl dequeues;

    public ConnectionStatistics() {

        enqueues = new CountStatisticImpl("enqueues", "The number of messages that have been sent to the connection");
        dequeues = new CountStatisticImpl("dequeues", "The number of messages that have been dispatched from the connection");

        addStatistic("enqueues", enqueues);
        addStatistic("dequeues", dequeues);
    }

    public CountStatisticImpl getEnqueues() {
        return enqueues;
    }

    public CountStatisticImpl getDequeues() {
        return dequeues;
    }

    public void reset() {
        super.reset();
        enqueues.reset();
        dequeues.reset();
    }

    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dequeues.setEnabled(enabled);
    }

    public void setParent(ConnectorStatistics parent) {
        if (parent != null) {
            enqueues.setParent(parent.getEnqueues());
            dequeues.setParent(parent.getDequeues());
        } else {
            enqueues.setParent(null);
            dequeues.setParent(null);
        }
    }
	
}
