package com.hippo.manager;

/**
 * 
 * @author saitxuc
 * write 2014-8-8
 */
public class ConnectorStatistics extends StatsImpl {

    
	protected CountStatisticImpl enqueues;
    protected CountStatisticImpl dequeues;
	
    protected CountStatisticImpl keys;
    
    public ConnectorStatistics() {

    	enqueues = new CountStatisticImpl("enqueues", "The number of data that that are being held by cache");
    	dequeues = new CountStatisticImpl("dequeues", "The number of data that have been dispatched from cache ");
    	
    	keys = new CountStatisticImpl("keys", "The number of keys that that are being held by cache");
    	
    	addStatistic("enqueues", enqueues);
        addStatistic("dequeues", dequeues);
        
        addStatistic("keys", keys);
    }

    public CountStatisticImpl getKeys() {
		return keys;
	}

	public void setKeys(CountStatisticImpl keys) {
		this.keys = keys;
	}

	public void reset() {
        super.reset();
        keys.reset();
        enqueues.reset();
        dequeues.reset();
    }
	
    public CountStatisticImpl getEnqueues() {
		return enqueues;
	}

	public void setEnqueues(CountStatisticImpl enqueues) {
		this.enqueues = enqueues;
	}

	public CountStatisticImpl getDequeues() {
		return dequeues;
	}

	public void setDequeues(CountStatisticImpl dequeues) {
		this.dequeues = dequeues;
	}

	public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        keys.setEnabled(enabled);
        enqueues.setEnabled(enabled);
        dequeues.setEnabled(enabled);
    }

    public void setParent(ConnectorStatistics parent) {
        if (parent != null) {
            keys.setParent(parent.keys);
            enqueues.setParent(parent.enqueues);
            dequeues.setParent(parent.dequeues);
        } else {
            keys.setParent(null);
        }
    }

}
