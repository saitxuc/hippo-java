package com.hippo.broker.useage;

/**
 * 
 * @author saitxuc
 * write 2014-8-13
 */
public interface UsageListener {
    void onUsageChanged(Usage usage, int oldPercentUsage, int newPercentUsage);
}
