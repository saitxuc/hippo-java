package com.pinganfu.hippo.network.disruptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author saitxuc
 *
 */
public class DisruptorUtils {
	
	protected static final Logger LOG = LoggerFactory.getLogger(DisruptorUtils.class);
	
	public static boolean localMode = false;
	
    public static void halt_process(int val, String msg) {
        LOG.info("Halting process: " + msg);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        if (localMode && val == 0) {
            // throw new RuntimeException(msg);
        } else {
            haltProcess(val);
        }
    }
	
    public static void haltProcess(int val) {
        Runtime.getRuntime().halt(val);
    }
    
    public static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {

        }
    }
    
}
