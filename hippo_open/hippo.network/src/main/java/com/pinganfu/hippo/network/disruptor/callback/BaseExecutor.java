package com.pinganfu.hippo.network.disruptor.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author saitxuc
 *
 */
public class BaseExecutor extends RunnableCallback  {
	
	protected static final Logger LOG = LoggerFactory.getLogger(BaseExecutor.class);
	
	protected Throwable error = null;
	
	protected String idStr;
	
	@Override
    public void run() {
        // this function will be override by SpoutExecutor or BoltExecutor
        throw new RuntimeException("Should implement this function");
    }

    @Override
    public Exception error() {
        if (error == null) {
            return null;
        }

        return new Exception(error);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutdown executing thread of " + idStr);
    }
}
