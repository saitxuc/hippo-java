package com.pinganfu.hippo.common.lifecycle;

import com.pinganfu.hippo.common.exception.HippoException;

/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public interface LifeCycle {
	
	public void init();
	
	public void start();
	
	public void stop();
	
	public boolean isStarted();
	
	public Throwable getStartException();
	
}
