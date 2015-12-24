package com.hippo.common.lifecycle;

import com.hippo.common.exception.HippoException;

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
