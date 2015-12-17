package com.hippo.common.listener;

import com.hippo.common.exception.HippoException;

/**
 * 
 * @author saitxuc
 *
 */
public interface EventListener<T> {
	
	/**
	 * 
	 * @param <T>
	 * @param event
	 */
	 void onEvent(T event) throws HippoException;
	
}
