package com.hippo.common.listener;

import com.hippo.common.exception.HippoException;


/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */ 
public interface ExceptionListener {
	
	 void onException(HippoException exception);
	
}
