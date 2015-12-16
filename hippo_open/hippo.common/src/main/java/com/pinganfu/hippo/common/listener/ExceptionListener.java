package com.pinganfu.hippo.common.listener;

import com.pinganfu.hippo.common.exception.HippoException;


/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */ 
public interface ExceptionListener {
	
	 void onException(HippoException exception);
	
}
