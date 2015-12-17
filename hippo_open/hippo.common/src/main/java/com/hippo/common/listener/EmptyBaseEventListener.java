package com.hippo.common.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.exception.HippoException;

/**
 * 
 * @author saitxuc
 *
 */
public class EmptyBaseEventListener implements EventListener<TransportEventEnum>{
	
	protected static final Logger LOG = LoggerFactory.getLogger(EmptyBaseEventListener.class);
	
	@Override
	public void onEvent(TransportEventEnum event) throws HippoException {
		LOG.info(" Empty event Listener do nothing.  ");
	}

}
