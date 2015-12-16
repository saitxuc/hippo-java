package com.pinganfu.hippo.common.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.exception.HippoException;

/**
 * 
 * @author saitxuc
 * 2015-4-21
 */
public class NettyBaseEventListener implements EventListener<NettyEventEnum> {
	
	protected static final Logger LOG = LoggerFactory.getLogger(NettyBaseEventListener.class);
	
	protected void onRegister() {
		LOG.info(" do register event from netty network ");
	}
	
	protected void onUnregister(){
		LOG.info(" do unregister event from netty network ");
	}
	
	protected void onActive(){
		LOG.info(" do active event from netty network ");
	}
	
	protected void onInactive() {
		LOG.info(" do inactive event from netty network ");
	}
	
	protected void onReconnect() throws HippoException{
		LOG.info(" do reconnect event from netty network ");
	}
	
	public void onEvent(NettyEventEnum event) throws HippoException{
		switch (event) {
		case EVENT_REGISTER:
			onRegister();
			break;
		case EVENT_UNREGISTER:
			onUnregister();
			break;
		case EVENT_ACTIVE:
			onActive();
			break;
		case EVENT_INACTIVED:
			onInactive();
			break;
		case EVENT_RECONNECT:
			onReconnect();
			break;
		default:
			return;
		}
	}


	
	
	
	
}	
