package com.pinganfu.hippo.client.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.client.transport.AbstractClientConnectionControl;


public class ReprocessSchedule implements Runnable {
	
	private static final Logger log = LoggerFactory.getLogger(ReprocessSchedule.class);
	
	private AbstractClientConnectionControl connectionControl;
	
	public ReprocessSchedule(AbstractClientConnectionControl connectionControl) {
		this.connectionControl = connectionControl;
	}
	
	public void run() {
		try{
			if(connectionControl != null) {
				connectionControl.reconnectionSchedule();
			}
		}catch(Exception e) {
			log.error(" reprocess hand happen error. ", e);
		}
		
	}
    
}
