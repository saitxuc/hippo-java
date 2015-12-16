package com.pinganfu.hippo.common.lifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public abstract class LifeCycleSupport implements LifeCycle{
	
	private static final Logger LOG = LoggerFactory.getLogger(LifeCycleSupport.class);
	
	protected AtomicBoolean inited = new AtomicBoolean(false);
	
	protected AtomicBoolean started = new AtomicBoolean(false);
	
	protected AtomicBoolean finsihed = new AtomicBoolean(false);
	
	protected AtomicBoolean stoped = new AtomicBoolean(false);
	
	private Throwable startException;
	
	public void init() {
		if(inited.compareAndSet(false, true)) {
			if(LOG.isInfoEnabled()) {
				LOG.info("class : " + this.getClass() + " Life Cycle will init.");
			}
			doInit();
			if(LOG.isInfoEnabled()) {
				LOG.info("class : " + this.getClass() + " Life Cycle has finsih init.");
			}
		}
	}
	
	public void start() {
        stoped.set(false);
		if(!inited.get()) {
			init();
		}
		if(started.compareAndSet(false, true)) {
			if(LOG.isInfoEnabled()) {
				LOG.info("class : " + this.getClass() + " Life Cycle will start.");
			}
			try{
				doStart();
			}catch(Throwable e) {
				startException = e;
				throw new RuntimeException(startException.getMessage(), e);
			}
			if(LOG.isInfoEnabled()) {
				LOG.info("class : " + this.getClass() + " Life Cycle has finsih start.");
			}
			finsihed.compareAndSet(false, true);
		}
	}
	
	public void stop() {
		if(stoped.compareAndSet(false, true)) {
			if(LOG.isInfoEnabled()) {
				LOG.info("class : " + this.getClass() + " Life Cycle will stop. ");
			}
			doStop();
			if(LOG.isInfoEnabled()) {
				LOG.info("class : " + this.getClass() + " Life Cycle has finsih stop.");
			}
		}
		started.set(false);
		finsihed.set(false);
	}
	
	public boolean isStarted() {
		return this.finsihed.get();
	}
	
	public boolean isStopped() {
		return this.stoped.get();
	}
	
	public Throwable getStartException() {
		return startException;
	}

	public abstract void doInit();
	
	public abstract void doStart();
	
	public abstract void doStop();
	
}


