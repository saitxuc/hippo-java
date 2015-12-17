package com.hippo.network.disruptor.callback;

import com.hippo.network.disruptor.Shutdownable;

/**
 * 
 * @author saitxuc
 *
 */
public class RunnableCallback implements Runnable, Shutdownable{
	
	public void preRun() {

    }

    @Override
    public void run() {

    }

    public void postRun() {

    }

    public Exception error() {
        return null;
    }

    public Object getResult() {
        return null;
    }

    public void shutdown() {

    }

    public String getThreadName() {
        return null;
    }
	
}
