package com.hippo.network;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;
import com.hippo.network.command.Response;

/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public class FutureResponse {
	
	private static final Logger LOG = LoggerFactory.getLogger(FutureResponse.class);

    private final ResponseCallback responseCallback;
    
    private Command command;
    
    private Object lock = new Object();
    
    private long timeout = 10000;
    
    private volatile Response response;
    
    private final long start = System.currentTimeMillis();
    
    public FutureResponse(Command command,  ResponseCallback responseCallback) {
        this.command = command;
    	this.responseCallback = responseCallback;
    }
    
    public boolean isDone() {
		return response != null;
	}
    
    public Response getResult() throws IOException {
    	if (!isDone()) {
			synchronized (lock) {
				try {
					while (!isDone()) {
						lock.wait(timeout);
						if (isDone()) {
							return response;
						} else {
							Response temp = new Response();
							temp.setFailure(true);
							temp.setAction(command.getAction());
							temp.setContent("timeout");
							return temp;
						}
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			return response;
		} else {
			return response;
		}
    }

    public Response getResult(long timeout) throws IOException {
    	this.timeout = timeout;
    	if (!isDone()) {
			synchronized (lock) {
				try {
					while (!isDone()) {
						lock.wait(timeout);
						if (isDone()) {
							return response;
						} else {
							Response temp = new Response();
							temp.setFailure(true);
							temp.setAction(command.getAction());
							temp.setContent("timeout");
							return temp;
						}
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
			return response;
		} else {
			return response;
		}
    }
    
	public void doReceived(Response response) {
		synchronized (lock) {
			this.response = response;
			lock.notifyAll();
		}
		if (responseCallback != null) {
            responseCallback.onCompletion(this);
        }
	}

	
	public long getStartTimestamp() {
		return start;
	}
    
	public long getTimeout() {
		return timeout;
	}
	
	public String getCommandId() {
		return command.getHeadValue(CommandConstants.COMMAND_ID);
	}	
	
	public String getAction() {
		return command.getAction();
	}
	
}
