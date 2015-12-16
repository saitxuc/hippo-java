package com.pinganfu.hippo.client.transport;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.client.ClientConstants;
import com.pinganfu.hippo.client.ClientSessionResult;
import com.pinganfu.hippo.client.HippoConnector;
import com.pinganfu.hippo.client.exception.HippoClientException;
import com.pinganfu.hippo.common.errorcode.HippoCodeDefine;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.lifecycle.LifeCycleSupport;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.Session;

/**
 * 
 * @author saitxuc 
 * 2015-4-7
 */
public abstract class AbstractClientConnectionControl extends LifeCycleSupport implements ClientConnectionControl {
	
	protected static final Logger log = LoggerFactory.getLogger(AbstractClientConnectionControl.class);
	
	protected int sessionInstance = ClientConstants.CLIENT_INIT_SESSIONPOOL_SIZE;
	
	protected String brokerUrl = null;
	
	protected Connection connection = null;
	/**
	 * session number control
	 */
	protected Semaphore sessionSemaphore;
	
	/**
	 * key:桶+brokerUrl, value:BlockingQueue<Session>
	 */
	protected ConcurrentHashMap<String, BlockingQueue<Session>> sessionPoolMap = new ConcurrentHashMap<String, BlockingQueue<Session>>();
	/**
	 * key:buckNum, value:brokerUrl
	 */
	protected ConcurrentSkipListSet<String> badConnectionList = new ConcurrentSkipListSet<String>();
	/**
	 * reconnect schedule executor
	 */
	protected ExecutorService timingReconnectExecutor;
	
	abstract public void reconnectionSchedule();
	
	abstract public void exceptionDispose(String brokerUrl);
	
	abstract public String getName();
	
    abstract public void setConnector(HippoConnector connector);
	
	public void createInitSessions(Connection connection) {
		BlockingQueue<Session> sessionPool = new LinkedBlockingQueue<Session>();
		for (int i = 0; i < sessionInstance; i++) {
			try {
				Session session = connection.createSession();
				sessionPool.offer(session);
			} catch (HippoException e) {
				log.error("create init Session happened error. ", e.getMessage());
			}
		}
		sessionPoolMap.put(brokerUrl, sessionPool);
	}
	
	
	@Override
	public ClientSessionResult getSession(byte[] key) throws HippoClientException {
		BlockingQueue<Session> sessionPool = sessionPoolMap.get(brokerUrl);
        //check bad connection
        if(sessionPool == null && badConnectionList.contains(brokerUrl)) {
        	throw new HippoClientException("brokerUrl["+ brokerUrl +"] is a bad connection.please wait reconnection success!", 
        			HippoCodeDefine.HIPPO_CONNECTION_FAILURE);
        }
		
		if (!hasCapacity()) {
			throw new HippoClientException("session pool has full!", HippoCodeDefine.HIPPO_SERVER_ERROR);
		}
		
		Session session = null;
		//get session from pool
		try {
			if(sessionPool != null) {
				session = sessionPool.poll(500, TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException e) {
			log.error("get session from pool error. ");
		}
		//create session
		if(session == null) {
			try {
				session = connection.createSession();
			} catch (HippoException e) {
				throw new HippoClientException("create session happened error! brokerUrl is ["+ brokerUrl +"]", 
						HippoCodeDefine.HIPPO_CLIENT_SESSION_ERROR);
			}
		}
		
		return new ClientSessionResult(session);
	}

	@Override
	public void offerSession(byte[] key, Session session) {
		try {
			if (session != null) {
				BlockingQueue<Session> sessionPool = sessionPoolMap.get(brokerUrl);
				if(sessionPool == null) {
					return;
				}
				try {
					if (!sessionPool.offer(session, ClientConstants.SESSION_POOL_TIMEOUT, TimeUnit.MILLISECONDS)) {
						try {
							session.close();
						} catch (HippoException e) {
							log.error(e.getMessage());
						}
					}
				} catch (InterruptedException e) {
					log.error(e.getMessage());
				}
			}
		} finally {
			returnCapacity();
		}
	}
	
	public boolean hasCapacity() {
        if (sessionSemaphore != null) {
            try {
                if (!sessionSemaphore.tryAcquire(5, TimeUnit.MILLISECONDS)) {
                    return false;
                }
            } catch (InterruptedException e) {
                return false;
            }
        }
        return true;
    }
	
	public void returnCapacity() {
        if (sessionSemaphore != null) {
            sessionSemaphore.release();
        }
    }
	
	public ConcurrentHashMap<String, BlockingQueue<Session>> getSessionPoolMap() {
		return sessionPoolMap;
	}

	public void setSessionPoolMap(
			ConcurrentHashMap<String, BlockingQueue<Session>> sessionPoolMap) {
		this.sessionPoolMap = sessionPoolMap;
	}

	public int getSessionInstance() {
		return sessionInstance;
	}

	public void setSessionInstance(int sessionInstance) {
		this.sessionInstance = sessionInstance;
		if(this.sessionInstance > ClientConstants.CLIENT_MAX_SESSIONPOOL_SIZE) {
			this.sessionInstance = ClientConstants.CLIENT_MAX_SESSIONPOOL_SIZE;
		}
	}

	public void setBrokerUrl(String brokerUrl) {
		this.brokerUrl = brokerUrl;
	}
	
}
