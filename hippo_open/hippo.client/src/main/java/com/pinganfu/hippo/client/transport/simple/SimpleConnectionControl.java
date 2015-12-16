package com.pinganfu.hippo.client.transport.simple;

import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.pinganfu.hippo.client.ClientConstants;
import com.pinganfu.hippo.client.HippoConnector;
import com.pinganfu.hippo.client.exception.HippoClientException;
import com.pinganfu.hippo.client.listener.ConnectionExceptionListener;
import com.pinganfu.hippo.client.schedule.ReprocessSchedule;
import com.pinganfu.hippo.client.transport.AbstractClientConnectionControl;
import com.pinganfu.hippo.common.errorcode.HippoCodeDefine;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.common.util.ExcutorUtils;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.ConnectionFactory;
import com.pinganfu.hippo.network.Session;
import com.pinganfu.hippo.network.impl.TransportConnectionFactory;

/**
 * 
 * @author saitxuc
 * 2015-4-7
 */
public class SimpleConnectionControl extends AbstractClientConnectionControl {
	
	protected static final Logger log = LoggerFactory.getLogger(SimpleConnectionControl.class);
	
    protected String userName;
    
    protected String password;
    
    public SimpleConnectionControl() {
		super();
	}
    
    public String getName() {
		return ClientConstants.TRANSPORT_PROTOCOL_HIPPO;
	}
	
	@Override
	public Connection createConnection() throws HippoException {
		if(StringUtils.isEmpty(this.brokerUrl)) {
			throw new RuntimeException("brokerurl value is null, please check! ");
		}
		return createConnection(null, null, this.brokerUrl);
	}

	@Override
	public Connection createConnection(String userName, String password,
			String brokerUrl) throws HippoException{
		ConnectionFactory connectionFactory = new TransportConnectionFactory(brokerUrl);
		Connection connection = null;
		try{
			connection = connectionFactory.createConnection(userName, password);
		}catch(Exception e) {
			throw new HippoClientException("create connection happened error.", HippoCodeDefine.HIPPO_CONNECTION_FAILURE);
		}
		return connection;
	}

	
	@Override
	public void doInit() {
		timingReconnectExecutor = ExcutorUtils.startSchedule(" reconnect schedule", 
        		new ReprocessSchedule(this), 5*1000L, 20*1000L);
		try{
			connection = createConnection();
		}catch(Exception e) {
			log.error("client do init happen error. ", e);
			if(!badConnectionList.contains(brokerUrl)) {
				badConnectionList.add(brokerUrl);
			}
		}
	}

	@Override
	public void doStart() {
		try{
			if(connection != null) {
				connection.start();
				connection.addExceptionListener(new ConnectionExceptionListener(this, brokerUrl));
				createInitSessions(connection);
				sessionSemaphore = new Semaphore(sessionInstance);
			}
		}catch(Exception e) {
			log.error("client do start happen error. ", e);
		}
	}

	@Override
	public void doStop() {
		try{
			for(Entry<String, BlockingQueue<Session>> entry : sessionPoolMap.entrySet()) {
				BlockingQueue<Session> sessions = entry.getValue();
				for(Session session : sessions) {
					session.close();
				}
			}
			sessionPoolMap.clear();
			
			if(connection != null) {
				connection.close();
			}
			
		}catch(Exception e) {
			log.error("client dostop happen error! ", e);
		}
	}
	
	public void exceptionDispose(String brokerUrl) {
		try{
			for(Entry<String, BlockingQueue<Session>> entry : sessionPoolMap.entrySet()) {
				BlockingQueue<Session> sessions = entry.getValue();
				for(Session session : sessions) {
					if(session != null) {
						session.close();
					}
				}
			}
			sessionPoolMap.clear();
			
			if(connection != null) {
				connection.close();
			}
			
			if(sessionSemaphore != null) {
				sessionSemaphore = null;
			}
			
			badConnectionList.add(brokerUrl);
		}catch(Exception e) {
			log.error("exception dispose dostop happen error! ", e);
		}
	}
	
	public void reconnectionSchedule() {
		for(String brokerUrl : badConnectionList) {
			Connection connection = null;
			try{
				connection = createConnection(userName, password, brokerUrl);
	            
	            if(connection != null) {
	                connection.start();
	                connection.addExceptionListener(new ConnectionExceptionListener(this, brokerUrl));
	                createInitSessions(connection);
	                badConnectionList.remove(brokerUrl);
	                sessionSemaphore = new Semaphore(sessionInstance);
	                log.info("brokerUrl["+ brokerUrl +"] reconnect success! ");
	            }
			}catch(Exception e) {
				log.error("client do init happen error. ", e);
			}
		}
	}
	
	public void setConnector(HippoConnector connector) {
	    this.brokerUrl = connector.getBrokerUrl();
    	this.sessionInstance = connector.getSessionInstance();
    }
}
