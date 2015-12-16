package com.pinganfu.hippo.network.command;

import com.pinganfu.hippo.network.ConnectionId;

/**
 * 
 * @author saitxuc
 * write 2014-7-8
 */
public class ConnectionInfo extends Command {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final byte DATA_STRUCTURE_TYPE = CommandTypes.CONNECTION_INFO;

    protected ConnectionId connectionId;
    protected String clientId;
    protected String clientIp;
    protected String userName;
    protected String password;
    protected boolean failoverReconnect;
    protected transient Object transportContext;

    public ConnectionInfo() {
    }

    public ConnectionInfo(ConnectionId connectionId) {
    	this.setAction(CommandConstants.CONNECTION_INFO);
        this.connectionId = connectionId;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public ConnectionInfo copy() {
        ConnectionInfo copy = new ConnectionInfo();
        copy(copy);
        return copy;
    }

    private void copy(ConnectionInfo copy) {
        super.copy(copy);
        copy.connectionId = connectionId;
        copy.clientId = clientId;
        copy.userName = userName;
        copy.password = password;
        copy.transportContext = transportContext;
        copy.clientIp = clientIp;
    }


    public ConnectionId getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
    
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }
    

    public Object getTransportContext() {
        return transportContext;
    }

    
    public void setTransportContext(Object transportContext) {
        this.transportContext = transportContext;
    }

    public boolean isFailoverReconnect() {
        return this.failoverReconnect;
    }

    public void setFailoverReconnect(boolean failoverReconnect) {
        this.failoverReconnect = failoverReconnect;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }
	
}
