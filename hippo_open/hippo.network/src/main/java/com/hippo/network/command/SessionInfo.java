package com.hippo.network.command;

import com.hippo.network.SessionId;

/**
 * 
 * @author saitxuc
 * write 2014-7-10
 */
public class SessionInfo extends Command {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 9022886581236765670L;

	public static final byte DATA_STRUCTURE_TYPE = CommandTypes.SESSION_INFO;

    protected SessionId sessionId;
    
    private String connectionId;
    
    public SessionInfo() {
        sessionId = new SessionId();
    }
    
    public SessionInfo copy() {
    	SessionInfo copy = new SessionInfo();
    	copy(copy);
        return copy;
    }
    
    protected void copy(SessionInfo copy) {
    	super.copy(copy);
        copy.sessionId = sessionId;
        copy.connectionId = connectionId;
    }
    
    public SessionInfo(ConnectionInfo connectionInfo, long sessionId) {
    	this.setAction(CommandConstants.SESSION_INFO);
    	this.connectionId = connectionInfo.getConnectionId().getValue();
    	this.sessionId = new SessionId(connectionInfo.getConnectionId(), sessionId);
    }

    public SessionInfo(SessionId sessionId) {
        this.sessionId = sessionId;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public SessionId getSessionId() {
        return sessionId;
    }

    public void setSessionId(SessionId sessionId) {
        this.sessionId = sessionId;
    }

	public String getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}
    
    
    
    
    public RemoveSessionCommand createRemoveCommand() {
    	RemoveSessionCommand command = new RemoveSessionCommand();
    	command.setSessionId(getSessionId());
        return command;
    }
    
    /***
    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processAddSession(this);
    }
	***/
	
}



