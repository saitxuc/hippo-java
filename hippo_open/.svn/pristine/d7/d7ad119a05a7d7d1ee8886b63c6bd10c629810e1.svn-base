package com.pinganfu.hippo.network.command;

import com.pinganfu.hippo.network.SessionId;

/**
 * 
 * @author saitxuc
 *
 */
public class RemoveSessionCommand extends Command {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4071345362660642677L;

	public static final byte DATA_STRUCTURE_TYPE = CommandTypes.SESSION_REMOVE_COMMAND_TYPE;

    protected SessionId sessionId;
	
	public RemoveSessionCommand() {
		super();
	}
	
	@Override
	public String getAction() {
		return CommandConstants.SESION_REMOVE_INFO;
	}

	public SessionId getSessionId() {
		return sessionId;
	}

	public void setSessionId(SessionId sessionId) {
		this.sessionId = sessionId;
	}
    
}
