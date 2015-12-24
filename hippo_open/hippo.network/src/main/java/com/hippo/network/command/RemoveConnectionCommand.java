package com.hippo.network.command;

import com.hippo.network.ConnectionId;

/**
 * 
 * @author saitxuc
 *
 */
public class RemoveConnectionCommand  extends Command{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1356453305372568804L;
	
	public static byte REMOVE_TYPE = CommandTypes.CONNECT_REMOVE_COMMAND_TYPE;
	
	protected ConnectionId connectionId;
	
	public RemoveConnectionCommand() {
		super();
	}
	
	@Override
	public String getAction() {
		return CommandConstants.CONNECT_REMOVE_INFO;
	}

	public ConnectionId getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(ConnectionId connectionId) {
		this.connectionId = connectionId;
	}
	
}
