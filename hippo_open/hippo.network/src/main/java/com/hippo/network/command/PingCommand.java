package com.hippo.network.command;

/**
 * 
 * @author saitxuc
 *
 */
public class PingCommand extends Command {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6154250375761782414L;
	
	public static String PING_ACTION = "ping";
	
	public PingCommand() {
		super();
	}
	
	@Override
	public String getAction() {
		return PING_ACTION;
	}
	
}
