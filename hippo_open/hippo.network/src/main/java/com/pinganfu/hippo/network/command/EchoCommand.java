package com.pinganfu.hippo.network.command;

/**
 * 
 * @author saitxuc
 * write 2014-6-30
 */
public class EchoCommand extends Command {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static String ECHO_ACTION = "echo";
	
	public EchoCommand() {
		super();
	}
	
	@Override
	public String getAction() {
		return ECHO_ACTION;
	}
	
}
