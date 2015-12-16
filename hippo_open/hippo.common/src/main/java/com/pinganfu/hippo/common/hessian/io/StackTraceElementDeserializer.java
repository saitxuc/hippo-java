package com.pinganfu.hippo.common.hessian.io;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:30
 *
 */
public class StackTraceElementDeserializer extends JavaDeserializer {
	public StackTraceElementDeserializer() {
		super(StackTraceElement.class);
	}

	@Override
	protected Object instantiate() throws Exception {
		return new StackTraceElement("", "", "", 0);
	}
}
