package com.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:38
 *
 */
public class RemoteSerializer extends AbstractSerializer {
	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		// XXX: needs to be handled as a separate class
		throw new UnsupportedOperationException(getClass().getName());
	}
}
