package com.hippo.common.hessian.io;


import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:35
 *
 */
public abstract class AbstractListDeserializer extends AbstractDeserializer {
	public Object readObject(AbstractHessianInput in) throws IOException {
		Object obj = in.readObject();

		if (obj != null)
			throw error("expected list at " + obj.getClass().getName() + " ("
					+ obj + ")");
		else
			throw error("expected list at null");
	}
}
