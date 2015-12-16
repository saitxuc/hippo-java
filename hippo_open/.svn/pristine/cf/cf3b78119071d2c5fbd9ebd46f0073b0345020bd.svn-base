package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.util.HashMap;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:35
 *
 */
public abstract class AbstractMapDeserializer extends AbstractDeserializer {

	public Class getType() {
		return HashMap.class;
	}

	public Object readObject(AbstractHessianInput in) throws IOException {
		Object obj = in.readObject();

		if (obj != null)
			throw error("expected map/object at " + obj.getClass().getName()
					+ " (" + obj + ")");
		else
			throw error("expected map/object at null");
	}
}
