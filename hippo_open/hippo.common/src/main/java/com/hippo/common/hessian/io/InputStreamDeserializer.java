package com.hippo.common.hessian.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 14:05
 *
 */
public class InputStreamDeserializer extends AbstractDeserializer {
	public InputStreamDeserializer() {
	}

	public Object readObject(AbstractHessianInput in) throws IOException {
		return in.readInputStream();
	}
}
