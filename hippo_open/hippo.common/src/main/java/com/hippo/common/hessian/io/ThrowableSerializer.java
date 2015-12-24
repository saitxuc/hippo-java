package com.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:25
 *
 */
public class ThrowableSerializer extends JavaSerializer {
	public ThrowableSerializer(Class cl, ClassLoader loader) {
		super(cl, loader);
	}

	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		Throwable e = (Throwable) obj;

		e.getStackTrace();

		super.writeObject(obj, out);
	}
}
