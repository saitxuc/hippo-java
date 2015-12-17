package com.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:05
 *
 */
public interface Deserializer {
	public Class getType();

	public Object readObject(AbstractHessianInput in) throws IOException;

	public Object readList(AbstractHessianInput in, int length)
			throws IOException;

	public Object readLengthList(AbstractHessianInput in, int length)
			throws IOException;

	public Object readMap(AbstractHessianInput in) throws IOException;

	public Object readObject(AbstractHessianInput in, String[] fieldNames)
			throws IOException;
}
