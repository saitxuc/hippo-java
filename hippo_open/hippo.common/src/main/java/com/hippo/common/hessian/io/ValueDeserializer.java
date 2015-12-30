package com.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:23
 *
 */
abstract public class ValueDeserializer extends AbstractDeserializer {
	public Object readMap(AbstractHessianInput in) throws IOException {
		String initValue = null;

		while (!in.isEnd()) {
			String key = in.readString();

			if (key.equals("value"))
				initValue = in.readString();
			else
				in.readObject();
		}

		in.readMapEnd();

		return create(initValue);
	}

	public Object readObject(AbstractHessianInput in, String[] fieldNames)
			throws IOException {
		String initValue = null;

		for (int i = 0; i < fieldNames.length; i++) {
			if ("value".equals(fieldNames[i]))
				initValue = in.readString();
			else
				in.readObject();
		}

		return create(initValue);
	}

	abstract Object create(String value) throws IOException;
}
