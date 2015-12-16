package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.util.Vector;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:00
 *
 */
public class EnumerationDeserializer extends AbstractListDeserializer {
	private static EnumerationDeserializer _deserializer;

	public static EnumerationDeserializer create() {
		if (_deserializer == null)
			_deserializer = new EnumerationDeserializer();

		return _deserializer;
	}

	public Object readList(AbstractHessianInput in, int length)
			throws IOException {
		Vector list = new Vector();

		in.addRef(list);

		while (!in.isEnd())
			list.add(in.readObject());

		in.readEnd();

		return list.elements();
	}
}
