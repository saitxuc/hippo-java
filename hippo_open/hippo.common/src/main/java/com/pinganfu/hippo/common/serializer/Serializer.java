package com.pinganfu.hippo.common.serializer;

import java.io.IOException;

/**
 * 
 * @author saitxuc
 * 2015-1-26
 */
public interface Serializer {
	
	<T> byte[] serialize(T obj) throws IOException;

	<T> T deserialize(byte[] source, Class<T> clazz) throws IOException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException;
	
	String getName();
	
	void close();
}
