package com.hippo.common.hessian.io;

import java.util.HashMap;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:00
 *
 */
public class ExtSerializerFactory extends AbstractSerializerFactory {
	private HashMap _serializerMap = new HashMap();
	private HashMap _deserializerMap = new HashMap();

	/**
	 * Adds a serializer.
	 * 
	 * @param cl
	 *            the class of the serializer
	 * @param serializer
	 *            the serializer
	 */
	public void addSerializer(Class cl, Serializer serializer) {
		_serializerMap.put(cl, serializer);
	}

	/**
	 * Adds a deserializer.
	 * 
	 * @param cl
	 *            the class of the deserializer
	 * @param deserializer
	 *            the deserializer
	 */
	public void addDeserializer(Class cl, Deserializer deserializer) {
		_deserializerMap.put(cl, deserializer);
	}

	/**
	 * Returns the serializer for a class.
	 * 
	 * @param cl
	 *            the class of the object that needs to be serialized.
	 * 
	 * @return a serializer object for the serialization.
	 */
	public Serializer getSerializer(Class cl) throws HessianProtocolException {
		return (Serializer) _serializerMap.get(cl);
	}

	/**
	 * Returns the deserializer for a class.
	 * 
	 * @param cl
	 *            the class of the object that needs to be deserialized.
	 * 
	 * @return a deserializer object for the serialization.
	 */
	public Deserializer getDeserializer(Class cl)
			throws HessianProtocolException {
		return (Deserializer) _deserializerMap.get(cl);
	}
}
