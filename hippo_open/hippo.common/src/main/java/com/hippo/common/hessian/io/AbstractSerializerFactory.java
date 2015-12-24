package com.hippo.common.hessian.io;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:25
 *
 */
public abstract class AbstractSerializerFactory {
	
	/**
	 * Returns the serializer for a class.
	 * 
	 * @param cl
	 *            the class of the object that needs to be serialized.
	 * 
	 * @return a serializer object for the serialization.
	 */
	abstract public Serializer getSerializer(Class cl)
			throws HessianProtocolException;

	/**
	 * Returns the deserializer for a class.
	 * 
	 * @param cl
	 *            the class of the object that needs to be deserialized.
	 * 
	 * @return a deserializer object for the serialization.
	 */
	abstract public Deserializer getDeserializer(Class cl)
			throws HessianProtocolException;
	
}
