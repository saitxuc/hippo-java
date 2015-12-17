package com.hippo.common.hessian.io;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:25
 *
 */
public class BeanSerializerFactory extends SerializerFactory {
	/**
	 * Returns the default serializer for a class that isn't matched directly.
	 * Application can override this method to produce bean-style serialization
	 * instead of field serialization.
	 * 
	 * @param cl
	 *            the class of the object that needs to be serialized.
	 * 
	 * @return a serializer object for the serialization.
	 */
	protected Serializer getDefaultSerializer(Class cl) {
		return new BeanSerializer(cl, getClassLoader());
	}

	/**
	 * Returns the default deserializer for a class that isn't matched directly.
	 * Application can override this method to produce bean-style serialization
	 * instead of field serialization.
	 * 
	 * @param cl
	 *            the class of the object that needs to be serialized.
	 * 
	 * @return a serializer object for the serialization.
	 */
	protected Deserializer getDefaultDeserializer(Class cl) {
		return new BeanDeserializer(cl);
	}
}
