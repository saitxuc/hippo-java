package com.hippo.common.serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * 
 * @author saitxuc
 * 2015-3-20
 */
public class SerializerFactory {
	
	private static Map<String, Serializer> serializerMap = new HashMap<String, Serializer>();
	
	static {
		ServiceLoader<Serializer> serializers = ServiceLoader.load(Serializer.class);
		for (Serializer serializer : serializers) {
			serializerMap.put(serializer.getName(), serializer);
        }
	}
	
	public static Serializer findSerializer(String type)  {
		Serializer serializer = serializerMap.get(type);
		return serializer;
	}
	
}
