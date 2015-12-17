package com.hippo.store;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * 
 * @author saitxuc
 * 2015-3-20
 */
public class StoreEngineFactory {
	
	private static Map<String, StoreEngine> storeEngineMap = new HashMap<String, StoreEngine>();
	
	static {
		ServiceLoader<StoreEngine> storeEngines = ServiceLoader.load(StoreEngine.class);
		for (StoreEngine storeEngine : storeEngines) {
			storeEngineMap.put(storeEngine.getName(), storeEngine);
        }
	}
	
	public static StoreEngine findStoreEngine(String type)  {
		StoreEngine storeEngine = storeEngineMap.get(type);
		return storeEngine;
	}
	
}
