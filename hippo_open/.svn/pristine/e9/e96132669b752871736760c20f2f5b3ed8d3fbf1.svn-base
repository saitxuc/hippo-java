package com.pinganfu.hippo.jmx;

import java.util.concurrent.Callable;

import com.pinganfu.hippo.store.StoreEngine;

/**
 * 
 * @author saitxuc
 *
 */
public class StoreAdapterView implements StoreAdapterViewMBean {
	
	private final String name;
	
	private final StoreEngine storeEngine;
	
	private Callable<String> dataViewCallable;
	
	public StoreAdapterView(StoreEngine storeEngine) {
		this.name = storeEngine.getName();
		this.storeEngine = storeEngine;
	}
	
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return name;
	}

	@Override
	public String getData() {
		// TODO Auto-generated method stub
		return invoke(dataViewCallable);
	}

	@Override
	public long getSize() {
		// TODO Auto-generated method stub
		return storeEngine.size();
	}
	
    private String invoke(Callable<String> callable) {
        String result = null;
        if (callable != null) {
            try {
                result = callable.call();
            } catch (Exception e) {
                result = e.toString();
            }
        }
        return result;
    }
    
    public void setDataViewCallable(Callable<String> dataViewCallable) {
        this.dataViewCallable = dataViewCallable;
    }

	@Override
	public long getCurrentUsedCapacity() {
		// TODO Auto-generated method stub
		return storeEngine.getCurrentUsedCapacity();
	}
    
}
