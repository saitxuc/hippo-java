package com.hippo.mdb;

import com.hippo.mdb.obj.OffsetInfo;
import com.hippo.store.exception.HippoStoreException;

/**
 * 
 * @author saitxuc
 * write 2014-8-5
 */
public interface Lru {
	
	public OffsetInfo lru() throws HippoStoreException;
	
	public void addOffsetInfo(OffsetInfo info);
	
}
