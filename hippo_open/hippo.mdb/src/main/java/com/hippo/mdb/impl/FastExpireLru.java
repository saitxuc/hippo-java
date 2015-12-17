package com.hippo.mdb.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hippo.mdb.Lru;
import com.hippo.mdb.obj.DBAssembleInfo;
import com.hippo.mdb.obj.DBInfo;
import com.hippo.mdb.obj.OffsetInfo;
import com.hippo.store.exception.HippoStoreException;

/**
 * 
 * @author saitxuc
 * 2015-1-12 
 */
public class FastExpireLru implements Lru {
	
	private AtomicBoolean directNext = new AtomicBoolean(true);
	
	private DBAssembleInfo dbAssembleInfo = null;
	
	public FastExpireLru(DBAssembleInfo dbAssembleInfo) {
		this.dbAssembleInfo = dbAssembleInfo;
	}
	
	@Override
	public OffsetInfo lru() throws HippoStoreException{
		DBInfo currentDB = dbAssembleInfo.getCurrentDB();
		return lruInternal(currentDB);
	}
	
	
	private OffsetInfo lruInternal(DBInfo dbinfo) {
		Map<String, DBInfo> dbInfoMap = dbAssembleInfo.getDbInfoMap();
		DBInfo currentDB = dbinfo;
		if(currentDB != null) {
			if(dbInfoMap.size() >= 2) {
				if(directNext.get()) {
				    /*DBInfo tempDB = currentDB.getNext();
					if(tempDB == null) {
						directNext.set(false);
						return lruInternal(currentDB);
					}
					
					currentDB = tempDB;
					if(currentDB.isExpiring()){
						return lruInternal(currentDB);
					}*/
				}else{
					/*DBInfo tempDB = currentDB.getPrevious();
					if(tempDB == null ) {
						directNext.set(true);
						return lruInternal(currentDB);
					}
					currentDB = tempDB;
					if(currentDB.isExpiring()){
						return lruInternal(currentDB);
					}*/
				}
			}
			OffsetInfo offsetInfo = currentDB.getMinExpire();
			if(offsetInfo == null){
			    return null;
			}
			offsetInfo.setLru(true);
			return offsetInfo;
		}
		return null;
	}
	
	@Override
	public void addOffsetInfo(OffsetInfo info) {
		//no use
	}
}
