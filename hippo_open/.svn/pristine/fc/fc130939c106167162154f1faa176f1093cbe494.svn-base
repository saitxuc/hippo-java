package com.pinganfu.hippo.mdb.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.pinganfu.hippo.mdb.Lru;
import com.pinganfu.hippo.mdb.obj.DBAssembleInfo;
import com.pinganfu.hippo.mdb.obj.DBInfo;
import com.pinganfu.hippo.mdb.obj.OffsetInfo;
import com.pinganfu.hippo.store.exception.HippoStoreException;

/**
 * 
 * @author saitxuc
 * 2015-1-12
 */
public class BasicExpireLru implements Lru {
	
	private List<OffsetInfo> offsets = new ArrayList<OffsetInfo>(); 
	
	private DBAssembleInfo dbAssembleInfo = null;
	
	public BasicExpireLru(DBAssembleInfo dbAssembleInfo) {
		this.dbAssembleInfo = dbAssembleInfo;
	}
	
	public void addOffsetInfo(OffsetInfo info) {
		synchronized(offsets){
			offsets.add(info);
			this.sort();
		}
	}
	
	@Override
	public OffsetInfo lru() throws HippoStoreException{
		synchronized(offsets){
			OffsetInfo info = offsets.remove(0);
			info.setLru(true);
			DBInfo dbinfo = this.dbAssembleInfo.getDbInfoMap().get(info.getDbNo());
			this.addOffsetInfo(dbinfo.getMinExpire());
			return info;
		}
	}
	
	
	private void sort() {
		synchronized(offsets){
			System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
			Collections.sort(offsets, new Comparator<OffsetInfo>() {
	            public int compare(OffsetInfo fo, OffsetInfo to) {
	                if (null == fo || null == to) {
	                	return 0;
	                }
	                long expire = fo.getExpireTime() - to.getExpireTime();
	                if(expire == 0) {
	                	return 0;
	                }else{
	                	if (expire > 0) {
	                        return 1;
	                    } else {
	                        return -1;
	                    }
	                }
	                
	            };
	        });
		}
	}
}
