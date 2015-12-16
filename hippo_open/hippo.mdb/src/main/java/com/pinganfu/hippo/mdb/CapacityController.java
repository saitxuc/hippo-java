package com.pinganfu.hippo.mdb;

import com.pinganfu.hippo.common.lifecycle.LifeCycle;
import com.pinganfu.hippo.mdb.exception.OutOfMaxCapacityException;
import com.pinganfu.hippo.mdb.obj.DBInfo;

public interface CapacityController extends LifeCycle{
    public DBInfo getNewDbInfo(int capacity, String b_size, String dbNo, Integer bucketNo) throws OutOfMaxCapacityException;
    
    public void notifyWait();
}
