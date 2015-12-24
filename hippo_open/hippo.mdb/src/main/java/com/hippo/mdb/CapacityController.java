package com.hippo.mdb;

import com.hippo.common.lifecycle.LifeCycle;
import com.hippo.mdb.exception.OutOfMaxCapacityException;
import com.hippo.mdb.obj.DBInfo;

public interface CapacityController extends LifeCycle{
    public DBInfo getNewDbInfo(int capacity, String b_size, String dbNo, Integer bucketNo) throws OutOfMaxCapacityException;
    
    public void notifyWait();
}
