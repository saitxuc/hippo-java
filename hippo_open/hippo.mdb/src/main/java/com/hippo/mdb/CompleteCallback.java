package com.hippo.mdb;

import com.hippo.common.exception.HippoException;
import com.hippo.mdb.obj.MdbPointer;

/**
 * 
 * @author saitxuc
 *
 */
public interface CompleteCallback {
    public void doComplete(MdbPointer info, long modifiedTime) throws HippoException;
}
