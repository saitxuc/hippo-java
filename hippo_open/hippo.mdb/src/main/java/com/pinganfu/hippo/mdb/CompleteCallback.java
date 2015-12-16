package com.pinganfu.hippo.mdb;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.mdb.obj.MdbPointer;

/**
 * 
 * @author saitxuc
 *
 */
public interface CompleteCallback {
    public void doComplete(MdbPointer info, long modifiedTime) throws HippoException;
}
