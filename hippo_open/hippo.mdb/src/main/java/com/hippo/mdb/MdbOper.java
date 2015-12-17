package com.hippo.mdb;

import com.hippo.common.exception.HippoException;
import com.hippo.mdb.obj.MdbPointer;

/**
 * 
 * @author saitxuc
 * write 2014-8-5
 */
public interface MdbOper {

    public void complete(MdbPointer info, long modifiedTime) throws HippoException;

    public OperEnum getOper();

}
