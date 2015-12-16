package com.pinganfu.hippo.mdb.obj;

import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.mdb.CompleteCallback;
import com.pinganfu.hippo.mdb.MdbOper;
import com.pinganfu.hippo.mdb.OperEnum;

/**
 * 
 * @author saitxuc
 * write 2014-8-5
 */
public abstract class MdbBaseOper implements MdbOper {

    public static final String ADD_OPER = "add";

    public static final String UPDATE_OPER = "update";

    public static final String REMOVE_OPER = "remove";

    public static final String GET_OPER = "get";
    
    public static final String BITSET_OPER = "bitset";

    public static final String BITGET_OPER = "bitget";

    private OperEnum oper;

    private Integer buckNo;

    private CompleteCallback completecallback;

    public MdbBaseOper(OperEnum oper, Integer buckNo, CompleteCallback callback) {
        this.oper = oper;
        this.buckNo = buckNo;
        this.completecallback = callback;
    }

    public void complete(MdbPointer info, long modifiedTime) throws HippoException {
        if (completecallback != null) {
            completecallback.doComplete(info, modifiedTime);
        }
    }

    public OperEnum getOper() {
        return oper;
    }

    public Integer getBuckNo() {
        return buckNo;
    }

}
