package com.hippo.mdb.obj;

import com.hippo.mdb.CompleteCallback;
import com.hippo.mdb.OperEnum;

/**
 * 
 * @author saitxuc
 * write 2014-8-5
 */
public class MdbGetOper extends MdbBaseOper {
	
	private MdbPointer sInfo;
	
	private byte[] gkey = null;
	
	public MdbGetOper(Integer buckNo, CompleteCallback callback) {
		super(OperEnum.GET_OPER, buckNo, callback);
	}
	
	public MdbGetOper(Integer buckNo, MdbPointer sInfo, byte[] gkey, CompleteCallback callback) {
		this(buckNo, callback);
		this.gkey = gkey;
		this.sInfo = sInfo;
	}

	public MdbPointer getsInfo() {
		return sInfo;
	}

	public void setsInfo(MdbPointer sInfo) {
		this.sInfo = sInfo;
	}

    public byte[] getGkey() {
        return gkey;
    }

    public void setGkey(byte[] gkey) {
        this.gkey = gkey;
    }

	
	
}
