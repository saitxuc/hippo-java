package com.hippo.leveldb.cluster;

/**
 * @author yangxin
 */
public interface BConstansts {
	/** 默认的版本号 */
	static final short KEY_VERSION = 1;
	
	/** 表示永远不会过期 */
	static final int KEY_EXPIRE_TIME = -1;
	
	/** 默认的应用编号 */
	static final short KEY_BIZ_APP = 0;
	
	/** 默认的bucket */
	static final short KEY_BUCKET = 0;
}
