package com.pinganfu.hippo.client.util;

import com.pinganfu.hippo.common.util.KeyHashUtil;

public class HippoClientUtil{

	
	public static int distributeBucket(byte[] key, int bucketSize) {
		return KeyHashUtil.calculateHashBucket(key, bucketSize);
	}
	
}
