package com.hippo.client.util;

import com.hippo.common.util.KeyHashUtil;

public class HippoClientUtil{

	
	public static int distributeBucket(byte[] key, int bucketSize) {
		return KeyHashUtil.calculateHashBucket(key, bucketSize);
	}
	
}
