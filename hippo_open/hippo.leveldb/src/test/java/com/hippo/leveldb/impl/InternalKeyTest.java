package com.hippo.leveldb.impl;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.hippo.leveldb.WriteOptions;
import com.hippo.leveldb.util.Slice;

/**
 * @author yangxin
 */
public class InternalKeyTest {
	
	@Test
	public void testPackageKey() {
		int bucketNo = 1;
    	int bizApp = 1;
    	short vertionNo = 3;
    	long expireTime = 1425460058832l;
    	
    	WriteOptions wOptions = new WriteOptions();
		wOptions.bucket(bucketNo).bizApp(bizApp).version(vertionNo).expireTime(expireTime);
		
		Slice slice = InternalKey.packageKey_w("1".getBytes(), wOptions);
		InternalKey key = new InternalKey(slice, 1l, ValueType.VALUE);
		Assert.assertEquals(bucketNo, key.bucket());
		Assert.assertEquals(bizApp, key.bizApp());
		Assert.assertEquals(expireTime, key.expireTime());
		Assert.assertEquals(vertionNo, key.version());
	}
}
