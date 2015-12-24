package com.hippo.leveldb.impl;

import java.util.concurrent.ConcurrentSkipListMap;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.hippo.leveldb.ReadOptions;
import com.hippo.leveldb.WriteOptions;
import com.hippo.leveldb.table.BytewiseComparator;
import com.hippo.leveldb.util.Slice;
import com.hippo.leveldb.util.Slices;

/**
 * @author yangxin
 */
public class MemTableTest {
	int bucketNo = 1;
	int appNo = 1;
	short vertionNo = 0;
	long expireTime = System.currentTimeMillis();
	
	private ConcurrentSkipListMap<InternalKey, Slice> memTable;
	WriteOptions wOptions;
	ReadOptions rOptions;

	@BeforeMethod
	public void setup() {
		memTable = new ConcurrentSkipListMap(new InternalKeyComparator(new BytewiseComparator()));
		wOptions = new WriteOptions().bucket(bucketNo).bizApp(appNo).version(vertionNo).expireTime(expireTime);
		rOptions = new ReadOptions().bucket(bucketNo).bizApp(appNo);
	}


	@Test
	public void testGet() {
		InternalKey key = new InternalKey(InternalKey.packageKey_w("key0".getBytes(), wOptions), 1, ValueType.VALUE);
		memTable.put(key, Slices.wrappedBuffer("111".getBytes()));
		
		InternalKey qKey = new InternalKey(InternalKey.packageKey_q("key0".getBytes(), rOptions), 2, ValueType.VALUE);
		System.out.println(memTable.ceilingKey(qKey));
	}
}
