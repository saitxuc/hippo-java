package com.pinganfu.hippo.leveldb.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.pinganfu.hippo.leveldb.DBException;
import com.pinganfu.hippo.leveldb.ReadOptions;
import com.pinganfu.hippo.leveldb.WriteOptions;
import com.pinganfu.hippo.leveldb.cluster.Ddb;
import com.pinganfu.hippo.leveldb.cluster.Ddb.Position;
import com.pinganfu.hippo.leveldb.cluster.DdbFactory;
import com.pinganfu.hippo.leveldb.cluster.Entrys;
import com.pinganfu.hippo.leveldb.util.Slice;

/**
 * @author yangxin
 */
public class DdbTest extends DbImplTest {
	private Ddb db;
	ArrayList<Ddb> opened = new ArrayList<Ddb>();

	@BeforeMethod
	public void setUp() throws Exception {
		this.db = (Ddb) DdbFactory.factory.open();
		this.opened.add(db);
	}

	@AfterMethod
	public void tearDown() throws Exception {
		// 等待compact
		//sleep(10 * 1000);
		for (Ddb db : opened) {
			db.close();
		}
		opened.clear();
	}

	int appNo = 1;
	short vertionNo = 1;
	long expireTime = -1;
	static final String val = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABZASDFASDKLFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFH"
			+ "DFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHF"
			+ "LKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDF"
			+ "LKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJS"
			+ "DHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFAAAAAAAAAAAAAAAAAAAABSDFHJASDF"
			+ "LKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJ"
			+ "SDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJA"
			+ "SDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDF"
			+ "LKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJS"
			+ "DHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJ"
			+ "SDF";

	@Test
	public void testWrite() {
		WriteOptions options = new WriteOptions();
		options.bizApp(appNo).version(vertionNo).expireTime(expireTime);
		for (int index = 0; index < 10; index++) {
			sleep(1);
			options.bucket(index % 2);
			String key = "key" + index;
			String value = "This is element " + index + val;
			try {
				db.put(InternalKey.packageKey_w(key.getBytes("UTF-8"), options).getBytes(), value.getBytes("UTF-8"), options);
			} catch (DBException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void testGet() {
		ReadOptions options = new ReadOptions();
		options.bizApp(appNo).version(vertionNo);
		for (int index = 0; index < 10; index++) {
			sleep(1);
			options.bucket(index % 2);
			String key = "key" + index;
			String value = "This is element " + index + val;
			try {
				System.out.println(db.get(InternalKey.packageKey_q(key.getBytes("UTF-8"), options).getBytes(), options));
			} catch (DBException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void testDelete() {
		WriteOptions options = new WriteOptions();
		options.bizApp(appNo).version(vertionNo);
		for (int index = 0; index < 10; index++) {
			sleep(1);
			options.bucket(index % 2);
			String key = "key" + index;
			String value = "This is element " + index + val;
			try {
				db.delete(InternalKey.packageKey_d(key.getBytes("UTF-8"), options).getBytes(), options);
			} catch (DBException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
	}

	private void sleep(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testReadOriginalData() throws IOException {
		ReadOptions rOptions = new ReadOptions();
		rOptions.bucket(keyIndex % 5).bizApp(appNo).version(vertionNo);

		Assert.assertNotNull(db.get(InternalKey.packageKey_q(("key" + keyIndex).getBytes("UTF-8"), rOptions).getBytes()));
	}

	int keyIndex = 11;

	final String standby = "127.0.0.1";

	@Test
	public void testGet(int bucket) {
		Position pos = new Position();
		int count = 0;
		try {
			while (true) {
				Entrys entrys = db.get(standby, bucket, pos, 32 * 1024);
				if (entrys != null && entrys.size() > 0) {
					for (Entry<Slice, Slice> a : entrys.getData()) {
						System.out.println(new InternalKey(a.getKey(), 0, ValueType.VALUE));
						count++;
					}
				}
				System.out.println(Thread.currentThread().getId() + "===" + count + "-------------------------\n");
				if (!pos.hasNext()) {
					pos.hasNext(true);
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (Exception e) {
			System.out.println(Thread.currentThread().getId());
			e.printStackTrace();
		}
	}

	ExecutorService executorService = Executors.newFixedThreadPool(3);
	
	@Test
	public void testPut_Get() {
		Future<?> a = executorService.submit(new Runnable() {

			@Override
			public void run() {
				sleep(1000);
				testGet(0);
			}
		});
		Future<?> a1 = executorService.submit(new Runnable() {

			@Override
			public void run() {
				sleep(1000);
				testGet(1);
			}
		});
		Future<?> b = executorService.submit(new Runnable() {

			@Override
			public void run() {
				testWrite();
			}
		});

		while (!a.isDone() || !a1.isDone() || !b.isDone()) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
