package com.pinganfu.hippo.broker.cluster;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.pinganfu.hippo.broker.store.StoreConstants;
import com.pinganfu.hippo.leveldb.LevelDbStoreEngine;
import com.pinganfu.hippo.store.StoreEngineFactory;
import com.pinganfu.hippo.store.exception.HippoStoreException;

/**
 * @author yangxin
 */
public class BackupTest {
	private LevelDbStoreEngine storeEngine;

	@BeforeMethod
	public void setUp() {
		storeEngine = (LevelDbStoreEngine) StoreEngineFactory.findStoreEngine(StoreConstants.STORE_ENGINE_LEVELDB);
		storeEngine.init();
		storeEngine.start();
	}

	@AfterMethod
	public void tearDown() throws Exception {

		try {
			Thread.sleep(60 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		storeEngine.stop();
	}

	int keyIndex = 101;

	@Test
	public void testBackup() {
		//		backup.replicate("127.0.0.1", keyIndex % 5);

		addData();
	}

	short appNo = 1;
	int expireTime = 300;

	private void addData() {
		for (int index = 0; index < 1000; index++) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			String key = "key" + index;
			String value = "This is element "
					+ index
					+ "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABZASDFASDKLFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDF";
			try {
				storeEngine.add(key.getBytes(), value.getBytes(), index % 5, appNo, expireTime, 0);
			} catch (HippoStoreException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		BackupTest test = new BackupTest();
		test.setUp();
		test.testBackup();
		try {
			test.tearDown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
