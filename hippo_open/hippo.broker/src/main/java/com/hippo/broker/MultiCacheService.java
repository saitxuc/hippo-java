package com.hippo.broker;


/**
 * @author yangxin
 */
public class MultiCacheService {

//	private StoreEngine mdbEngine;
//	
//	public MultiCacheService(BrokerService brokerService, StoreEngine mdb, StoreEngine levelDb) {
//		super(brokerService, levelDb);
//		this.mdbEngine = mdb;
//	}
//
//	@Override
//	public HippoResult set(byte[] key, byte[] value, int bucket, int bizApp, int expire, int version) {
//		HippoResult result = super.set(key, value, bucket, bizApp, expire, version);
//		
//		if (result.isSuccess()) {
//			try {
//				mdbEngine.addData(key, value, (int)expire);
//			} catch (HippoStoreException e) {
//				// todo
//			}
//		}
//		
//		return result;
//	}
//
//	@Override
//	public HippoResult get(byte[] key, int bucket, int bizApp, int version) {
//		byte[] data = null;
//		try {
//			data = mdbEngine.getData(key);
//		} catch (HippoStoreException e) {
//			// todo
//		}
//		HippoResult result = new HippoResult(true);
//		if (data != null) {
//			result.setData(data);
//			return result;
//		}
//		
//		return super.get(key, bucket, bizApp, version);
//	}
//
//	@Override
//	public HippoResult remove(byte[] key, int bucket, int bizApp, int version) {
//		HippoResult result = super.remove(key, bucket, bizApp, version);
//		if (result.isSuccess()) {
//			try {
//				if (!mdbEngine.removeData(key)) {
//					result.setSuccess(false);
//				}
//			} catch (HippoStoreException e) {
//				result.setSuccess(false);
//			}
//		}
//		return result;
//	}
//	
}
