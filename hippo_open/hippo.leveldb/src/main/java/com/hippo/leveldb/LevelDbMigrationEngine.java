package com.hippo.leveldb;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.leveldb.cluster.Ddb.Position;
import com.hippo.leveldb.cluster.Entrys;
import com.hippo.store.MigrationEngine;
import com.hippo.store.StoreEngine;

/**
 * @author yangxin
 */
public class LevelDbMigrationEngine extends LifeCycleSupport implements MigrationEngine {
    private LevelDbStoreEngine storeEngine;
    
    public LevelDbMigrationEngine(StoreEngine storeEngine) {
		this.storeEngine = (LevelDbStoreEngine)storeEngine;
	}

	@Override
    public Map<String, List<String>> getBucketStorageInfo(String bucketNo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] migration(String bucketNo, String sizetype, String blockNo, int offset, int size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replicated(String bucketNo, String sizeType, String blocketNo, byte[] data) {
        throw new UnsupportedOperationException();
    }

	/**
	 * @throws RuntimeException 可以重试的异常
	 */
    public byte[] migration(String standby, int bucket, Object offset, int size) {
    	Preconditions.checkNotNull(standby, "standby is null");
        Preconditions.checkNotNull(offset, "offset is null");
        
        Position pos = (Position)offset;
        Entrys entrys = storeEngine.get(standby, bucket, pos, size);
        return entrys.encode().getBytes();
    }
    
    public long getMaxSeq(int bucket) {
		 return storeEngine.getMaxSeq(bucket);
	}

    @Override
    public void resetBuckets(List<BucketInfo> bucketNos) {
        throw new UnsupportedOperationException();
    }

	@Override
	public Throwable getStartException() {
		return null;
	}

	@Override
	public void doInit() {
		storeEngine.init();
	}

	@Override
	public void doStart() {
		storeEngine.start();
	}

	@Override
	public void doStop() {
		//TODO
	}

    @Override
    public String getName() {
        return storeEngine.getName();
    }

	public LevelDbStoreEngine getStoreEngine() {
		return storeEngine;
	}

}
