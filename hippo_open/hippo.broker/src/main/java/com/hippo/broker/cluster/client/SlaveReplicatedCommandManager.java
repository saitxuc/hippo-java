package com.hippo.broker.cluster.client;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.client.handle.SlaveReplicatedBucketDataResponseHandle;
import com.hippo.broker.cluster.client.handle.SlaveReplicatedBucketResponseHandle;
import com.hippo.broker.cluster.simple.client.mdb.MdbSlaveReplicatedClient;
import com.hippo.network.BaseCommandManager;

public class SlaveReplicatedCommandManager extends BaseCommandManager<String> {

    private MdbSlaveReplicatedClient client;

    public SlaveReplicatedCommandManager(MdbSlaveReplicatedClient client) {
        this.client = client;
        init();
    }

    @Override
    public void initConmandHandler() {
        SlaveReplicatedBucketResponseHandle handle = new SlaveReplicatedBucketResponseHandle(this.client);
        SlaveReplicatedBucketDataResponseHandle datahandle = new SlaveReplicatedBucketDataResponseHandle(this.client);
        
        addCommandHandler(ReplicatedConstants.GET_BLOCK_UPDATE_LIST_ACTION, handle);
        addCommandHandler(ReplicatedConstants.HEART_BEAT_ACTION, handle);
        
        addCommandHandler(ReplicatedConstants.DELETE_BUCKET_DATA_RESPONES_ACTION, handle);
        
        addCommandHandler(ReplicatedConstants.EXPIRE_VERIFICATION_ACTION, datahandle);
        addCommandHandler(ReplicatedConstants.BUCKET_DATA_REQUEST_ACTION, datahandle);
    }
}