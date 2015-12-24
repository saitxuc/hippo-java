package com.hippo.broker.cluster.controltable.client;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.controltable.client.mdb.MdbCtrlTableReplicatedClient;
import com.hippo.broker.cluster.controltable.client.mdb.handle.CtrlTableSlaveReplicatedBucketDataResponseHandle;
import com.hippo.broker.cluster.controltable.client.mdb.handle.CtrlTableSlaveReplicatedBucketResponseHandle;
import com.hippo.network.BaseCommandManager;

public class CtrlTableSlaveReplicatedCommandManager extends BaseCommandManager<String> {

    private MdbCtrlTableReplicatedClient client;

    public CtrlTableSlaveReplicatedCommandManager(MdbCtrlTableReplicatedClient client) {
        this.client = client;
        init();
    }

    @Override
    public void initConmandHandler() {
        CtrlTableSlaveReplicatedBucketResponseHandle handle = new CtrlTableSlaveReplicatedBucketResponseHandle(this.client);
        CtrlTableSlaveReplicatedBucketDataResponseHandle datahandle = new CtrlTableSlaveReplicatedBucketDataResponseHandle(this.client);
        
        addCommandHandler(ReplicatedConstants.GET_BLOCK_UPDATE_LIST_ACTION, handle);
        addCommandHandler(ReplicatedConstants.HEART_BEAT_ACTION, handle);
        addCommandHandler(ReplicatedConstants.DELETE_BUCKET_DATA_RESPONES_ACTION, handle);
        addCommandHandler(ReplicatedConstants.EXPIRE_VERIFICATION_ACTION, datahandle);
        addCommandHandler(ReplicatedConstants.BUCKET_DATA_REQUEST_ACTION, datahandle);
        
        addCommandHandler(ReplicatedConstants.RESET_BUCKET_RESPONES_ACTION, handle);
    }
}