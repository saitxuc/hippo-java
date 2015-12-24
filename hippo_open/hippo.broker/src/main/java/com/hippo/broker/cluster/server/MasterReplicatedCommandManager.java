package com.hippo.broker.cluster.server;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.server.handle.MasterHeartBeatRequest;
import com.hippo.broker.cluster.server.handle.MasterReplicatedBucketDataRequestHandle;
import com.hippo.broker.cluster.server.handle.MasterReplicatedBucketRequestHandle;
import com.hippo.broker.cluster.simple.master.mdb.MdbMasterReplicatedServer;
import com.hippo.network.BaseCommandManager;

public class MasterReplicatedCommandManager extends BaseCommandManager<String> {
    private MdbMasterReplicatedServer server;

    public MasterReplicatedCommandManager(MdbMasterReplicatedServer server) {
        this.server = server;
        init();
    }

    @Override
    public void initConmandHandler() {
        MasterReplicatedBucketRequestHandle requestHandle = new MasterReplicatedBucketRequestHandle(server);
        MasterReplicatedBucketDataRequestHandle dataRequestHandle = new MasterReplicatedBucketDataRequestHandle(server);
        MasterHeartBeatRequest heartBeatHandle = new MasterHeartBeatRequest(server);
        //MasterRegisterRequestHandle registerHandle = new MasterRegisterRequestHandle(server);

        addCommandHandler(ReplicatedConstants.GET_BLOCK_UPDATE_LIST_ACTION, requestHandle);
        addCommandHandler(ReplicatedConstants.VERIFICATION_ACTION, requestHandle);
        
        addCommandHandler(ReplicatedConstants.EXPIRE_VERIFICATION_ACTION, dataRequestHandle);
        addCommandHandler(ReplicatedConstants.BUCKET_DATA_REQUEST_ACTION, dataRequestHandle);
        
        addCommandHandler(ReplicatedConstants.HEART_BEAT_ACTION, heartBeatHandle);
        
        //addCommandHandler(ReplicatedConstants.REGISTER_RESPONSE_ACTION, registerHandle);
        //addCommandHandler(ReplicatedConstants.UNREGISTER_RESPONSE_ACTION, registerHandle);
    }
}
