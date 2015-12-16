package com.pinganfu.hippo.broker.cluster.controltable.master.mdb;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.controltable.master.mdb.handle.MasterHeartBeatRequest;
import com.pinganfu.hippo.broker.cluster.controltable.master.mdb.handle.MasterRegisterRequestHandle;
import com.pinganfu.hippo.broker.cluster.controltable.master.mdb.handle.MasterReplicatedBucketDataRequestHandle;
import com.pinganfu.hippo.broker.cluster.controltable.master.mdb.handle.MasterReplicatedBucketRequestHandle;
import com.pinganfu.hippo.network.BaseCommandManager;

public class MdbCtrlTableReplicatedCommandManager extends BaseCommandManager {
    private MdbCtrlTableReplicatedServer server;

    public MdbCtrlTableReplicatedCommandManager(MdbCtrlTableReplicatedServer server) {
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
