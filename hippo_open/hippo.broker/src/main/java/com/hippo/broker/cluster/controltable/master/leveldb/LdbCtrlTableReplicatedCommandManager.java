package com.hippo.broker.cluster.controltable.master.leveldb;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.controltable.master.leveldb.handle.LdbCtrlTableReplicatedRequestHandle;
import com.hippo.network.BaseCommandManager;

/**
 * 
 * @author saitxuc
 * 2015-1-15
 */
public class LdbCtrlTableReplicatedCommandManager extends BaseCommandManager {

    private LdbCtrlTableReplicatedServer server;

    public LdbCtrlTableReplicatedCommandManager(LdbCtrlTableReplicatedServer server) {
        this.server = server;
        init();
    }

    @Override
    public void initConmandHandler() {
        LdbCtrlTableReplicatedRequestHandle requestHandle = new LdbCtrlTableReplicatedRequestHandle(server);
        
        // TODO: actions
        addCommandHandler(ReplicatedConstants.BUCKET_DATA_REQUEST_ACTION, requestHandle);

    }
}
