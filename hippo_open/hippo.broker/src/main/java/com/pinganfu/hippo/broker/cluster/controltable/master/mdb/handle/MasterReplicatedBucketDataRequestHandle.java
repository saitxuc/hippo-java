package com.pinganfu.hippo.broker.cluster.controltable.master.mdb.handle;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketDataRequest;
import com.pinganfu.hippo.broker.cluster.controltable.master.mdb.MdbCtrlTableReplicatedServer;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

public class MasterReplicatedBucketDataRequestHandle implements CommandHandle {
    private MdbCtrlTableReplicatedServer server;

    public MasterReplicatedBucketDataRequestHandle(MdbCtrlTableReplicatedServer server) {
        this.server = server;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
        if (command instanceof ReplicatedBucketDataRequest) {
            if (command.getAction().equals(ReplicatedConstants.BUCKET_DATA_REQUEST_ACTION)) {
                return server.processSyncDataRequest((ReplicatedBucketDataRequest) command);
            }
        }
        return null;
    }
}
