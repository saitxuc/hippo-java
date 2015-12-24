package com.hippo.broker.cluster.controltable.master.mdb.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.command.ReplicatedBucketDataRequest;
import com.hippo.broker.cluster.controltable.master.mdb.MdbCtrlTableReplicatedServer;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

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
