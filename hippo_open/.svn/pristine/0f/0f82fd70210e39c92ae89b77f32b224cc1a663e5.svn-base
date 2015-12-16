package com.pinganfu.hippo.broker.cluster.server.handle;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketDataRequest;
import com.pinganfu.hippo.broker.cluster.simple.master.mdb.MdbMasterReplicatedServer;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

public class MasterReplicatedBucketDataRequestHandle implements CommandHandle {
    private MdbMasterReplicatedServer server;

    public MasterReplicatedBucketDataRequestHandle(MdbMasterReplicatedServer server) {
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
