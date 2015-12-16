package com.pinganfu.hippo.broker.cluster.server.handle;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketRequest;
import com.pinganfu.hippo.broker.cluster.simple.master.mdb.MdbMasterReplicatedServer;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

public class MasterReplicatedBucketRequestHandle implements CommandHandle {
    private MdbMasterReplicatedServer server;

    public MasterReplicatedBucketRequestHandle(MdbMasterReplicatedServer server) {
        this.server = server;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
        if (command instanceof ReplicatedBucketRequest) {
            if (command.getAction().equals(ReplicatedConstants.GET_BLOCK_UPDATE_LIST_ACTION)) {
                return server.processSyncTimeRequest((ReplicatedBucketRequest) command);
            } /*else if(command.getAction().equals(ReplicatedConstants.VERIFICATION_ACTION)){
                return server.processVerification((ReplicatedBucketRequest) command);
            }*/
        }
        return null;
    }
}
