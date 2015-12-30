package com.hippo.broker.cluster.controltable.master.mdb.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.command.ReplicatedBucketRequest;
import com.hippo.broker.cluster.controltable.master.mdb.MdbCtrlTableReplicatedServer;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

public class MasterReplicatedBucketRequestHandle implements CommandHandle {
    private MdbCtrlTableReplicatedServer server;

    public MasterReplicatedBucketRequestHandle(MdbCtrlTableReplicatedServer server) {
        this.server = server;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
        if (command instanceof ReplicatedBucketRequest) {
            if (command.getAction().equals(ReplicatedConstants.GET_BLOCK_UPDATE_LIST_ACTION)) {
                return server.processSyncTimeRequest((ReplicatedBucketRequest) command);
            }/* else if(command.getAction().equals(ReplicatedConstants.VERIFICATION_ACTION)){
                return server.processVerification((ReplicatedBucketRequest) command);
            }*/
        }
        return null;
    }
}
