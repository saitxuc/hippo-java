package com.pinganfu.hippo.broker.cluster.controltable.client.mdb.handle;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketResponse;
import com.pinganfu.hippo.broker.cluster.controltable.client.mdb.MdbCtrlTableReplicatedClient;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.Response;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

public class CtrlTableSlaveReplicatedBucketResponseHandle implements CommandHandle {

    private MdbCtrlTableReplicatedClient client;

    public CtrlTableSlaveReplicatedBucketResponseHandle(MdbCtrlTableReplicatedClient client) {
        this.client = client;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
        if (command instanceof Response) {
            if (command.getAction().equals(ReplicatedConstants.GET_BLOCK_UPDATE_LIST_ACTION)) {
                client.processSyncTimeResponse((Response) command);
            } else if (command.getAction().equals(ReplicatedConstants.HEART_BEAT_ACTION)) {
                client.processHeartBeatResponse((Response) command);
            }
        }
        return null;
    }
}