package com.hippo.broker.cluster.controltable.client.mdb.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.command.ReplicatedBucketResponse;
import com.hippo.broker.cluster.controltable.client.mdb.MdbCtrlTableReplicatedClient;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.Response;
import com.hippo.network.transport.nio.CommandHandle;

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