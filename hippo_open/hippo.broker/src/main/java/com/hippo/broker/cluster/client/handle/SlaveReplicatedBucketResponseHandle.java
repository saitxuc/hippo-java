package com.hippo.broker.cluster.client.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.simple.client.mdb.MdbSlaveReplicatedClient;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.Response;
import com.hippo.network.transport.nio.CommandHandle;

public class SlaveReplicatedBucketResponseHandle implements CommandHandle {

    private MdbSlaveReplicatedClient client;

    public SlaveReplicatedBucketResponseHandle(MdbSlaveReplicatedClient client) {
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