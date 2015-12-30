package com.hippo.broker.cluster.client.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.simple.client.mdb.MdbSlaveReplicatedClient;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.Response;
import com.hippo.network.transport.nio.CommandHandle;

public class SlaveReplicatedBucketDataResponseHandle implements CommandHandle {

    private MdbSlaveReplicatedClient client;

    public SlaveReplicatedBucketDataResponseHandle(MdbSlaveReplicatedClient client) {
        this.client = client;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
        if ((command instanceof Response)) {
            if (command.getAction().equals(ReplicatedConstants.BUCKET_DATA_REQUEST_ACTION)) {
                client.processSyncDataResponse((Response) command);
            }
        }
        return null;
    }
}
