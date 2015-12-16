package com.pinganfu.hippo.broker.cluster.client.handle;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.simple.client.mdb.MdbSlaveReplicatedClient;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.Response;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

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
