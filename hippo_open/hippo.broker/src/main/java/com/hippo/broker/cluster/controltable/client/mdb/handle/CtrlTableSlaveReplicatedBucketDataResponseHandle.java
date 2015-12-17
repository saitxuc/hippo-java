package com.hippo.broker.cluster.controltable.client.mdb.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.command.ReplicatedBucketDataResponse;
import com.hippo.broker.cluster.controltable.client.mdb.MdbCtrlTableReplicatedClient;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.command.Response;
import com.hippo.network.transport.nio.CommandHandle;

public class CtrlTableSlaveReplicatedBucketDataResponseHandle implements CommandHandle {

    private MdbCtrlTableReplicatedClient client;

    public CtrlTableSlaveReplicatedBucketDataResponseHandle(MdbCtrlTableReplicatedClient client) {
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
