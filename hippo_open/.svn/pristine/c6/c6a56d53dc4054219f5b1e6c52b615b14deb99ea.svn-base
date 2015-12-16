package com.pinganfu.hippo.broker.cluster.controltable.client.mdb.handle;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.ReplicatedBucketDataResponse;
import com.pinganfu.hippo.broker.cluster.controltable.client.mdb.MdbCtrlTableReplicatedClient;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.Response;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

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
