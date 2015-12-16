package com.pinganfu.hippo.broker.cluster.controltable.master.leveldb.handle;

import com.pinganfu.hippo.broker.cluster.controltable.master.leveldb.LdbCtrlTableReplicatedServer;
import com.pinganfu.hippo.broker.cluster.simple.client.leveldb.ClientProxy.ReplicationRequest;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

public class LdbCtrlTableReplicatedRequestHandle implements CommandHandle {
    private LdbCtrlTableReplicatedServer server;

    public LdbCtrlTableReplicatedRequestHandle(LdbCtrlTableReplicatedServer server) {
        this.server = server;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
        if (command instanceof ReplicationRequest) {
            return server.handleCommand(command);
        }
        return null;
    }
}
