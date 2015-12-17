package com.hippo.broker.cluster.controltable.master.mdb.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.command.HeartBeatRequest;
import com.hippo.broker.cluster.controltable.master.mdb.MdbCtrlTableReplicatedServer;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

public class MasterHeartBeatRequest implements CommandHandle {
    private MdbCtrlTableReplicatedServer server;

    public MasterHeartBeatRequest(MdbCtrlTableReplicatedServer server) {
        this.server = server;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
        if (command instanceof HeartBeatRequest) {
            if (command.getAction().equals(ReplicatedConstants.HEART_BEAT_ACTION)) {
                return server.processHeartBeatRequest((HeartBeatRequest) command);
            }
        }
        return null;
    }
}
