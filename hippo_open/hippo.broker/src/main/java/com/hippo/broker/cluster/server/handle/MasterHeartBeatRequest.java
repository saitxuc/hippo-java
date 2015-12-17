package com.hippo.broker.cluster.server.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.command.HeartBeatRequest;
import com.hippo.broker.cluster.simple.master.mdb.MdbMasterReplicatedServer;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

public class MasterHeartBeatRequest implements CommandHandle {
    private MdbMasterReplicatedServer server;

    public MasterHeartBeatRequest(MdbMasterReplicatedServer server) {
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
