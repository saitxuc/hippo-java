package com.pinganfu.hippo.broker.cluster.server.handle;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.HeartBeatRequest;
import com.pinganfu.hippo.broker.cluster.simple.master.mdb.MdbMasterReplicatedServer;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

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
