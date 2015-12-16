package com.pinganfu.hippo.broker.cluster.server.handle;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.broker.cluster.command.RegisterRequest;
import com.pinganfu.hippo.broker.cluster.simple.master.mdb.MdbMasterReplicatedServer;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.transport.nio.CommandHandle;

public class MasterRegisterRequestHandle implements CommandHandle {
    private MdbMasterReplicatedServer server;

    public MasterRegisterRequestHandle(MdbMasterReplicatedServer server) {
        this.server = server;
    }

    @Override
    public CommandResult doCommand(Command command) throws Exception {
        if (command instanceof RegisterRequest) {
            /*if (command.getAction().equals(ReplicatedConstants.REGISTER_RESPONSE_ACTION)) {
                return server.processRegisterRequest((RegisterRequest) command);
            } else if (command.getAction().equals(ReplicatedConstants.UNREGISTER_RESPONSE_ACTION)) {
                return server.processUnRegisterRequest((RegisterRequest) command);
            }*/
        }
        return null;
    }
}