package com.hippo.broker.cluster.server.handle;

import com.hippo.broker.cluster.ReplicatedConstants;
import com.hippo.broker.cluster.command.RegisterRequest;
import com.hippo.broker.cluster.simple.master.mdb.MdbMasterReplicatedServer;
import com.hippo.network.CommandResult;
import com.hippo.network.command.Command;
import com.hippo.network.transport.nio.CommandHandle;

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