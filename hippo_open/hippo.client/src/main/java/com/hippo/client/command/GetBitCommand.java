package com.hippo.client.command;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;

/**
 * Created by Owen on 2015/11/26.
 */
public class GetBitCommand extends Command {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public GetBitCommand() {
        super();
    }

    @Override
    public String getAction() {
        return CommandConstants.BITGET_COMMAND_ACTION;
    }
}
