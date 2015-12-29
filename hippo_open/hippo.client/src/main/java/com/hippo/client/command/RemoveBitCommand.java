package com.hippo.client.command;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;

/**
 * Created by Owen on 2015/12/28.
 */
public class RemoveBitCommand extends Command {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public RemoveBitCommand() {
        super();
    }

    public String getAction() {
        return CommandConstants.BITREMOVE_COMMAND_ACTION;
    }
}