package com.hippo.client.command;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;

/**
 * @author saitxuc
 *         write 2014-6-30
 */
public class GetCommand extends Command {

    private static final long serialVersionUID = 1L;

    public GetCommand() {
        super();
    }

    @Override
    public String getAction() {
        return CommandConstants.GET_COMMAND_ACTION;
    }

}
