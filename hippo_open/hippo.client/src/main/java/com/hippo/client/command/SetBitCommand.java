package com.hippo.client.command;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;

/**
 * Created by Owen on 2015/11/27.
 */
public class SetBitCommand extends Command {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private int expire;

    public SetBitCommand() {
        super();
    }

    @Override
    public String getAction() {
        return CommandConstants.BITSET_COMMAND_ACTION;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }
}
