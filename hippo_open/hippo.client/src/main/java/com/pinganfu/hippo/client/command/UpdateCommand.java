package com.pinganfu.hippo.client.command;

import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.CommandConstants;

/**
 * @author saitxuc
 */
public class UpdateCommand extends Command {

    /**
     *
     */
    private static final long serialVersionUID = 2994021214222549201L;

    private int expire;

    private int klength;

    public UpdateCommand() {
        super();
    }

    @Override
    public String getAction() {
        return CommandConstants.UPDATE_COMMAND_ACTION;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public int getKlength() {
        return klength;
    }

    public void setKlength(int klength) {
        this.klength = klength;
    }

}
