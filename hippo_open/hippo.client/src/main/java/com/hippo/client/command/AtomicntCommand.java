package com.hippo.client.command;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;

/**
 *
 */
public class AtomicntCommand extends Command {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private long initv;

    private long delta;

    private int expire;

    public AtomicntCommand() {
        super();
    }

    @Override
    public String getAction() {
        return CommandConstants.ATOMICNT_COMMAND_ACTION;
    }

    public long getDelta() {
        return delta;
    }

    public void setDelta(long delta) {
        this.delta = delta;
    }

    public long getInitv() {
        return initv;
    }

    public void setInitv(long initv) {
        this.initv = initv;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

}
