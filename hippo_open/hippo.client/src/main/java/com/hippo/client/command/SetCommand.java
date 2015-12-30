package com.hippo.client.command;

import com.hippo.network.command.Command;
import com.hippo.network.command.CommandConstants;

/**
 * 
 * @author saitxuc
 * write 2014-6-30  
 */
public class SetCommand extends Command {
    
    private static final long serialVersionUID = 1L;

    private int expire;
    
    private int klength;
    
    public SetCommand() {
        super();
    }
    
    @Override
    public String getAction() {
        return CommandConstants.SET_COMMAND_ACTION;
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
