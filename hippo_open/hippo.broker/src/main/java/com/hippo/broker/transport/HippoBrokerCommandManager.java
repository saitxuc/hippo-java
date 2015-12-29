package com.hippo.broker.transport;

import com.hippo.broker.Broker;
import com.hippo.broker.transport.command.handle.*;
import com.hippo.network.BaseCommandManager;
import com.hippo.network.command.CommandConstants;

/**
 * @author saitxuc
 */
public class HippoBrokerCommandManager extends BaseCommandManager<String> {

    private Broker brokerService;

    public HippoBrokerCommandManager() {
        super();
    }

    public HippoBrokerCommandManager(Broker brokerService) {
        this.brokerService = brokerService;
        this.init();
    }

    @Override
    public void initConmandHandler() {
        addCommandHandler(CommandConstants.SET_COMMAND_ACTION, new SetCommandHandle(brokerService));
        addCommandHandler(CommandConstants.UPDATE_COMMAND_ACTION, new UpdateCommandHandle(brokerService));
        addCommandHandler(CommandConstants.GET_COMMAND_ACTION, new GetCommandHandle(brokerService));
        addCommandHandler(CommandConstants.REMOVE_COMMAND_ACTION, new RemoveCommandHandle(brokerService));
        addCommandHandler(CommandConstants.ATOMICNT_COMMAND_ACTION, new AtomicntCommandHandle(brokerService));
        addCommandHandler(CommandConstants.BITGET_COMMAND_ACTION, new BitGetCommandHandle(brokerService));
        addCommandHandler(CommandConstants.BITSET_COMMAND_ACTION, new BitSetCommandHandle(brokerService));
    }

    public void setBrokerService(Broker brokerService) {
        this.brokerService = brokerService;
    }

}
