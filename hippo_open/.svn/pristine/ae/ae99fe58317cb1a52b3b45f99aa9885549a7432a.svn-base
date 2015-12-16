package com.pinganfu.hippo.broker.cluster.controltable;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import com.pinganfu.hippo.network.exception.TransportException;

public class CtrlTableClusterReplicatedFactoryFinder {
    private static Map<String, ICtrlTableClusterReplicatedFactory> clusterReplicatedMap = new HashMap<String, ICtrlTableClusterReplicatedFactory>();

    static {
        ServiceLoader<ICtrlTableClusterReplicatedFactory> factorys = ServiceLoader.load(ICtrlTableClusterReplicatedFactory.class);
        for (ICtrlTableClusterReplicatedFactory factory : factorys) {
            clusterReplicatedMap.put(factory.getName(), factory);
        }
    }

    public static ICtrlTableClusterReplicatedFactory getClusterReplicatedFactory(String modelName) {
        ICtrlTableClusterReplicatedFactory factory = clusterReplicatedMap.get(modelName+"-cluster");
        if (factory == null) {
            throw new TransportException("can't find the impl class of type[" + modelName + "]");
        }
        return factory;
    }
}
