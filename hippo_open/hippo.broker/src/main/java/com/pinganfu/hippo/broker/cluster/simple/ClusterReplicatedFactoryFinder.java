package com.pinganfu.hippo.broker.cluster.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import com.pinganfu.hippo.network.exception.TransportException;

public class ClusterReplicatedFactoryFinder {
    private static Map<String, MSClusterReplicatedFactory> clusterReplicatedMap = new HashMap<String, MSClusterReplicatedFactory>();

    static {
        ServiceLoader<MSClusterReplicatedFactory> factorys = ServiceLoader.load(MSClusterReplicatedFactory.class);
        for (MSClusterReplicatedFactory factory : factorys) {
            clusterReplicatedMap.put(factory.getName(), factory);
        }
    }

    public static MSClusterReplicatedFactory getClusterReplicatedFactory(String modelName) {
        MSClusterReplicatedFactory factory = clusterReplicatedMap.get(modelName);
        if (factory == null) {
            throw new TransportException("can't find the impl class of type[" + modelName + "]");
        }
        return factory;
    }
}
