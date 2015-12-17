package com.hippo.broker.cluster.controltable;

import java.util.List;

import com.hippo.broker.cluster.simple.client.ISlaveReplicatedClient;
import com.hippo.common.domain.BucketInfo;

public abstract class ICtrlTableReplicatedClient extends ISlaveReplicatedClient {
    abstract public boolean resetBuckets(List<BucketInfo> resetBuckets);
}
