package com.hippo.broker.cluster.controltable;

import java.util.List;

import com.hippo.broker.cluster.simple.master.IMasterReplicatedServer;
import com.hippo.common.domain.BucketInfo;

public abstract class ICtrlTableReplicatedServer extends IMasterReplicatedServer {
    abstract public boolean resetBuckets(List<BucketInfo> resetBuckets, boolean clearTriggerReplicatedEvent);
}
