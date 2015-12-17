package com.hippo.broker.cluster.simple.client;

import org.I0Itec.zkclient.IZkDataListener;

import com.hippo.broker.cluster.simple.MSClusterReplicatedFactory;
import com.hippo.broker.cluster.simple.ClusterReplicatedFactoryFinder;
import com.hippo.broker.cluster.simple.ZkRegisterService;
import com.hippo.broker.cluster.zk.PrevLockListener;
import com.hippo.broker.cluster.zk.StateListener;
import com.hippo.broker.cluster.zk.StateListenerImpl;
import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippo.store.MigrationEngine;

/**
 * 
 * @author saitxuc
 *
 */
public class SlaveService extends LifeCycleSupport {

    private ZkRegisterService registerService;

    private IZkDataListener prevLockListener;

    private StateListener stateListener;

    private ISlaveReplicatedClient slaveReplicatedClient;

    public SlaveService(MigrationEngine migrationEngine, ZkRegisterService registerService, String replicatedPort) {
        MSClusterReplicatedFactory factory = ClusterReplicatedFactoryFinder.getClusterReplicatedFactory(migrationEngine.getName());
        this.registerService = registerService;
        slaveReplicatedClient = factory.getReplicatedClient(migrationEngine, registerService, replicatedPort);
    }

    @Override
    public void doInit() {
        this.prevLockListener = new PrevLockListener(registerService);
        this.stateListener = new StateListenerImpl(registerService);
        subscribe();
        slaveReplicatedClient.init();
    }

    @Override
    public void doStart() {
        slaveReplicatedClient.start();
    }

    @Override
    public void doStop() {
        //stop transport
        unsubscribe();
        slaveReplicatedClient.stop();
    }

    public void reconnectMaster() {

    }

    private void subscribe() {
        if (registerService.exists(registerService.getClusterLockPath())) {
            registerService.subscribeDataChanges(registerService.getClusterLockPath(), this.prevLockListener);
        }
        registerService.subscribeStateChanges(this.stateListener);

    }

    private void unsubscribe() {
        if (registerService.exists(registerService.getClusterLockPath())) {
            registerService.unsubscribeData(registerService.getClusterLockPath(), this.prevLockListener);
        }
        registerService.unsubscribeStateChanges(this.stateListener);
    }

}
