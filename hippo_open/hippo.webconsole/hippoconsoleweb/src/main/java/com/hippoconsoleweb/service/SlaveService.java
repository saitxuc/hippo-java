package com.hippoconsoleweb.service;

import org.I0Itec.zkclient.IZkDataListener;

import com.hippo.common.lifecycle.LifeCycleSupport;
import com.hippoconsoleweb.lisneter.PrevLockListener;
import com.hippoconsoleweb.zk.ZkRegisterService;

/**
 * 
 * @author DPJ
 *
 */
public class SlaveService extends LifeCycleSupport {

    private ZkRegisterService registerService;

    private IZkDataListener prevLockListener;

    // DPJ
    /// private StateListener stateListener;

    public SlaveService(ZkRegisterService registerService) {
        this.registerService = registerService;
    }

    @Override
    public void doInit() {
        this.prevLockListener = new PrevLockListener(registerService);
        ///this.stateListener = new StateListenerImpl(registerService);
        subscribe();
    }

    @Override
    public void doStart() {
    }

    @Override
    public void doStop() {
        unsubscribe();
    }

    public void reconnectMaster() {

    }

    private void subscribe() {
        if (registerService.exists(registerService.getClusterLockPath())) {
            registerService.subscribeDataChanges(registerService.getClusterLockPath(), this.prevLockListener);
        }
        ///registerService.subscribeStateChanges(this.stateListener);

    }

    private void unsubscribe() {
        ///if (registerService.exists(registerService.getClusterLockPath())) {
            registerService.unsubscribeData(registerService.getClusterLockPath(), this.prevLockListener);
        ///}
        ///registerService.unsubscribeStateChanges(this.stateListener);
    }

}
