package com.pinganfu.hippoconsoleweb.lisneter;

import org.I0Itec.zkclient.IZkStateListener;

/**
 * 
 * @author saitxuc
 * 2015-3-27
 */
public interface StateListener extends IZkStateListener {
    
    int DISCONNECTED = 0;

    int CONNECTED = 1;

    int RECONNECTED = 2;

    void stateChanged(int connected);

}