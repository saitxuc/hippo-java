package com.pinganfu.hippo.client.listener;

import com.pinganfu.hippo.client.transport.cluster.ClusterConnectionControl;

public interface StartupListener {
	void startup(ClusterConnectionControl clusterControl);
}
