package com.hippo.client.listener;

import com.hippo.client.transport.cluster.ClusterConnectionControl;

public interface StartupListener {
	void startup(ClusterConnectionControl clusterControl);
}
