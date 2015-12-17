package com.hippo.client.util;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author sait.xuc Date: 13/10/23 Time: 15:31
 * 
 */
public class ZkUtil {

	private static final Logger logger = LoggerFactory.getLogger(ZkUtil.class);

	protected static final int zkSessionTimeoutMs = 30000;

	private static ConcurrentMap<String, ZkClient> zkClientMap = new ConcurrentHashMap<String, ZkClient>();

	public static ConcurrentMap<String, ZkClient> getZKClientMap() {
		return zkClientMap;
	}

	public static ZkClient getZKClient(String zkAddress) {
		ZkClient zkClient = zkClientMap.get(zkAddress);
		if (zkClient == null) {
			ZKConfig zkConfig = new ZKConfig(zkAddress);
			try {
				zkClient = new ZkClient(zkConfig.zkConnect,
						zkConfig.zkSessionTimeoutMs,
						zkConfig.zkConnectionTimeoutMs,
						new ZkUtil.StringSerializer());
			} catch (ZkTimeoutException e) {
				throw new ZkTimeoutException("zookeeper address["
						+ zkConfig.zkConnect + "] connect timeout", e);
			}
			zkClientMap.put(zkAddress, zkClient);
		}
		return zkClient;
	}

	public static boolean exist(final String zkAddress, final String path) {
		return getZKClient(zkAddress).exists(path);
	}

	public static List<String> getChildren(final String zkAddress,
			final String path) {
		return getZKClient(zkAddress).getChildren(path);
	}

	public static String getData(final String zkAddress, final String path) {
		return getZKClient(zkAddress).readData(path);
	}
	
    public static void createEphemeralPath(final ZkClient client, final String path, final String data)
            throws Exception {
        try {
            client.createEphemeral(path, data);
        }
        catch (final ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }
	
    public static void createParentPath(final ZkClient client, final String path) throws Exception {
        final String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            client.createPersistent(parentDir, true);
        }
    }
    
	public static class StringSerializer implements ZkSerializer {

		@Override
		public Object deserialize(final byte[] bytes) throws ZkMarshallingError {
			try {
				return new String(bytes, "utf-8");
			} catch (final UnsupportedEncodingException e) {
				throw new ZkMarshallingError(e);
			}
		}

		@Override
		public byte[] serialize(final Object data) throws ZkMarshallingError {
			try {
				return ((String) data).getBytes("utf-8");
			} catch (final UnsupportedEncodingException e) {
				throw new ZkMarshallingError(e);
			}
		}

	}

	public static class ZKConfig implements Serializable {
		static final long serialVersionUID = -1L;

		public static String zkRoot = "/hippo";
		/**
		 * If enable zookeeper
		 */
		public boolean zkEnable = true;

		/** ZK host string */
		public String zkConnect;

		/** zookeeper session timeout */
		public int zkSessionTimeoutMs = 30000;

		/**
		 * the max time that the client waits to establish a connection to
		 * zookeeper
		 */
		public int zkConnectionTimeoutMs = 30000;

		/** how far a ZK follower can be behind a ZK leader */
		public int zkSyncTimeMs = 5000;

		public ZKConfig(final String zkConnect) {
			super();
			this.zkConnect = zkConnect;
		}

		public ZKConfig(final String zkConnect, final int zkSessionTimeoutMs,
				final int zkConnectionTimeoutMs, final int zkSyncTimeMs) {
			super();
			this.zkConnect = zkConnect;
			this.zkSessionTimeoutMs = zkSessionTimeoutMs;
			this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
			this.zkSyncTimeMs = zkSyncTimeMs;
		}

		public ZKConfig() {
			super();
		}

		public ZKConfig(final String zkRoot, final String zkConnect,
				final int zkSessionTimeoutMs, final int zkConnectionTimeoutMs,
				final int zkSyncTimeMs, final boolean zkEnable) {
			super();
			this.zkRoot = zkRoot;
			this.zkConnect = zkConnect;
			this.zkSessionTimeoutMs = zkSessionTimeoutMs;
			this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
			this.zkSyncTimeMs = zkSyncTimeMs;
			this.zkEnable = zkEnable;
		}
	}

}
