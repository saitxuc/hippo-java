package com.hippo.broker.cluster.controltable.client.leveldb;

import static com.hippo.broker.cluster.simple.master.leveldb.BackupServer.MASTER_PORT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hippo.broker.cluster.controltable.ICtrlTableReplicatedClient;
import com.hippo.broker.cluster.simple.client.leveldb.ClientProxy;
import com.hippo.broker.cluster.simple.client.leveldb.ClientProxy.SyncMasterDataException;
import com.hippo.broker.cluster.simple.client.leveldb.ClientProxy.VersionExpiredException;
import com.hippo.common.NamedThreadFactory;
import com.hippo.common.domain.BucketInfo;
import com.hippo.common.exception.HippoException;
import com.hippo.common.util.ListUtils;
import com.hippo.leveldb.LevelDbStoreEngine;
import com.hippo.leveldb.cluster.BConstansts;
import com.hippo.leveldb.cluster.Ddb.Position;
import com.hippo.network.Connection;
import com.hippo.network.ConnectionFactory;
import com.hippo.network.impl.TransportConnectionFactory;
import com.hippo.store.StoreEngine;

/**
 * @author yangxin
 */
public class LdbCtrlTableReplicatedClient extends ICtrlTableReplicatedClient {
	private final static Logger log = LoggerFactory.getLogger(LdbCtrlTableReplicatedClient.class);

	private String masterUrl;
	private int master_replicate_port = MASTER_PORT;
	private final LevelDbStoreEngine storeEngine;
	private volatile List<String> buckets;
	private String client;

	/**
	 * 避免重复建立拉取任务
	 */
	private BlockingQueue<String> replicatings = new LinkedBlockingQueue<String>();
	
	private Map<String, ClientProxy> cpMap = new HashMap<String, ClientProxy>();
	
	/**
	 * 定时拉取master数据任务future，主要用于任务的取消
	 */
	private Map<String, ScheduledFuture<?>> futures = new HashMap<String, ScheduledFuture<?>>();
	
	/**
	 * 暂存指向master的连接
	 */
	private List<Connection> conns = Lists.newArrayList();

	/**
	 * 数据拉取任务线程池,线程池大小为1
	 */
	private final ScheduledExecutorService executors = 
			Executors.newScheduledThreadPool(1, new NamedThreadFactory("slave", true));
	
	/**
	 * 心跳线程池
	 */
	private final ScheduledExecutorService heartBeatExecutor = 
			Executors.newScheduledThreadPool(1, new NamedThreadFactory("heartBeat", true));

	/**
	 * 
	 * @param storeEngine LevelDbStoreEngine
	 * @param masterUrl ip+port
	 * @param buckets
	 * @param client
	 */
	public LdbCtrlTableReplicatedClient(StoreEngine storeEngine, String masterUrl, List<String> buckets, String client) {
		Preconditions.checkNotNull(masterUrl, "masterUrl is null.");
		Preconditions.checkNotNull(buckets, "buckets is null.");
		Preconditions.checkNotNull(client, "client is null.");

		if (log.isInfoEnabled()) {
			log.info("masterUrl:{} client:{} buckets:{}.", masterUrl, client, buckets);
		}

		String[] arr = masterUrl.split(":");
		this.masterUrl = arr[0];
		if (arr.length > 1) {
			this.master_replicate_port = Integer.parseInt(arr[1]);
		}

		this.buckets = buckets;
		this.storeEngine = (LevelDbStoreEngine) storeEngine;
		this.client = client;
		
		List<BucketInfo> resetBuckets = new ArrayList<BucketInfo>();
		for (String b : buckets) {
			resetBuckets.add(new BucketInfo(Integer.parseInt(b), true));
		}
		resetBuckets(resetBuckets);
	}
	
	@Override
	public void doStart() {
		if (log.isInfoEnabled()) {
			log.info("start to replicate master data, buckets=" + buckets);
		}
		
		if (buckets == null) {
			// TODO
			replicate(masterUrl, String.valueOf(BConstansts.KEY_BUCKET));
		} else {
			for (String b : buckets) {
				replicate(masterUrl, b);
			}
		}

		final Map<String, ClientProxy> buckets = this.cpMap;
		heartBeatExecutor.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				// 重试初始化失败的bucket
				if (failBuckets.size() > 0) {
					log.warn("retry to pull master data! failBuckets=" + failBuckets.keySet());
					
					Iterator<Entry<String, Position>> fails = failBuckets.entrySet().iterator();
					while (fails.hasNext()) {
						Entry<String, Position> entry = fails.next();
						fails.remove();
						replicate(masterUrl, entry.getKey(), entry.getValue());
					}
				}
				
				//维持和master的心跳，显示当前数据同步的状态
				Iterator<Entry<String, ClientProxy>> it = buckets.entrySet().iterator();
				while (it.hasNext()) {
					Entry<String, ClientProxy> entry = it.next();
					ClientProxy clientProxy = entry.getValue();
					try {
						clientProxy.heartBeat();
					} catch (SyncMasterDataException e) {
						log.error("Heartbeat master error! Caused by: ", e);
					}
				}
			}
		}, 10, 10, TimeUnit.SECONDS);
	}

	@Override
	public void doStop() {
		resetBuckets(new ArrayList<BucketInfo>());
		heartBeatExecutor.shutdown();
		executors.shutdown();
	}

	private ConnectionFactory factory;
	
	/**
	 * 创建到master的连接
	 * @param master
	 * @return
	 * @throws HippoException master不存在
	 */
	private synchronized Connection newConnection(String master) throws HippoException {
		if (conns.size() > 0) {
			return conns.iterator().next();
		}

		if (factory == null) {
			factory = new TransportConnectionFactory(null, null, "recovery://" + master + ":" + master_replicate_port);
//			factory.setCommandManager(this);
		}

		Connection conn = factory.createConnection();
		conn.setClientID(client);
		conn.init();
		conn.start();
		conns.add(conn);

		return conn;
	}

	private final ConcurrentHashMap<String, Position> failBuckets = new ConcurrentHashMap<String, Position>();

	/**
	 * 向线程池提交同步数据任务，当所有的bucket初始化完成之后，执行定期拉取任务
	 * @param master
	 * @param bucket
	 * @param warner
	 */
	private synchronized void replicate(final String master, final String bucket, final Position pos,
			final ReplicationErrorWarner warner) {
		if (replicatings.contains(bucket)) {
			log.warn("{} is being recover!", bucket);
			return;
		} else {
			replicatings.offer(bucket);
		}

		executors.execute(new Runnable() {
			private int retryCount;//业务异常
			private ClientProxy clientProxy;

			@Override
			public void run() {
				if (clientProxy == null) {
					try {
						clientProxy = new ClientProxy(newConnection(master), storeEngine, client)
								.setBucket(Integer.parseInt(bucket))
								.setMaster(master)
								.curPosition(pos);
					} catch (Throwable e) {
						log.error("Create connection to master fail, Caused by: ", e);
						replicatings.remove(bucket);
						failBuckets.put(String.valueOf(bucket), pos);
						warner.warn(master, bucket);
						return;
					}
				}

				String msg = master + "-" + bucket;
				try {
					log.warn("Start pulling master data, {}.", msg);
					/*
					 * 由于seq不是全局唯一性
					 */
//					if (clientProxy.curPosition().maxSeq() < 0) {
//						// 获取本地存储引擎中最大的seq，拉取大于seq的数据
//						// 否则从头开始
//						long maxSeq = storeEngine.getMaxSeq(Integer.parseInt(bucket));
//						if (maxSeq > -1) {
//							clientProxy.curPosition().maxSeq(maxSeq);
//							clientProxy.curPosition().curMaxSeq(maxSeq);
//						}
//					}
					clientProxy.doBackup();// 循环拉取直到结束
					cpMap.put(bucket, clientProxy);
					maybeScheduleWithFixedDelay();
					
					log.warn("Finish pull data, {}.", msg);
				} catch (VersionExpiredException e) {
					if (++retryCount > 3) {
						// 大于3次的话让出当前线程
						log.warn("Re-pull " + msg + " over three times that pause pull. Caused by: ", e);
						replicatings.remove(bucket);
						failBuckets.put(String.valueOf(bucket), clientProxy.curPosition());
					} else {
						// 可重试异常
						clientProxy.curPosition().reset();
						log.warn("Reset " + msg + " data replication offset for version expire, Restart point is " 
								+ clientProxy.curPosition() + ", Caused by: ", e);
						executors.execute(this);
					}
				} catch (Throwable e) {
					//非version过期异常，都立即让出当前线程等待心跳线程去触发
					log.warn("Slave replicate data from master error! " + msg + ", Caused by: ", e);
					replicatings.remove(bucket);
					failBuckets.put(String.valueOf(bucket), clientProxy.curPosition());
					warner.warn(master, bucket);
				}
			}
		});

	}

	/**
	 * 当所有的bucket除过fail bucket
	 * 在master和slave达到同步点之后开始以间隔10s的频率去定期拉取master的数据
	 */
	private void maybeScheduleWithFixedDelay() {
		List<String> all = Lists.newArrayList(buckets);
		all.removeAll(failBuckets.keySet());
		if (all.size() != cpMap.size()) {
			// 让出资源，让没有初始化的bucket先执行
			return;
		}

		Iterator<Entry<String, ClientProxy>> it = cpMap.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, ClientProxy> e = it.next();
			final String bucket = e.getKey();
			final ClientProxy cp = e.getValue();
			futures.put(bucket, executors.scheduleWithFixedDelay(new Runnable() {

				@Override
				public void run() {
					try {
						cp.doBackup();
					} catch (VersionExpiredException e) {
						cp.curPosition().reset();
						log.warn("Reset data replication offset for version expire, Restart point is " + cp.curPosition());
					} catch (SyncMasterDataException e) {
						log.error("Replicate master data error", e);
					} catch (Throwable e) {
						log.error("Unknow error!", e);
					}
				}
			}, 0, 10, TimeUnit.SECONDS));
		}
	}


	/**
	 * 提交复制任务
	 * @param master ip
	 * @param bucket
	 */
	public void replicate(final String master, final String bucket) {
		replicate(master, bucket, null);
	}
	
	/**
	 * 提交复制任务
	 * @param master ip
	 * @param bucket
	 */
	public synchronized void replicate(final String master, final String bucket, final Position pos) {
		replicate(master, bucket, pos, new ReplicationErrorWarner() {

			@Override
			public void warn(String master, String bucket) {
			}
		});
	}


	/**
	 * 取消复制任务
	 * @param master ip
	 * @param bucket
	 */
	public synchronized void cancel(final String master, final String bucket) {
		cpMap.remove(bucket);
		replicatings.remove(bucket);
		
		Future<?> future = futures.get(bucket);
		if (future != null) {
			future.cancel(true);
			futures.remove(bucket);
		}

		// 没有指向master的复制任务，则关闭连接
		if (replicatings.size() == 0) {
			for (Connection c : conns) {
				try {
					c.close();
				} catch (Throwable e) {
					log.error("Close connection to master-" + master + " error, Casused by: ", e);
				}
			}
			conns.clear();
		}
	}

	private int count = 0;

	@Override
	public synchronized boolean resetBuckets(List<BucketInfo> resetBuckets) {
		count++;
		if (log.isDebugEnabled()) {
			log.debug(">>>>>>> " + this.hashCode() + "-" + count + " times >> Slave start reset buckets.");
		}
		List<String> newSlaveBuckets = new ArrayList<String>();
		if (resetBuckets != null) {
			List<BucketInfo> slaveBuckets = new ArrayList<BucketInfo>();
			for (BucketInfo info : resetBuckets) {
				if (info.isSlave()) {
					newSlaveBuckets.add(info.getBucketNo() + "");
					slaveBuckets.add(info);
				}
			}
			
			if (storeEngine != null) {
				storeEngine.setBuckets(slaveBuckets, this.hashCode());
			} else {
				log.warn("migrationEngine is null when reset buckets.");
			}
		} else {
			return true;
		}

		List<String> added = ListUtils.getDiffItemsFromSource(newSlaveBuckets, buckets);
		List<String> removed = ListUtils.getDiffItemsFromSource(buckets, newSlaveBuckets);
		
		if (log.isDebugEnabled()) {
			log.debug(">>>>>>> " + this.hashCode() + "-" + count + " times >> Slave reset remove-buckets:{} add-buckets:{}.",
					removed, added);
		}

		for (String bucket : removed) {
			failBuckets.remove(bucket);
		}
		
		buckets.removeAll(removed);
		for (String bucket : removed) {
			cancel(masterUrl, bucket);
		}

		buckets.addAll(added);
		for (String bucket : added) {
			replicate(masterUrl, bucket);
		}
		
		if (log.isDebugEnabled()) {
			log.debug(">>>>>>> " + this.hashCode() + "-" + count + " times >> Slave finish reset.");
		}

		return true;
	}

	public interface ReplicationErrorWarner {
		public void warn(final String master, final String bucket);
	}

	@Override
	public void doInit() {
	}
}
