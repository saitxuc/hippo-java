package com.hippo.broker.cluster.controltable.master.leveldb;

import static com.hippo.broker.cluster.simple.client.leveldb.ClientProxy.EnvType.HEART_BEAT;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.broker.cluster.ReplicationNioConnectionManager;
import com.hippo.broker.cluster.controltable.CtrlTableClusterBrokerService;
import com.hippo.broker.cluster.controltable.ICtrlTableReplicatedServer;
import com.hippo.broker.cluster.simple.client.leveldb.ClientProxy.ReplicationRequest;
import com.hippo.broker.cluster.simple.master.leveldb.BackupServer.IResponseCode;
import com.hippo.common.domain.BucketInfo;
import com.hippo.leveldb.LevelDbMigrationEngine;
import com.hippo.leveldb.LevelDbStoreEngine.UserType;
import com.hippo.leveldb.cluster.Ddb.IfileNotFoundException;
import com.hippo.leveldb.cluster.Ddb.Position;
import com.hippo.network.CommandResult;
import com.hippo.network.ServerFortress;
import com.hippo.network.ServerFortressFactory;
import com.hippo.network.command.Command;
import com.hippo.network.impl.TransportServerFortressFactory;
import com.hippo.store.MigrationEngine;

/**
 * @author yangxin
 */
public class LdbCtrlTableReplicatedServer extends ICtrlTableReplicatedServer {
	protected static final Logger log = LoggerFactory.getLogger(LdbCtrlTableReplicatedServer.class);

	static public final int MASTER_PORT = 61100;
	private int master_port = MASTER_PORT;
	private CtrlTableClusterBrokerService broker;
	private ServerFortress server;
	private LevelDbMigrationEngine migrationEngine;
	private final ReplicationNioConnectionManager channelManager;

	private ConcurrentHashMap<String, Object> triggerReplicatedEvent = new ConcurrentHashMap<String, Object>();

	public LdbCtrlTableReplicatedServer(String replicated_port, CtrlTableClusterBrokerService broker) {
		this(null, replicated_port, broker);
	}

	public LdbCtrlTableReplicatedServer(LevelDbMigrationEngine migrationEngine, String replicated_port,
			CtrlTableClusterBrokerService broker) {
		this.migrationEngine = migrationEngine;
		this.channelManager = new ReplicationNioConnectionManager();
		this.broker = broker;
		if (StringUtils.isNotEmpty(replicated_port)) {
			this.master_port = Integer.parseInt(replicated_port);
		}
	}

	@Override
	public void doInit() {
		try {
			ServerFortressFactory sfactory = new TransportServerFortressFactory();
			server = sfactory.createServer(master_port, TransportServerFortressFactory.DEFAULT_SHEME,
					new LdbCtrlTableReplicatedCommandManager(this), channelManager);
		} catch (Exception e) {
			log.error("Master start failed!", e);
		}
	}

	@Override
	public void doStart() {
		try {
			server.start();
		} catch (Exception e) {
			log.error("Master start failed!", e);
		}
	}

	@Override
	public void doStop() {
		// 清除bucket
		resetBuckets(new ArrayList<BucketInfo>(), true);
		
		try {
			server.stop();
		} catch (Exception e) {
			log.error("Master stop failed!", e);
		}
	}

	/**
	 * 接收slave命令，
	 * 包括：心跳、拉去数据
	 */
	public CommandResult handleCommand(Command command) {
		CommandResult result = new CommandResult();

		ReplicationRequest request = (ReplicationRequest) command;
		int bucket = request.getBuckNo();
		String standby = getStandby(request);
		try {
			if (migrationEngine == null || !migrationEngine.isStarted()) {
				result.setErrorCode(IResponseCode.uninited);
				result.setSuccess(false);
				return result;
			}
			
			if (request.isEnv()) {
				// heart beat
				if (HEART_BEAT.equals(request.getEnvType())) {
					result.setMessage(migrationEngine.getMaxSeq(bucket));
					result.setSuccess(true);
					return result;
				} else {
					log.error("invalid env! env=" + request.getEnvType());
				}
			} // pull data
			else {
				Position pos = request.getPos();
				int size = request.getSize();
				result.setData(migrationEngine.migration(standby, bucket, pos, size));
				result.setMessage(pos);
				
				if (!pos.hasNext()) {// master和slave已经达到临时同步状态
					notify(bucket, standby);
				}
			}
			result.setSuccess(true);
		} catch (IfileNotFoundException e) {
			log.error("Probably due to the long time to pull, Caused by:", e);
			result.setErrorCode(IResponseCode.fileNotFound);
			result.setSuccess(false);
		} catch (Throwable e) {
			log.error("Invoke MigrationEngine'method error, Caused by:", e);
			result.setSuccess(false);
			result.setErrorCode(IResponseCode.unknow);
		}

		return result;
	}

	private void notify(int bucket, String standby) {
		String identify = bucket + "-" + standby;
		if (!triggerReplicatedEvent.contains(identify)) {
			Object oldValue = triggerReplicatedEvent.putIfAbsent(identify, new Object());
			if (oldValue == null) {
				 broker.whenMigrateBucketFinishedCallback(bucket, standby);
			}
		}
	}

	private String getStandby(ReplicationRequest request) {
		return request.getClientId();
	}

	public void setMigerateEngine(MigrationEngine migrationEngine) {
		if (migrationEngine instanceof LevelDbMigrationEngine) {
		    /*migrationEngine.init();
            migrationEngine.start();*/
			this.migrationEngine = (LevelDbMigrationEngine) migrationEngine;
		}
	}
	
	private int count = 0;
	@Override
	public boolean resetBuckets(List<BucketInfo> resetBuckets, boolean clearTriggerReplicatedEvent) {
		count++;
		if (log.isDebugEnabled()) {
			log.debug("+++++++ " + this.hashCode() + "-" + count + " times ++ Master start reset buckets.");
		}
		
		if (resetBuckets == null) {
			return true;
		} else if (clearTriggerReplicatedEvent) {
			triggerReplicatedEvent.clear();
			if (log.isDebugEnabled()) {
				log.debug("clearTriggerReplicatedEvent: " + clearTriggerReplicatedEvent);
			}
		} else {
//			List<BucketInfo> masterBuckets = new ArrayList<BucketInfo>();
//			for (BucketInfo info : resetBuckets) {
//				// 不关心是否发生翻转，即使发生翻转，对引擎来说是无状态的
//				// 对应由msater翻转为slave，则由slave去维护该桶是否需要被清除
//				if (!info.isSlave()) {
//					masterBuckets.add(info);
//				}
//			}
			
			if (migrationEngine != null) {
				migrationEngine.getStoreEngine().setBuckets(resetBuckets, UserType.MASTER);
			} else {
				log.warn("migrationEngine is null when reset buckets.");
			}
		}
		
		if (log.isDebugEnabled()) {
			log.debug("+++++++ " + this.hashCode() + "-" + count + " times ++ Master finish reset.");
		}
		return true;
	}
}