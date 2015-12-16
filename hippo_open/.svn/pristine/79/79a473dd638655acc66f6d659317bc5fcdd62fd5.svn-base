package com.pinganfu.hippo.broker.cluster.simple.master.leveldb;

import static com.pinganfu.hippo.broker.cluster.simple.client.leveldb.ClientProxy.EnvType.HEART_BEAT;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.broker.cluster.ReplicationNioConnectionManager;
import com.pinganfu.hippo.broker.cluster.simple.client.leveldb.ClientProxy.ReplicationRequest;
import com.pinganfu.hippo.broker.cluster.simple.master.IMasterReplicatedServer;
import com.pinganfu.hippo.leveldb.LevelDbMigrationEngine;
import com.pinganfu.hippo.leveldb.cluster.Ddb;
import com.pinganfu.hippo.leveldb.cluster.Ddb.IfileNotFoundException;
import com.pinganfu.hippo.leveldb.cluster.Ddb.Position;
import com.pinganfu.hippo.network.CommandManager;
import com.pinganfu.hippo.network.CommandResult;
import com.pinganfu.hippo.network.ServerFortress;
import com.pinganfu.hippo.network.ServerFortressFactory;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.impl.TransportServerFortressFactory;
import com.pinganfu.hippo.store.MigrationEngine;

/**
 * @author yangxin
 */
public class BackupServer extends IMasterReplicatedServer implements CommandManager {
	protected static final Logger log = LoggerFactory.getLogger(BackupServer.class);
	public final static int MASTER_PORT = 61100;
	private int master_port;
	
	private ServerFortress server;
	private LevelDbMigrationEngine migrationEngine;
	private final ReplicationNioConnectionManager channelManager;
	
	public BackupServer(LevelDbMigrationEngine migrationEngine) {
		this(migrationEngine, MASTER_PORT + "");
	}
	
	public BackupServer(LevelDbMigrationEngine migrationEngine, String replicatedPort) {
		this.migrationEngine = migrationEngine;
		try {
			this.master_port = Integer.parseInt(replicatedPort);
		} catch (NumberFormatException e) {
			this.master_port = MASTER_PORT;
		}
		this.channelManager = new ReplicationNioConnectionManager();
	}

	@Override
	public void doInit() {
		try {
			ServerFortressFactory sfactory = new TransportServerFortressFactory();
			server = sfactory.createServer(master_port, TransportServerFortressFactory.DEFAULT_SHEME, this, channelManager);
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
		try {
			server.stop();
		} catch (Exception e) {
			log.error("Master stop failed!", e);
		}
	}
	
	/**
	 * 接收slave命令，
	 * 包括：注册、取消注册、拉去数据
	 */
	@Override
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
					result.setMessage(migrationEngine.getMaxSeq(Ddb.GLOBAL_BUCKET));
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

	private String getStandby(ReplicationRequest request) {
		return request.getClientId();
	}
	
	public static interface IResponseCode {
		public final static String succ = "0";
		public final static String maxConbackup = "1";
		public final static String fileNotFound = "2";
		public final static String unknow = "3";
		public final static String unregister = "4";
		public final static String uninited = "5";
	}


	@Override
	public void setMigerateEngine(MigrationEngine migrationEngine) {
		if (migrationEngine instanceof LevelDbMigrationEngine) {
			migrationEngine.init();
			migrationEngine.start();
			this.migrationEngine = (LevelDbMigrationEngine) migrationEngine;
		}
	}
	
}