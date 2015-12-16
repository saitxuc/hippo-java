package com.pinganfu.hippo.broker.cluster.simple.client.leveldb;

import static com.pinganfu.hippo.broker.cluster.simple.master.leveldb.BackupServer.IResponseCode.fileNotFound;
import static com.pinganfu.hippo.broker.cluster.simple.master.leveldb.BackupServer.IResponseCode.uninited;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.broker.cluster.ReplicatedConstants;
import com.pinganfu.hippo.common.exception.HippoException;
import com.pinganfu.hippo.leveldb.LevelDbStoreEngine;
import com.pinganfu.hippo.leveldb.LevelDbStoreEngine.SyncResult;
import com.pinganfu.hippo.leveldb.cluster.Ddb.Position;
import com.pinganfu.hippo.network.Connection;
import com.pinganfu.hippo.network.command.Command;
import com.pinganfu.hippo.network.command.Response;

/**
 * @author yangxin
 */
public class ClientProxy {
	protected static final Logger log = LoggerFactory.getLogger(ClientProxy.class);

	static private final int replication_batch_size = 32768;// 32k
	static private final int RMI_TIMEOUT = 30000;// 30s 
	
	private final Connection ioSession;
	private final LevelDbStoreEngine storeEngine;
	private int bucket;
	private String clientFlag;
	private String master;
	private volatile Position pos = new Position();

	public ClientProxy(Connection connection, LevelDbStoreEngine storeEngine) throws HippoException {
		this(connection, storeEngine, null);
	}

	public ClientProxy(Connection connection, LevelDbStoreEngine storeEngine, String clientFlag) throws HippoException {
		this.ioSession = connection;
		this.storeEngine = storeEngine;
		if (clientFlag == null) {
			this.clientFlag = ioSession.getClientID();
		} else {
			this.clientFlag = clientFlag;
		}
	}

	public Position curPosition() {
		return pos;
	}
	
	public ClientProxy curPosition(Position pos) {
		if (pos != null) {
			this.pos = pos;
		}
		return this;
	}

	public ClientProxy setBucket(int bucket) {
		this.bucket = bucket;
		return this;
	}
	
	/**
	 * @param master the master to set
	 */
	public ClientProxy setMaster(String master) {
		this.master = master;
		return this;
	}



	public enum EnvType {
		START, END, HEART_BEAT, SYNC
	}

	/**
	 * master和slave之间的同步状态
	 */
	public void heartBeat() {
		ReplicationRequest command = new ReplicationRequest(true);
		command.setBuckNo(bucket);
		command.setEnvType(EnvType.HEART_BEAT);
		packageRequestHeader(command);

		try {
			Response rsp = (Response) ioSession.syncSendPacket(command, RMI_TIMEOUT);
			if (!rsp.isFailure()) {
				Long maxSeq = (Long) rsp.getContent();
				if (log.isInfoEnabled()) {
					log.info("{}-bucket master-slave max sequence is [{}, {}].", bucket, maxSeq, pos.curMaxSeq());	
				}
			} else {
				throw new SyncMasterDataException(rsp.getErrorCode());
			}
		} catch (HippoException e) {
			throw new SyncMasterDataException("Rmi error! " + master + "-" + bucket, e);
		}
	}

	/**
	 * 未知异常，比如网络、超时等异常
	 */
	public class SyncMasterDataException extends RuntimeException {
		// inner use
		private static final long serialVersionUID = 1L;

		public SyncMasterDataException(String message) {
			super(message);
		}

		public SyncMasterDataException(String message, Exception cause) {
			super(message, cause);
		}
	}

	/**
	 * 版本过期异常</br>
	 * 可以由于长时间没有拉去，或者拉取参数指定的偏移量不存在导致.
	 */
	public class VersionExpiredException extends RuntimeException {
		// inner use
		private static final long serialVersionUID = 1L;

		public VersionExpiredException(String message) {
			super(message);
		}

		public VersionExpiredException(String message, Exception cause) {
			super(message, cause);
		}
	}

	private void packageRequestHeader(ReplicationRequest command) {
		command.setAction(ReplicatedConstants.BUCKET_DATA_REQUEST_ACTION);
		command.setClientId(clientFlag);
	}

	/**
	 * 以pull的方式拉取master数据
	 */
	public void doBackup() {
		for (;;) {
			pos = doBackup(pos);
			if (!pos.hasNext()) {
				pos.hasNext(true);
				break;
			}
		}
	}

	private Position doBackup(Position pos) {
		ReplicationRequest command = new ReplicationRequest();
		command.setPos(pos);
		command.setBuckNo(bucket);
		command.setSize(replication_batch_size);
		packageRequestHeader(command);
		
		Position next = null;
		try {
			Response rsp = (Response) ioSession.syncSendPacket(command, RMI_TIMEOUT);
			if (!rsp.isFailure()) {
				next = (Position) rsp.getContent();
				
				byte[] d = rsp.getData();
				if (d != null) {
					SyncResult r = storeEngine.set(d, bucket);
					if (!r.isSuccess()) {
						//回滚
						pos.pointer(next.pointer());
						throw new SyncMasterDataException("save migration data error.");
					}
				}
				//提交,默认情况下服务端会维护offset，不需要客户端设置
				next.pointer(-1);
				return next;
			} else {
				String ec = rsp.getErrorCode();
				if (fileNotFound.equals(ec)) {
					throw new VersionExpiredException("version expire");
				} else if (uninited.equals(ec)) {
					throw new SyncMasterDataException("migrationEngine uninited");
				} else {
					throw new SyncMasterDataException("unknow error = " + ec);
				}
			}
		} catch (HippoException e) {
			throw new SyncMasterDataException("Rmi error! " + master + "-" + bucket, e);
		}
	}

	static public class ReplicationRequest extends Command {
		private static final long serialVersionUID = -2307890932970768227L;
		private int buckNo;
		private EnvType envType;
		private String clientId;
		private int size;
		private boolean env = false;
		private Position pos;

		public ReplicationRequest() {
		}

		public ReplicationRequest(boolean env) {
			this.env = env;
		}

		public int getBuckNo() {
			return buckNo;
		}

		public void setBuckNo(int buckNo) {
			this.buckNo = buckNo;
		}

		public EnvType getEnvType() {
			return envType;
		}

		public void setEnvType(EnvType envType) {
			this.envType = envType;
		}

		public String getClientId() {
			return clientId;
		}

		public void setClientId(String clientId) {
			this.clientId = clientId;
		}

		public int getSize() {
			return size;
		}

		public void setSize(int size) {
			this.size = size;
		}

		public Position getPos() {
			return pos;
		}

		public void setPos(Position pos) {
			this.pos = pos;
		}

		public boolean isEnv() {
			return env;
		}

		public void setEnv(boolean env) {
			this.env = env;
		}

	}
	
}
