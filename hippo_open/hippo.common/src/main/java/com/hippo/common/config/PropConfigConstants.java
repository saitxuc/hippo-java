package com.hippo.common.config;

/**
 * 
 * @author saitxuc
 * 2015-3-19
 */
public class PropConfigConstants {
	
	public static final String DEFAULT_BROKER_TYPE = "simple";
	
	public static final String MASTERSLAVE_BROKER_TYPE = "mscluster";
	
	public static final String CLUSTER_BROKER_TYPE = "ccluster";
	
	public static final String ZK_URL = "zk.url";
	
	public static final String CLUSTER_NAME = "cluster.name";
	
	public static final String BROKER_TYPE = "broker.type";
	
	public static final String BROKER_NAME = "broker.name";
	
	public static final String NIO_TYPE = "nio.type";
	
	public static final String BROKER_USEJMX = "broker.usejmx";
	
	public static final String JMX_CONNECTOR_HOST = "jmx.connector.host";
	
	public static final String BROKER_SERIALIZER = "broker.serializer";
	
	public static final String BROKER_COMMANDMANAGER_CLASS = "broker.commandmanager.class";
	
	public static final String BROKER_URIS = "broker.uris";
	
	public static final String BROKER_STORE = "broker.store";
	
	public static final String DB_LIMIT = "db.limit";
	
	public static final String DB_BUCKETS= "db.buckets";
	
	public static final String SERVICE_PORT = "cluster.service.port";
	
    public static final String REPLICATED_PORT = "cluster.replicated.port";
    
    public static final String STORE_EXPIRE_COUNT_LIMIT = "store.expire.count.limit";
    
    public static final String STORE_LRU_FATE = "store.lru.fate";
    
    public static final String STORE_DATA_SIZE_TYPE = "store.data.size.type";

	public static final String LEVELDB_MDB_USE_FLAG = "leveldb.mdb.use.flag";

	public static final String LEVELDB_MDB_USE_LIMIT = "leveldb.mdb.use.limit";

	public static final String STORE_BIT_SIZE_TYPE = "store.bit.size.per";

}
