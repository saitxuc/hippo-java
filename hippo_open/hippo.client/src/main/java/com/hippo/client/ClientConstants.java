package com.hippo.client;

public class ClientConstants {

	public final static int MAX_POOL_SIZE = 20;
	
	public final static int MIN_POOL_SIZE = 1;
	
	public static final int SESSION_POOL_TIMEOUT = 5;
	
	public static final int CLIENT_INIT_SESSIONPOOL_SIZE = 5;
	
	public static final int CLIENT_MAX_SESSIONPOOL_SIZE = 50;

	public static final String TRANSPORT_PROTOCOL_HIPPO = "hippo";
	
	public static final String TRANSPORT_PROTOCOL_FAILOVER = "failover";

	public static final String TRANSPORT_PROTOCOL_CLUSTER = "cluster";
	
	public static final String TRANSPORT_PROTOCOL_SIMPLE = "simple";
	
	public static final String HEAD_BUCKET_NO = "head_bucket_no";
	
	public static final String HEAD_VERSION = "head_version";
	
	//public static final int FAILOVER_CHECK_SCHEDULE_INIT_TIME = 1 * 1000;
	
	//public static final int FAILOVER_CHECK_SCHEDULE_INTERVAL_TIME = 10 * 1000;
}
