package com.hippo.common;


public class ZkConstants {
    /* ZK path layout:
        /hippo          /cluster     
                                     /cluster_1
                                     /cluster_2
                                                 /config (data: json(HippoClusterConifg); writer: console; reader: dataserver,client)     
                                                 /dataservers (subscriber: console)
                                                              /ServerInfo1 (label: ip:host; writer: dataserver)
                                                              /ServerInfo2
                                                 /tables
                                                              /ctable (data: json(HippoClusterTableInfo); writer: console; subscriber: client)
                                                              /mtable (data: json(HippoClusterTableInfo); writer: console; subscriber: dataserver)
                                                              /dtable (data: json(HippoClusterTableInfo); writer: console; subscriber: dataserver)
                                                 /migration (subscriber: console)
                                                              /mig_1 (data: json(HippoClusterMigrateInfo); writer: dataserver)
                                                              /mig_2
                        /console/lock (console's lock)
                                /result/master
       
       1. dataserver = broker
       2. update ctable only when all mig_x is complete
     */
    public static final String DEFAULT_PATH_ROOT = "/hippo/cluster";
    public static String PATH_ROOT = "/hippo/cluster/default0";
    public static String CONFIG = PATH_ROOT + "/config";
    public static String DATA_SERVERS = PATH_ROOT + "/dataservers";
    public static String TABLES = PATH_ROOT + "/tables";
    public static String MIGRATION = PATH_ROOT + "/migration";

    public static void initClusterName(String clusterName) {
        if (clusterName != null) {
            clusterName.replaceAll("/", "");
            if(clusterName.length() > 0) {
                ZkConstants.PATH_ROOT = DEFAULT_PATH_ROOT + "/" + clusterName;
                ZkConstants.CONFIG = ZkConstants.PATH_ROOT + "/config";
                ZkConstants.DATA_SERVERS = ZkConstants.PATH_ROOT + "/dataservers";
                ZkConstants.TABLES = ZkConstants.PATH_ROOT + "/tables";
                MIGRATION = PATH_ROOT + "/migration";
            }
        }
    }
    
    public static final String NODE_HIPPO = "/hippo";
    public static final String NODE_CLUSTER = "/cluster";
    public static final String NODE_CONFIG = "/config";
    public static final String NODE_DATA_SERVERS = "/dataservers";
    public static final String NODE_TABLES = "/tables";
    public static final String NODE_LOCK = "/lock";
    public static final String NODE_MIGRATION = "/migration";
    public static final String NODE_BUCKET_COUNT = "/bucketcount";
    public static final String NODE_COPY_COUNT = "/copycount";
    public static final String NODE_CTABLE = "/ctable";
    public static final String NODE_MTABLE = "/mtable";
    public static final String NODE_DTABLE = "/dtable";
}
