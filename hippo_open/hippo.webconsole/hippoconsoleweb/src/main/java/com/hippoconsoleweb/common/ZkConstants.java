package com.hippoconsoleweb.common;


public class ZkConstants {
    /* ZK path layout:
        /hippo     /console     /cluster     /c_1
                                                 /config      /bucketcount (data: int)
                                                              /copycount (data: int)
                                                 /dataservers
                                                              /ds_1 (data: json(ServerInfo))
                                                              /ds_2
                                                 /table (data: json(Map<Integer, Vector<String>>))
                                /lock
     */
    public static String PATH_ROOT = "/hippo/console/cluster/c_1";
    public static String DEFAULT_PATH_ROOT = "/hippo/console/cluster";
    public static String CONFIG = PATH_ROOT + "/config";
    public static String DATA_SERVERS = PATH_ROOT + "/dataservers";
    public static String TABLE = PATH_ROOT + "/table";
    public static String LOCK = PATH_ROOT + "/lock";
   public static String DS_CHANGE_TYPE = "/hippo/console/changetype";

    public static void initRoot(String zkRoot) {
        if (zkRoot != null && zkRoot.indexOf('/') == 0 && zkRoot.length() > 1) {
            ZkConstants.PATH_ROOT = zkRoot;
            ZkConstants.CONFIG = ZkConstants.PATH_ROOT + "/config";
            ZkConstants.DATA_SERVERS = ZkConstants.PATH_ROOT + "/dataservers";
            ZkConstants.TABLE = ZkConstants.PATH_ROOT + "/table";
        }
    }
    
    public static final String NODE_BUCKET_COUNT = "/bucketcount";
    public static final String NODE_COPY_COUNT = "/copycount";
}
