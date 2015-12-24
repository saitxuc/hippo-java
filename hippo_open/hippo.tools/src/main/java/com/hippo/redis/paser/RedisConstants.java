package com.hippo.redis.paser;

import java.util.HashMap;
import java.util.Map;

public class RedisConstants {
    public static final int REDIS_RDB_6BITLEN = 0;
    public static final int REDIS_RDB_14BITLEN = 1;
    public static final int REDIS_RDB_32BITLEN = 2;
    public static final int REDIS_RDB_ENCVAL = 3;

    public static final int REDIS_RDB_OPCODE_EXPIRETIME_MS = 252;
    public static final int REDIS_RDB_OPCODE_EXPIRETIME = 253;
    public static final int REDIS_RDB_OPCODE_SELECTDB = 254;
    public static final int REDIS_RDB_OPCODE_EOF = 255;

    public static final int REDIS_RDB_TYPE_STRING = 0;
    public static final int REDIS_RDB_TYPE_LIST = 1;
    public static final int REDIS_RDB_TYPE_SET = 2;
    public static final int REDIS_RDB_TYPE_ZSET = 3;
    public static final int REDIS_RDB_TYPE_HASH = 4;
    public static final int REDIS_RDB_TYPE_HASH_ZIPMAP = 9;
    public static final int REDIS_RDB_TYPE_LIST_ZIPLIST = 10;
    public static final int REDIS_RDB_TYPE_SET_INTSET = 11;
    public static final int REDIS_RDB_TYPE_ZSET_ZIPLIST = 12;
    public static final int REDIS_RDB_TYPE_HASH_ZIPLIST = 13;

    public static final int REDIS_RDB_ENC_INT8 = 0;
    public static final int REDIS_RDB_ENC_INT16 = 1;
    public static final int REDIS_RDB_ENC_INT32 = 2;
    public static final int REDIS_RDB_ENC_LZF = 3;

    public static final Map<Integer, String> DATA_TYPE_MAPPING = new HashMap<Integer, String>();
    
    public static final int BUFFER_SIZE = 30 * 1024 * 1024; 

    static {
        DATA_TYPE_MAPPING.put(0, "string");
        DATA_TYPE_MAPPING.put(1, "list");
        DATA_TYPE_MAPPING.put(2, "set");
        DATA_TYPE_MAPPING.put(3, "sortedset");
        DATA_TYPE_MAPPING.put(4, "hash");
        DATA_TYPE_MAPPING.put(9, "hash");
        DATA_TYPE_MAPPING.put(10, "list");
        DATA_TYPE_MAPPING.put(11, "set");
        DATA_TYPE_MAPPING.put(12, "sortedset");
        DATA_TYPE_MAPPING.put(13, "hash");
    }
}
