package com.hippo.mdb;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author saitxuc
 * write 2014-7-28
 */
public class MdbConstants {

    public static final int CAPACITY_SIZE = 1 * 1024 * 1024;

    public static final double SIZE_FACTOR = 2;

    public static final int SIZE_1G = 1073741824;

    public static final int SIZE_LIMIT = 1024;

    public static final int SIZE_1K = 1024;

    public static final String DEFAULT_SERIALIZER_TYPE = "kryo";

    public static final long WAIT_TIME = 1000;

    public static final int HEADER_LENGTH_FOR_INT = 4;

    public static final int INCREASE_FACTOR = 2;

    public static final int LONG_BYTE_SIZE = 8;

    public static final int DEFAULT_BUCKET_NO = 0;
    
    public static final int DEFAULT_VERSION = 0;
    
    public static final byte DBINFO_KEEP_FLAG = 1;
    
    public static final byte DBINFO_CANCEL_FLAG = 0;

    public static final int MAX_HIPPO_OFFSET = 100000000;
}
