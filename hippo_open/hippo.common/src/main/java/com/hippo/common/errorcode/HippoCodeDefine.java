package com.hippo.common.errorcode;

/**
 * @author saitxuc
 *         2015-4-3
 */
public class HippoCodeDefine {

    /**
     * basic error code
     */
    public static final String HIPPO_SUCCESS = "000";

    public static final String HIPPO_FAILURE = "010";

    public static final String HIPPO_TIMEOUT = "011";

    public static final String HIPPO_WRITE_FAILURE = "012";

    public static final String HIPPO_READ_FAILURE = "013";

    public static final String HIPPO_AUTH_FAILURE = "014";

    public static final String HIPPO_HOST_LOOKUP_FAILURE = "015";

    public static final String HIPPO_NO_SERVERS = "016";

    public static final String HIPPO_PROTOCOL_ERROR = "017";

    public static final String HIPPO_SERVER_ERROR = "018";

    /**
     * hippo data error code, begin with 1
     */
    public static final String HIPPO_UNKNOWN_READ_FAILURE = "101";

    public static final String HIPPO_DATA_DOES_NOT_EXIST = "102";

    public static final String HIPPO_DATA_EXISTS = "103";

    public static final String HIPPO_SIZE_NOT_EXISTED = "104";

    public static final String HIPPO_BUCKET_NOT_EXISTED = "105";

    public static final String HIPPO_BUCKET_OUT_MEMORY = "106";

    public static final String HIPPO_PARAM_NOT_RIGHT = "107";

    public static final String HIPPO_OPERATION_VERSION_WRONG = "108";

    public static final String HIPPO_DATA_EXPIRED = "109";

    public static final String HIPPO_ATOMIC_OPER_ERROR = "110";

    public static final String HIPPO_LOCAL_SERIAL_ERROR = "111";

    public static final String HIPPO_DATA_OUT_RANGE = "112";

    public static final String HIPPO_DATA_OUT_DATE = "113";



    /**
     * levelDb data error code, begin with 2
     * */
    public static final String HIPPO_UNKNOW_ERROR = "201";

    public static final String HIPPO_SYS_ERROR = "202";

    public static final String HIPPO_OPER_ERROR = "203";

    public static final String HIPPO_OVER_LIMIT = "204";

    public static final String HIPPO_MDB_DELETE = "205";

    /**
     * connection error, begin with 3
     */
    public static final String HIPPO_RECONNECTION_ERROR = "301";

    public static final String HIPPO_CONNECTION_SOCKET_CREATE_FAILURE = "302";

    public static final String HIPPO_CONNECTION_FAILURE = "303";

    public static final String HIIPO_CONNECTION_BIND_FAILURE = "304";

    /**
     * client error,begin with 4
     */
    public static final String HIPPO_CLIENT_ERROR = "401";

    public static final String HIPPO_CLIENT_SESSION_ERROR = "402";

    public static final String HIPPO_CLIENT_SPI_INIT_ERROR = "403";
}
