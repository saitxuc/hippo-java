package com.hippo.network.command;

/**
 * 
 * @author saitxuc
 * write 2014-7-1
 *
 */
public class CommandConstants {

    public static final String COMMAND_ID = "commandId";
    public static final String TYPE_REQ = "req";
    public static final String TYPE_RSP = "rsp";
    public static final String VERSION = "version";
    public static final String TYPE = "type";
    public static final String KEY = "key";
    public static final String CONNECTION_INFO = "cinfo";
    public static final String SESSION_INFO = "sinfo";
    public static final String CONNECT_REMOVE_INFO = "crinfo";
    public static final String SESION_REMOVE_INFO = "srinfo";
    public static final String KEEPALIVE = "keepalive";
    public static final String ALIVE_MSG = "alive_msg";
    public static final String ALIVE_MSG_CONTENT = "hello";

    public static final String RESPONSE_FAILURE = "failed";

    public static final int REQUEST_TIMEOUT = 6000;

    public static final String SET_COMMAND_ACTION = "set";
    public static final String GET_COMMAND_ACTION = "get";
    public static final String EXISTS_COMMAND_ACTION = "exists";
    public static final String ATOMICNT_COMMAND_ACTION = "atomicnt";
    public static final String HEAD_KEY = "key";

    public static final byte ATOMICNT_OPR_TYPE_ADD = 1;
    public static final byte ATOMICNT_OPR_TYPE_SUB = -1;
    //public static final byte exchange = 4;  // exchange
    //public static final byte compare_exchange = 5;  // compare exchange
    public static final String UPDATE_COMMAND_ACTION = "update";
    public static final String ADD_COMMAND_ACTION = "add";
    public static final String REMOVE_COMMAND_ACTION = "remove";
    public static final String REMOVELIST_COMMAND_ACTION = "removelist";
    public static final String BITGET_COMMAND_ACTION = "bitget";
    public static final String BITSET_COMMAND_ACTION = "bitset";
    public static final String BIT_OFFSET = "bitoffset";
    public static final String BIT_OFFSETBEGIN = "bitoffsetbegin";
    public static final String BIT_OFFSETEND = "bitoffsetend";
    public static final String BIT_WHOLEGET = "bitwholeget";
    public static final String BIT_NOT_EXIST = "bitnotexist";
    public static final String BIT_VAL = "bitval";
    public static final String BIT_GET_EXT_CODE = "bitgeterrorcode";
    public static final String BITSET_SEPRATOR = "|";
}
