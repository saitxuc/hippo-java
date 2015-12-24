package com.hippo.stomp;

/**
 * 
 * @author saitxuc
 *  write 2014-6-30
 */
public interface Stomp {
	
	String NULL = "\u0000";
    String NEWLINE = "\n";

    byte BREAK = '\n';
    byte COLON = ':';
    byte ESCAPE = '\\';
    
    String COMMA = ",";
    String V1_0 = "1.0";
    String V1_1 = "1.1";
    String V1_2 = "1.2";
    String DEFAULT_HEART_BEAT = "0,0";
    String DEFAULT_VERSION = "1.0";
    String EMPTY = "";

    String[] SUPPORTED_PROTOCOL_VERSIONS = {"1.2", "1.1", "1.0"};

    String TEXT_PLAIN = "text/plain";
    String TRUE = "true";
    String FALSE = "false";
    String END = "end";
    
    String TYPE = "type";
    
    public static interface Commands {
        String STOMP = "STOMP";
        String CONNECT = "CONNECT";
        String SET = "SET";
        String GET = "GET";
        String DISCONNECT = "DISCONNECT";
        String KEEPALIVE = "KEEPALIVE";
    }
    
    public interface Responses {
        String CONNECTED = "CONNECTED";
        String ERROR = "ERROR";
        String MESSAGE = "MESSAGE";
    }
    
    public interface Headers {
        String SEPERATOR = ":";
        String CONTENT_LENGTH = "content-length";
        String CONTENT_TYPE = "content-type";
        
        String COMMAND_ID = "commandId";
		String TYPE_REQ = "req";
		String TYPE_RSP = "rsp";
		String KEY = "key";
		String VERSION = "version";
		String MSG = "msg";
        
        public interface Response {
            String TRACE_ID = "TRACE-id";
        }
        
        public interface Send {
            String KEY = "key";
            String EXPIRATION_TIME = "expires";
            String TYPE = "type";
            //String PERSISTENT = "persistent";
            
        }
        
    }
}
