package com.pinganfu.hippo.network;

import java.io.Serializable;

import com.pinganfu.hippo.network.command.CommandTypes;

/**
 * 
 * @author saitxuc
 * write 2014-7-11
 */
public class ConnectionId implements Serializable, Comparable<ConnectionId> {

    /**
	 * 
	 */
	private static final long serialVersionUID = 3103073402900330847L;

	public static final byte DATA_STRUCTURE_TYPE = CommandTypes.CONNECTION_ID;

    protected String value;

    public ConnectionId() {
    }

    public ConnectionId(String connectionId) {
        this.value = connectionId;
    }

    public ConnectionId(ConnectionId id) {
        this.value = id.getValue();
    }

    public ConnectionId(SessionId id) {
        this.value = id.getConnectionId();
    }


    public int hashCode() {
        return value.hashCode();
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != ConnectionId.class) {
            return false;
        }
        ConnectionId id = (ConnectionId)o;
        return value.equals(id.value);
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public String toString() {
        return value;
    }

    /**
     * @openwire:property version=1
     */
    public String getValue() {
        return value;
    }

    public void setValue(String connectionId) {
        this.value = connectionId;
    }

    public boolean isMarshallAware() {
        return false;
    }

    public int compareTo(ConnectionId o) {
        return value.compareTo(o.value);
    }
}