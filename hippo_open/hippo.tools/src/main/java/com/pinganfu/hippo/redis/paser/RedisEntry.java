package com.pinganfu.hippo.redis.paser;

public class RedisEntry {
    private String key;
    private Object value;
    private int type; /* redis数据类型 */
    private long expire; /* 过期时间 , milliseconds*/

    public RedisEntry() {
    }

    public RedisEntry(String key, Object value, int type, long expire) {
        super();
        this.key = key;
        this.value = value;
        this.type = type;
        this.expire = expire;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getExpire() {
        return expire;
    }

    public void setExpire(long expire) {
        this.expire = expire;
    }

}
