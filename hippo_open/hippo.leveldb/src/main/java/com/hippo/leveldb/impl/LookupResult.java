package com.hippo.leveldb.impl;

import com.google.common.base.Preconditions;
import com.hippo.leveldb.util.Slice;

public class LookupResult
{
    public static LookupResult ok(LookupKey key, Slice value)
    {
        return new LookupResult(key, value, false);
    }

    public static LookupResult deleted(LookupKey key)
    {
        return new LookupResult(key, null, true);
    }

    private final LookupKey key;
    private final Slice value;
    private final boolean deleted;

    private LookupResult(LookupKey key, Slice value, boolean deleted)
    {
        Preconditions.checkNotNull(key, "key is null");
        this.key = key;
        if (value != null) {
            this.value = value.slice();
        } else {
            this.value = null;
        }
        this.deleted = deleted;
    }

    public LookupKey getKey()
    {
        return key;
    }

    public Slice getValue()
    {
        if (value == null) {
            return null;
        }
        return value;
    }

    public boolean isDeleted()
    {
        return deleted;
    }
    
    private long expireTime;

    public LookupResult expireTime(long expireTime) {
        this.expireTime = expireTime;
        return this;
    }

    public long getExpireTime() {
        return expireTime;
    }

    private int version;
    public LookupResult version(int version) {
        this.version = version;
        return this;
    }
    
    public int version() {
        return version;
    }
}
