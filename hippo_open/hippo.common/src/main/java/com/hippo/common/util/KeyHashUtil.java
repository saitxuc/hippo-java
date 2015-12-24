package com.hippo.common.util;

/**
 * 
 * @author saitxuc
 *
 */
public class KeyHashUtil {
	
    public static int calculateHashBucket(byte[] key, int bucket) {
        int hash = HashingUtil.murmur3_32(key);
        hash += (hash << 15) ^ 0xffffcd7d;
        hash ^= (hash >>> 10);
        hash += (hash << 3);
        hash ^= (hash >>> 6);
        hash += (hash << 2) + (hash << 14);
        hash = hash ^ (hash >>> 16);
        return Math.abs(hash) % bucket;
    }
	
}
