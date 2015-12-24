package com.hippo.leveldb.cluster;

/**
 * @author yangxin
 */
public class Pair<K, V> {
	private K key;
	private V value;

	public Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	public final K getKey() {
		return key;
	}

	public final V getValue() {
		return value;
	}
}
