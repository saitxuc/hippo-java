package com.pinganfu.hippo.store;



/**
 * @author yangxin
 */
public interface IoNetCallable<E> {
	public abstract void transport(byte[] data, E standby, int bucket);
}
