package com.hippo.client;

import java.io.Serializable;

import com.hippo.common.lifecycle.LifeCycle;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author saitxuc
 * write 2014-6-30
 */
public interface HippoClient extends LifeCycle {
	
	public static final int DEFAULT_NAMESPACE = -1;
	public HippoResult get(Serializable key);

	public HippoResult set(int expire, Serializable key, Serializable value,int version);
	public HippoResult set(int expire, Serializable key, Serializable value);
	public HippoResult set(Serializable key, Serializable value);
	
	public <T> HippoResult set(int expire, Serializable key, List<T> value,int version);
	public <K, V> HippoResult set(int expire, Serializable key, Map<K, V> value,int version);
	public <T> HippoResult set(int expire, Serializable key, Set<T> value,int version);

	public HippoResult update(int expire, Serializable key, Serializable value,int version);
	public HippoResult update(int expire, Serializable key, Serializable value);
	
	@Deprecated
	public HippoResult update(Serializable key, Serializable value);

	
	public <T> HippoResult update(int expire, Serializable key, List<T> value,int version);
	public <T> HippoResult update(int expire, Serializable key, List<T> value);
	
	@Deprecated
	public <T> HippoResult update(Serializable key, List<T> value);
	
	public <T> HippoResult update(int expire, Serializable key, Set<T> value,int version);
	public <T> HippoResult update(int expire, Serializable key, Set<T> value);
	
	@Deprecated
	public <T> HippoResult update(Serializable key, Set<T> value);
	
	public <K, V> HippoResult update(int expire, Serializable key, Map<K, V> value,int version);
	public <K, V> HippoResult update(int expire, Serializable key, Map<K, V> value);
	
	@Deprecated
	public <K, V> HippoResult update(Serializable key, Map<K, V> value);
	
	@Deprecated
	public HippoResult remove(int expire, Serializable key,int version);
	@Deprecated
	public HippoResult remove(int expire, Serializable key);
	
	public HippoResult remove(Serializable key);

	/***
	 * 
	 * @param expire
	 * @param key
	 * @param value
	 * @param defaultValue
	 * @param checkValEdge 是否检查数据边界,如果true 则发生数据溢出,返回失败,更新也失败 , 如果是false不做数据检查
	 * @return
	 */
    public HippoResult incr(int expire, Serializable key, long value, long defaultValue, boolean checkValEdge);
    public HippoResult incr(Serializable key, long value, long defaultValue);
    public HippoResult incr(Serializable key, long value);
    public HippoResult incr(Serializable key);
 
    /***
     * @param expire
     * @param key
     * @param value
     * @param defaultValue
     * @param checkValEdge 是否检查数据边界,如果true 则发生数据溢出,返回失败,更新也失败 , 如果是false不做数据检查
     * @return
     */
    public HippoResult decr(int expire, Serializable key, long value, long defaultValue, boolean checkValEdge);
    public HippoResult decr(Serializable key, long value, long defaultValue);
    public HippoResult decr(Serializable key, long value);
    public HippoResult decr(Serializable key);

    /**
    *
    * @param key
    * @param maxOffset
    * @param requestExpire
    * @return
    */
   public HippoResult getWholeBit(Serializable key, int maxOffset, int requestExpire, int timeOut);

   /**
    *
    * @param key
    * @param maxOffset
    * @param requestExpire
    * @param timeOut
    * @return
    */
   public HippoResult removeWholeBit(Serializable key, int maxOffset, int requestExpire, int timeOut);

   /**
    *
    * @param key
    * @param offset
    * @param requestExpire
    * @return
    */
   public HippoResult getBit(Serializable key, int offset, int requestExpire);

   /**
    *
    * @param key
    * @param offset
    * @param val
    * @param expire
    * @param requestExpire
    * @return
    */
   public HippoResult setBit(Serializable key, int offset, boolean val, int expire, int requestExpire);
}
