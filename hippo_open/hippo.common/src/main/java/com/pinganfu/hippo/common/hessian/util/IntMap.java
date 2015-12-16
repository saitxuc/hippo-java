package com.pinganfu.hippo.common.hessian.util;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 11:59
 *
 */
public class IntMap {
	
	/**
	   * Encoding of a null entry.  Since NULL is equal to Integer.MIN_VALUE, 
	   * it's impossible to distinguish between the two.
	   */
	  public final static int NULL = 0xdeadbeef; // Integer.MIN_VALUE + 1;

	  private static final Object DELETED = new Object();

	  private Object []_keys;
	  private int []_values;

	  private int _size;
	  private int _mask;

	  /**
	   * Create a new IntMap.  Default size is 16.
	   */
	  public IntMap()
	  {
	    _keys = new Object[256];
	    _values = new int[256];

	    _mask = _keys.length - 1;
	    _size = 0;
	  }

	  /**
	   * Clear the hashmap.
	   */
	  public void clear()
	  {
	    Object []keys = _keys;
	    int []values = _values;

	    for (int i = keys.length - 1; i >= 0; i--) {
	      keys[i] = null;
	      values[i] = 0;
	    }

	    _size = 0;
	  }
	  /**
	   * Returns the current number of entries in the map.
	   */
	  public int size()
	  {
	    return _size;
	  }

	  /**
	   * Puts a new value in the property table with the appropriate flags
	   */
	  public int get(Object key)
	  {
	    int mask = _mask;
	    int hash = key.hashCode() % mask & mask;

	    Object []keys = _keys;

	    while (true) {
	      Object mapKey = keys[hash];

	      if (mapKey == null)
	        return NULL;
	      else if (mapKey == key || mapKey.equals(key))
	        return _values[hash];

	      hash = (hash + 1) % mask;
	    }
	  }

	  /**
	   * Expands the property table
	   */
	  private void resize(int newSize)
	  {
	    Object []newKeys = new Object[newSize];
	    int []newValues = new int[newSize];

	    int mask = _mask = newKeys.length - 1;

	    Object []keys = _keys;
	    int values[] = _values;

	    for (int i = keys.length - 1; i >= 0; i--) {
	      Object key = keys[i];

	      if (key == null || key == DELETED)
	        continue;

	      int hash = key.hashCode() % mask & mask;

	      while (true) {
	        if (newKeys[hash] == null) {
	          newKeys[hash] = key;
	          newValues[hash] = values[i];
	          break;
	        }

	        hash = (hash + 1) % mask;
	      }
	    }

	    _keys = newKeys;
	    _values = newValues;
	  }

	  /**
	   * Puts a new value in the property table with the appropriate flags
	   */
	  public int put(Object key, int value)
	  {
	    int mask = _mask;
	    int hash = key.hashCode() % mask & mask;

	    Object []keys = _keys;

	    while (true) {
	      Object testKey = keys[hash];

	      if (testKey == null || testKey == DELETED) {
	        keys[hash] = key;
	        _values[hash] = value;

	        _size++;

	        if (keys.length <= 4 * _size)
	          resize(4 * keys.length);

	        return NULL;
	      }
	      else if (key != testKey && ! key.equals(testKey)) {
	        hash = (hash + 1) % mask;

	        continue;
	      }
	      else {
	        int old = _values[hash];

	        _values[hash] = value;

	        return old;
	      }
	    }
	  }

	  /**
	   * Deletes the entry.  Returns true if successful.
	   */
	  public int remove(Object key)
	  {
	    int mask = _mask;
	    int hash = key.hashCode() % mask & mask;

	    while (true) {
	      Object mapKey = _keys[hash];

	      if (mapKey == null)
	        return NULL;
	      else if (mapKey == key) {
	        _keys[hash] = DELETED;

	        _size--;

	        return _values[hash];
	      }

	      hash = (hash + 1) % mask;
	    }
	  }

	  public String toString()
	  {
	    StringBuffer sbuf = new StringBuffer();

	    sbuf.append("IntMap[");
	    boolean isFirst = true;

	    for (int i = 0; i <= _mask; i++) {
	      if (_keys[i] != null && _keys[i] != DELETED) {
	        if (! isFirst)
	          sbuf.append(", ");

	        isFirst = false;
	        sbuf.append(_keys[i]);
	        sbuf.append(":");
	        sbuf.append(_values[i]);
	      }
	    }
	    sbuf.append("]");

	    return sbuf.toString();
	  }
	
}
