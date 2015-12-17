package com.hippo.leveldb.util;

import com.hippo.leveldb.impl.InternalKey;
import com.hippo.leveldb.impl.SeekingIterator;

/**
 * <p>A common interface for internal iterators.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface InternalIterator extends SeekingIterator<InternalKey, Slice> {
}
