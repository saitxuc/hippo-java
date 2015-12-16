package com.pinganfu.hippo.leveldb.util;

import com.pinganfu.hippo.leveldb.impl.InternalKey;
import com.pinganfu.hippo.leveldb.impl.SeekingIterator;

/**
 * <p>A common interface for internal iterators.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface InternalIterator extends SeekingIterator<InternalKey, Slice> {
}
