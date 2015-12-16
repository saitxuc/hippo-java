package com.pinganfu.hippo.leveldb.table;

import java.util.Comparator;

import com.pinganfu.hippo.leveldb.util.Slice;

// todo this interface needs more thought
public interface UserComparator extends Comparator<Slice>
{
    String name();

    Slice findShortestSeparator(Slice start, Slice limit);

    Slice findShortSuccessor(Slice key);
}
