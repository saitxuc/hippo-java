/**
 *  Copyright 2012 Dain Sundstrom
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.hippo.leveldb.table;


import com.hippo.leveldb.DBComparator;
import com.hippo.leveldb.util.Slice;

public class CustomUserComparator
        implements UserComparator
{
    private final DBComparator comparator;

    public CustomUserComparator(DBComparator comparator)
    {
        this.comparator = comparator;
    }

    @Override
    public String name() {
        return comparator.name();
    }

    @Override
    public Slice findShortestSeparator(Slice start, Slice limit) {
        return new Slice(comparator.findShortestSeparator(start.getBytes(), limit.getBytes()));
    }

    @Override
    public Slice findShortSuccessor(Slice key) {
        return new Slice(comparator.findShortSuccessor(key.getBytes()));
    }

    @Override
    public int compare(Slice o1, Slice o2) {
        return comparator.compare(o1.getBytes(), o2.getBytes());
    }
}
