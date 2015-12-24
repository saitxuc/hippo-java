package com.hippo.leveldb.util;

import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.hippo.leveldb.impl.InternalKey;

public class InternalTableIterator extends AbstractSeekingIterator<InternalKey, Slice> implements InternalIterator
{
    private final TableIterator tableIterator;

    public InternalTableIterator(TableIterator tableIterator)
    {
        this.tableIterator = tableIterator;
    }

    @Override
    protected void seekToFirstInternal()
    {
        tableIterator.seekToFirst();
    }

    @Override
    public void seekInternal(InternalKey targetKey)
    {
        tableIterator.seek(targetKey.encode());
    }

    @Override
    protected Entry<InternalKey, Slice> getNextElement()
    {
        if (tableIterator.hasNext()) {
            Entry<Slice, Slice> next = tableIterator.next();
            return Maps.immutableEntry(new InternalKey(next.getKey()), next.getValue());
        }
        return null;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("InternalTableIterator");
        sb.append("{fromIterator=").append(tableIterator);
        sb.append('}');
        return sb.toString();
    }
}
