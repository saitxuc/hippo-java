package com.hippo.mdb.impl;

import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.mdb.Lru;
import com.hippo.mdb.obj.DBAssembleInfo;
import com.hippo.mdb.obj.OffsetInfo;
import com.hippo.store.exception.HippoStoreException;

/**
 * @author Owen
 */
public class FastPeriodsLRU implements Lru {
    protected static final Logger LOG = LoggerFactory.getLogger(FastPeriodsLRU.class);

    private DBAssembleInfo dbAssembleInfo = null;

    private LinkedList<OffsetInfo> sortedList4LRU = null;

    private Object waiting = new Object();

    public FastPeriodsLRU(DBAssembleInfo dbAssembleInfo) {
        this.dbAssembleInfo = dbAssembleInfo;
        sortedList4LRU = dbAssembleInfo.getSortedLRUList();
    }

    @Override
    public OffsetInfo lru() throws HippoStoreException {
        OffsetInfo offsetInfo;
        
        for (;;) {
            dbAssembleInfo.callLRU();
            synchronized (sortedList4LRU) {
                offsetInfo = sortedList4LRU.pollFirst();
            }
            if (offsetInfo != null) {
                break;
            }

            synchronized (waiting) {
                try {
                    waiting.wait(50);
                } catch (InterruptedException ignored) {
                }
            }
        }

        return offsetInfo;
    }

    @Override
    public void addOffsetInfo(OffsetInfo info) {
    }
}
