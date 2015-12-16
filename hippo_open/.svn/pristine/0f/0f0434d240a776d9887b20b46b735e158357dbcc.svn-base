package com.pinganfu.hippo.mdb.impl;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import com.pinganfu.hippo.common.util.HashingUtil;
import com.pinganfu.hippo.mdb.DBAssembleManager;
import com.pinganfu.hippo.mdb.MdbManager;
import com.pinganfu.hippo.mdb.obj.MdbPointer;

/**
 * 
 * @author saitxuc
 * @param <K>
 *
 */
public class OffHeapMap<K, Pointer> extends AbstractMap<K, Pointer> implements ConcurrentMap<K, Pointer>, Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -8651062614701297088L;

    /* ---------------- Constants -------------- */

    private MdbManager mdbManager;

    /**
     * The default initial capacity for this table,
     * used when not otherwise specified in a constructor.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The default load factor for this table, used when not
     * otherwise specified in a constructor.
     */
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * The default concurrency level for this table, used when not
     * otherwise specified in a constructor.
     */
    static final int DEFAULT_CONCURRENCY_LEVEL = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly
     * specified by either of the constructors with arguments.  MUST
     * be a power of two <= 1<<30 to ensure that entries are indexable
     * using ints.
     */
    static final int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * The minimum capacity for per-segment tables.  Must be a power
     * of two, at least two to avoid immediate resizing on next use
     * after lazy construction.
     */
    static final int MIN_SEGMENT_TABLE_CAPACITY = 2;

    /**
     * The maximum number of segments to allow; used to bound
     * constructor arguments. Must be power of two less than 1 << 24.
     */
    static final int MAX_SEGMENTS = 1 << 16; // slightly conservative

    /**
     * Number of unsynchronized retries in size and containsValue
     * methods before resorting to locking. This is used to avoid
     * unbounded retries if tables undergo continuous modification
     * which would make it impossible to obtain an accurate result.
     */
    static final int RETRIES_BEFORE_LOCK = 2;

    /**
     * Mask value for indexing into segments. The upper bits of a
     * key's hash code are used to choose the segment.
     */
    final int segmentMask;

    /**
     * Shift value for indexing within segments.
     */
    final int segmentShift;

    /**
     * The segments, each of which is a specialized hash table.
     */
    final Segment<K, Pointer>[] segments;

    /* ---------------- Fields -------------- */

    /**
     * Creates a new, empty map with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param initialCapacity the initial capacity. The implementation
     * performs internal sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     * Resizing may be performed when the average number of elements per
     * bin exceeds this threshold.
     * @param concurrencyLevel the estimated number of concurrently
     * updating threads. The implementation performs internal sizing
     * to try to accommodate this many threads.
     * @throws IllegalArgumentException if the initial capacity is
     * negative or the load factor or concurrencyLevel are
     * nonpositive.
     */
    @SuppressWarnings("unchecked")
    public OffHeapMap(int initialCapacity, float loadFactor, int concurrencyLevel, MdbManager mdbManager) {
        if (!(loadFactor > 0) || initialCapacity < 0 || concurrencyLevel <= 0)
            throw new IllegalArgumentException();
        if (concurrencyLevel > MAX_SEGMENTS)
            concurrencyLevel = MAX_SEGMENTS;
        // Find power-of-two sizes best matching arguments
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrencyLevel) {
            ++sshift;
            ssize <<= 1;
        }
        this.mdbManager = mdbManager;
        this.segmentShift = 32 - sshift;
        this.segmentMask = ssize - 1;
        if (initialCapacity > MAXIMUM_CAPACITY)
            initialCapacity = MAXIMUM_CAPACITY;
        int c = initialCapacity / ssize;
        if (c * ssize < initialCapacity)
            ++c;
        int cap = MIN_SEGMENT_TABLE_CAPACITY;
        while (cap < c)
            cap <<= 1;
        // create segments and segments[0]
        Segment<K, Pointer> s0 = new Segment<K, Pointer>(loadFactor, (int) (cap * loadFactor), (HashEntry<Pointer>[]) new HashEntry[cap], this.mdbManager);
        Segment<K, Pointer>[] ss = (Segment<K, Pointer>[]) new Segment[ssize];
        UNSAFE.putOrderedObject(ss, SBASE, s0); // ordered write of segments[0]
        this.segments = ss;
    }

    /**
     * Creates a new, empty map with the specified initial capacity
     * and load factor and with the default concurrencyLevel (16).
     *
     * @param initialCapacity The implementation performs internal
     * sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     * Resizing may be performed when the average number of elements per
     * bin exceeds this threshold.
     * @throws IllegalArgumentException if the initial capacity of
     * elements is negative or the load factor is nonpositive
     *
     * @since 1.6
     */
    public OffHeapMap(int initialCapacity, float loadFactor) {
        this(initialCapacity, loadFactor, DEFAULT_CONCURRENCY_LEVEL, null);
    }

    /**
     * Creates a new, empty map with the specified initial capacity,
     * and with default load factor (0.75) and concurrencyLevel (16).
     *
     * @param initialCapacity the initial capacity. The implementation
     * performs internal sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     * elements is negative.
     */
    public OffHeapMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL, null);
    }

    public OffHeapMap(MdbManager mdbManager) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL, mdbManager);
    }

    /**
     * Creates a new, empty map with a default initial capacity (16),
     * load factor (0.75) and concurrencyLevel (16).
     */
    public OffHeapMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL, null);
    }

    @Override
    public Pointer putIfAbsent(K key, Pointer value) {
        Segment<K, Pointer> s;
        if (value == null)
            throw new NullPointerException();
        int hash = hash(key);
        int j = (hash >>> segmentShift) & segmentMask;
        if ((s = (Segment<K, Pointer>) UNSAFE.getObject(segments, (j << SSHIFT) + SBASE)) == null)
            s = ensureSegment(j);
        return s.put(key, hash, value, true);
    }

    @SuppressWarnings("unchecked")
    public Pointer put(K key, Pointer value) {
        Segment<K, Pointer> s;
        if (value == null)
            throw new NullPointerException();
        int hash = hash(key);
        int j = (hash >>> segmentShift) & segmentMask;
        if ((s = (Segment<K, Pointer>) UNSAFE.getObject // nonvolatile; recheck
            (segments, (j << SSHIFT) + SBASE)) == null) //  in ensureSegment
            s = ensureSegment(j);
        return s.put(key, hash, value, false);
    }

    public void putAll(Map<? extends K, ? extends Pointer> m) {
        for (Map.Entry<? extends K, ? extends Pointer> e : m.entrySet())
            put(e.getKey(), e.getValue());
    }

    public Pointer remove(Object key) {
        int hash = hash(key);
        Segment<K, Pointer> s = segmentForHash(hash);
        return s == null ? null : s.remove(key, hash, null);
    }

    @Override
    public boolean remove(Object key, Object value) {
        int hash = hash(key);
        Segment<K, Pointer> s;
        return value != null && (s = segmentForHash(hash)) != null && s.remove(key, hash, value) != null;
    }

    @Override
    public boolean replace(K key, Pointer oldValue, Pointer newValue) {
        int hash = hash(key);
        if (oldValue == null || newValue == null)
            throw new NullPointerException();
        Segment<K, Pointer> s = segmentForHash(hash);
        return s != null && s.replace(key, hash, oldValue, newValue);
    }

    @Override
    public Pointer replace(K key, Pointer value) {
        int hash = hash(key);
        if (value == null)
            throw new NullPointerException();
        Segment<K, Pointer> s = segmentForHash(hash);
        return s == null ? null : s.replace(key, hash, value);
    }

    public Pointer get(Object key) {
        Segment<K, Pointer> s; // manually integrate access methods to reduce overhead
        HashEntry<Pointer>[] tab;
        int h = hash(key);
        long u = (((h >>> segmentShift) & segmentMask) << SSHIFT) + SBASE;
        if ((s = (Segment<K, Pointer>) UNSAFE.getObjectVolatile(segments, u)) != null && (tab = s.table) != null) {
            for (HashEntry<Pointer> e = (HashEntry<Pointer>) UNSAFE.getObjectVolatile(tab, ((long) (((tab.length - 1) & h)) << TSHIFT) + TBASE); e != null; e = e.next) {

                MdbPointer mp = (MdbPointer) e.value;
                byte[] nkey = (byte[]) key;
                String sizeKey = mdbManager.getSizeType(mp.getLength());
                DBAssembleManager manager = mdbManager.getDBAssembleManager(sizeKey);

                boolean verify = manager.verifyKey(nkey, mp);
                if (e.hash == h && verify) {
                    return e.value;
                }

            }
        }
        return null;
    }

    @Override
    @Deprecated
    public Set<java.util.Map.Entry<K, Pointer>> entrySet() {
        throw new UnsupportedOperationException("entrySet() | This method not suppouted in this version!");
    }

    /**
     * Returns the number of key-value mappings in this map.  If the
     * map contains more than <tt>Integer.MAX_VALUE</tt> elements, returns
     * <tt>Integer.MAX_VALUE</tt>.
     *
     * @return the number of key-value mappings in this map
     */
    @Override
    public int size() {
        // Try a few times to get accurate count. On failure due to
        // continuous async changes in table, resort to locking.
        final Segment<K, Pointer>[] segments = this.segments;
        int size;
        boolean overflow; // true if size overflows 32 bits
        long sum; // sum of modCounts
        long last = 0L; // previous sum
        int retries = -1; // first iteration isn't retry
        try {
            for (;;) {
                if (retries++ == RETRIES_BEFORE_LOCK) {
                    for (int j = 0; j < segments.length; ++j)
                        ensureSegment(j).lock(); // force creation
                }
                sum = 0L;
                size = 0;
                overflow = false;
                for (int j = 0; j < segments.length; ++j) {
                    Segment<K, Pointer> seg = segmentAt(segments, j);
                    if (seg != null) {
                        sum += seg.modCount;
                        int c = seg.count;
                        if (c < 0 || (size += c) < 0)
                            overflow = true;
                    }
                }
                if (sum == last)
                    break;
                last = sum;
            }
        } finally {
            if (retries > RETRIES_BEFORE_LOCK) {
                for (int j = 0; j < segments.length; ++j)
                    segmentAt(segments, j).unlock();
            }
        }
        return overflow ? Integer.MAX_VALUE : size;
    }

    // Accessing segments
    /**
     * Gets the jth element of given segment array (if nonnull) with
     * volatile element access semantics via Unsafe. (The null check
     * can trigger harmlessly only during deserialization.) Note:
     * because each element of segments array is set only once (using
     * fully ordered writes), some performance-sensitive methods rely
     * on this method only as a recheck upon null reads.
     */
    @SuppressWarnings("unchecked")
    static final <K, V> Segment<K, V> segmentAt(Segment<K, V>[] ss, int j) {
        long u = (j << SSHIFT) + SBASE;
        return ss == null ? null : (Segment<K, V>) UNSAFE.getObjectVolatile(ss, u);
    }

    private Segment<K, Pointer> segmentForHash(int h) {
        long u = (((h >>> segmentShift) & segmentMask) << SSHIFT) + SBASE;
        return (Segment<K, Pointer>) UNSAFE.getObjectVolatile(segments, u);
    }

    /*	private transient final int hashSeed = randomHashSeed(this);
    	
       private int randomHashSeed(OffHeapMap<K, Pointer> offHeapMap) {
        	if (sun.misc.VM.isBooted() && Holder.ALTERNATIVE_HASHING) {
                return sun.misc.Hashing.randomHashSeed(offHeapMap);
            }

            return 0;
    	}*/

    static final <K, V> void setEntryAt(HashEntry<V>[] tab, int i, HashEntry<V> e) {
        UNSAFE.putOrderedObject(tab, ((long) i << TSHIFT) + TBASE, e);
    }

    /**
     * Returns the segment for the given index, creating it and
     * recording in segment table (via CAS) if not already present.
     *
     * @param k the index
     * @return the segment
     */
    @SuppressWarnings("unchecked")
    private Segment<K, Pointer> ensureSegment(int k) {
        final Segment<K, Pointer>[] ss = this.segments;
        long u = (k << SSHIFT) + SBASE; // raw offset
        Segment<K, Pointer> seg;
        if ((seg = (Segment<K, Pointer>) UNSAFE.getObjectVolatile(ss, u)) == null) {
            Segment<K, Pointer> proto = ss[0]; // use segment 0 as prototype
            int cap = proto.table.length;
            float lf = proto.loadFactor;
            int threshold = (int) (cap * lf);
            HashEntry<Pointer>[] tab = (HashEntry<Pointer>[]) new HashEntry[cap];
            if ((seg = (Segment<K, Pointer>) UNSAFE.getObjectVolatile(ss, u)) == null) { // recheck
                Segment<K, Pointer> s = new Segment<K, Pointer>(lf, threshold, tab, this.mdbManager);
                while ((seg = (Segment<K, Pointer>) UNSAFE.getObjectVolatile(ss, u)) == null) {
                    if (UNSAFE.compareAndSwapObject(ss, u, null, seg = s))
                        break;
                }
            }
        }
        return seg;
    }

    private int hash(Object k) {

        if (k instanceof byte[]) {
            return HashingUtil.murmur3_32((byte[]) k);
        } else {

            int h = k.hashCode();

            /*  if ((0 != h) && (k instanceof String)) {
                  return sun.misc.Hashing.stringHash32((String) k);
              }*/

            h ^= k.hashCode();

            // Spread bits to regularize both segment and index locations,
            // using variant of single-word Wang/Jenkins hash.
            h += (h << 15) ^ 0xffffcd7d;
            h ^= (h >>> 10);
            h += (h << 3);
            h ^= (h >>> 6);
            h += (h << 2) + (h << 14);
            return h ^ (h >>> 16);
        }

    }

    @SuppressWarnings("unchecked")
    static final <K, V> HashEntry<V> entryAt(HashEntry<V>[] tab, int i) {
        return (tab == null) ? null : (HashEntry<V>) UNSAFE.getObjectVolatile(tab, ((long) i << TSHIFT) + TBASE);
    }

    private static class Holder {

        /**
        * Enable alternative hashing of String keys?
        *
        * <p>Unlike the other hash map implementations we do not implement a
        * threshold for regulating whether alternative hashing is used for
        * String keys. Alternative hashing is either enabled for all instances
        * or disabled for all instances.
        */
        static final boolean ALTERNATIVE_HASHING;

        static {
            // Use the "threshold" system property even though our threshold
            // behaviour is "ON" or "OFF".
            String altThreshold = java.security.AccessController.doPrivileged(new sun.security.action.GetPropertyAction("jdk.map.althashing.threshold"));

            int threshold;
            try {
                threshold = (null != altThreshold) ? Integer.parseInt(altThreshold) : Integer.MAX_VALUE;

                // disable alternative hashing if -1
                if (threshold == -1) {
                    threshold = Integer.MAX_VALUE;
                }

                if (threshold < 0) {
                    throw new IllegalArgumentException("value must be positive integer.");
                }
            } catch (IllegalArgumentException failed) {
                throw new Error("Illegal value for 'jdk.map.althashing.threshold'", failed);
            }
            ALTERNATIVE_HASHING = threshold <= MAXIMUM_CAPACITY;
        }
    }

    static final class Segment<K, Pointer> extends ReentrantLock implements Serializable {

        private static final long serialVersionUID = 2249069246763182397L;

        /**
         * The maximum number of times to tryLock in a prescan before
         * possibly blocking on acquire in preparation for a locked
         * segment operation. On multiprocessors, using a bounded
         * number of retries maintains cache acquired while locating
         * nodes.
         */
        static final int MAX_SCAN_RETRIES = Runtime.getRuntime().availableProcessors() > 1 ? 64 : 1;

        /**
         * The per-segment table. Elements are accessed via
         * entryAt/setEntryAt providing volatile semantics.
         */
        transient volatile HashEntry<Pointer>[] table;

        /**
         * The number of elements. Accessed only either within locks
         * or among other volatile reads that maintain visibility.
         */
        transient int count;

        /**
         * The total number of mutative operations in this segment.
         * Even though this may overflows 32 bits, it provides
         * sufficient accuracy for stability checks in CHM isEmpty()
         * and size() methods.  Accessed only either within locks or
         * among other volatile reads that maintain visibility.
         */
        transient int modCount;

        /**
         * The table is rehashed when its size exceeds this threshold.
         * (The value of this field is always <tt>(int)(capacity *
         * loadFactor)</tt>.)
         */
        transient int threshold;

        /**
         * The load factor for the hash table.  Even though this value
         * is same for all segments, it is replicated to avoid needing
         * links to outer object.
         * @serial
         */
        final float loadFactor;

        private MdbManager mdbManager;

        Segment(float lf, int threshold, HashEntry<Pointer>[] tab, MdbManager mdbManager) {
            this.loadFactor = lf;
            this.threshold = threshold;
            this.table = tab;
            this.mdbManager = mdbManager;
        }

        final Pointer put(K key, int hash, Pointer value, boolean onlyIfAbsent) {
            HashEntry<Pointer> node = tryLock() ? null : scanAndLockForPut(key, hash, value);
            Pointer oldValue;
            try {
                HashEntry<Pointer>[] tab = table;
                int index = (tab.length - 1) & hash;
                HashEntry<Pointer> first = entryAt(tab, index);
                for (HashEntry<Pointer> e = first;;) {
                    if (e != null) {
                        MdbPointer oldPoint = (MdbPointer) e.value;
                        byte[] nkey = (byte[]) key;
                        String sizeKey = mdbManager.getSizeType(oldPoint.getLength());
                        DBAssembleManager manager = mdbManager.getDBAssembleManager(sizeKey);
                        boolean verify = manager.verifyKey(nkey, oldPoint);
                        if (e.hash == hash && verify) {
                            oldValue = e.value;
                            if (!onlyIfAbsent) {
                                e.value = value;
                                ++modCount;
                                //make sure to remove the old point refer to the dbinfo
                                MdbPointer newPoint = (MdbPointer) value;
                                if (!newPoint.getDbNo().equals(oldPoint.getDbNo()) || newPoint.getOffset() != oldPoint.getOffset() || newPoint.getBuckId() != oldPoint
                                    .getBuckId()) {
                                    String oldSizeKey = mdbManager.getSizeType(oldPoint.getLength());
                                    manager = mdbManager.getDBAssembleManager(oldSizeKey);
                                    //set the recycle the flag, so the expire thread could find it and decrease the freecount
                                    manager.setRecyclePoint(oldPoint);
                                }
                            }
                            break;
                        }
                        e = e.next;
                    } else {
                        if (node != null)
                            node.setNext(first);
                        else
                            node = new HashEntry<Pointer>(hash, value, first);
                        int c = count + 1;
                        if (c > threshold && tab.length < MAXIMUM_CAPACITY)
                            rehash(node);
                        else
                            setEntryAt(tab, index, node);
                        ++modCount;
                        count = c;
                        oldValue = null;
                        break;
                    }
                }
            } finally {
                unlock();
            }
            return oldValue;
        }

        /**
         * Doubles size of table and repacks entries, also adding the
         * given node to new table
         */
        @SuppressWarnings("unchecked")
        private void rehash(HashEntry<Pointer> node) {
            /*
             * Reclassify nodes in each list to new table.  Because we
             * are using power-of-two expansion, the elements from
             * each bin must either stay at same index, or move with a
             * power of two offset. We eliminate unnecessary node
             * creation by catching cases where old nodes can be
             * reused because their next fields won't change.
             * Statistically, at the default threshold, only about
             * one-sixth of them need cloning when a table
             * doubles. The nodes they replace will be garbage
             * collectable as soon as they are no longer referenced by
             * any reader thread that may be in the midst of
             * concurrently traversing table. Entry accesses use plain
             * array indexing because they are followed by volatile
             * table write.
             */
            HashEntry<Pointer>[] oldTable = table;
            int oldCapacity = oldTable.length;
            int newCapacity = oldCapacity << 1;
            threshold = (int) (newCapacity * loadFactor);
            HashEntry<Pointer>[] newTable = (HashEntry<Pointer>[]) new HashEntry[newCapacity];
            int sizeMask = newCapacity - 1;
            for (int i = 0; i < oldCapacity; i++) {
                HashEntry<Pointer> e = oldTable[i];
                if (e != null) {
                    HashEntry<Pointer> next = e.next;
                    int idx = e.hash & sizeMask;
                    if (next == null) //  Single node on list
                        newTable[idx] = e;
                    else { // Reuse consecutive sequence at same slot
                        HashEntry<Pointer> lastRun = e;
                        int lastIdx = idx;
                        for (HashEntry<Pointer> last = next; last != null; last = last.next) {
                            int k = last.hash & sizeMask;
                            if (k != lastIdx) {
                                lastIdx = k;
                                lastRun = last;
                            }
                        }
                        newTable[lastIdx] = lastRun;
                        // Clone remaining nodes
                        for (HashEntry<Pointer> p = e; p != lastRun; p = p.next) {
                            Pointer v = p.value;
                            int h = p.hash;
                            int k = h & sizeMask;
                            HashEntry<Pointer> n = newTable[k];
                            newTable[k] = new HashEntry<Pointer>(h, v, n);
                        }
                    }
                }
            }
            int nodeIndex = node.hash & sizeMask; // add the new node
            node.setNext(newTable[nodeIndex]);
            newTable[nodeIndex] = node;
            table = newTable;
        }

        /**
         * Scans for a node containing given key while trying to
         * acquire lock, creating and returning one if not found. Upon
         * return, guarantees that lock is held. UNlike in most
         * methods, calls to method equals are not screened: Since
         * traversal speed doesn't matter, we might as well help warm
         * up the associated code and accesses as well.
         *
         * @return a new node if key not found, else null
         */
        private HashEntry<Pointer> scanAndLockForPut(K key, int hash, Pointer value) {
            HashEntry<Pointer> first = entryForHash(this, hash);
            HashEntry<Pointer> e = first;
            HashEntry<Pointer> node = null;
            int retries = -1; // negative while locating node
            while (!tryLock()) {
                HashEntry<Pointer> f; // to recheck first below
                if (retries < 0) {
                    if (e == null) {
                        if (node == null) // speculatively create node
                            node = new HashEntry<Pointer>(hash, value, null);
                        retries = 0;
                    } else {
                        MdbPointer mp = (MdbPointer) e.value;
                        byte[] nkey = (byte[]) key;
                        String sizeKey = mdbManager.getSizeType(mp.getLength());
                        DBAssembleManager manager = mdbManager.getDBAssembleManager(sizeKey);
                        if (manager.verifyKey(nkey, mp)) {
                            retries = 0;
                        } else {
                            e = e.next;
                        }
                    }
                } else if (++retries > MAX_SCAN_RETRIES) {
                    lock();
                    break;
                } else if ((retries & 1) == 0 && (f = entryForHash(this, hash)) != first) {
                    e = first = f; // re-traverse if entry changed
                    retries = -1;
                }
            }
            return node;
        }

        /**
         * Scans for a node containing the given key while trying to
         * acquire lock for a remove or replace operation. Upon
         * return, guarantees that lock is held.  Note that we must
         * lock even if the key is not found, to ensure sequential
         * consistency of updates.
         */
        private void scanAndLock(Object key, int hash) {
            // similar to but simpler than scanAndLockForPut
            HashEntry<Pointer> first = entryForHash(this, hash);
            HashEntry<Pointer> e = first;
            int retries = -1;
            while (!tryLock()) {
                HashEntry<Pointer> f;
                if (retries < 0) {
                    if (e == null) {
                        retries = 0;
                    } else {
                        MdbPointer mp = (MdbPointer) e.value;
                        byte[] nkey = (byte[]) key;
                        String sizeKey = mdbManager.getSizeType(mp.getLength());
                        DBAssembleManager manager = mdbManager.getDBAssembleManager(sizeKey);
                        if (manager.verifyKey(nkey, mp)) {
                            retries = 0;
                        } else {
                            e = e.next;
                        }
                    }

                } else if (++retries > MAX_SCAN_RETRIES) {
                    lock();
                    break;
                } else if ((retries & 1) == 0 && (f = entryForHash(this, hash)) != first) {
                    e = first = f;
                    retries = -1;
                }
            }
        }

        /**
         * Remove; match on key only if value null, else match both.
         */
        final Pointer remove(Object key, int hash, Object value) {
            if (!tryLock())
                scanAndLock(key, hash);
            Pointer oldValue = null;
            try {
                HashEntry<Pointer>[] tab = table;
                int index = (tab.length - 1) & hash;
                HashEntry<Pointer> e = entryAt(tab, index);
                HashEntry<Pointer> pre = null;
                while (e != null) {
                    HashEntry<Pointer> next = e.next;

                    MdbPointer mp = (MdbPointer) e.value;
                    byte[] nKey = (byte[]) key;
                    String sizeKey = mdbManager.getSizeType(mp.getLength());
                    DBAssembleManager manager = mdbManager.getDBAssembleManager(sizeKey);
                    boolean verify = manager.verifyKey(nKey, mp);
                    if (e.hash == hash && verify) {
                        Pointer v = e.value;
                        if (value == null || value == v || value.equals(v)) {
                            if (pre == null)
                                setEntryAt(tab, index, next);
                            else
                                pre.setNext(next);
                            ++modCount;
                            --count;
                            oldValue = v;
                        }
                        break;
                    }
                    pre = e;
                    e = next;
                }
            } finally {
                unlock();
            }
            return oldValue;
        }

        final boolean replace(K key, int hash, Pointer oldValue, Pointer newValue) {
            if (!tryLock())
                scanAndLock(key, hash);
            boolean replaced = false;
            try {
                HashEntry<Pointer> e;
                for (e = entryForHash(this, hash); e != null; e = e.next) {

                    MdbPointer mp = (MdbPointer) e.value;
                    byte[] nkey = (byte[]) key;
                    String sizeKey = mdbManager.getSizeType(mp.getLength());
                    DBAssembleManager manager = mdbManager.getDBAssembleManager(sizeKey);
                    boolean verify = manager.verifyKey(nkey, mp);
                    if (e.hash == hash && verify) {
                        if (oldValue.equals(e.value)) {
                            e.value = newValue;
                            ++modCount;
                            replaced = true;
                        }
                        break;
                    }
                }
            } finally {
                unlock();
            }
            return replaced;
        }

        final Pointer replace(K key, int hash, Pointer value) {
            if (!tryLock())
                scanAndLock(key, hash);
            Pointer oldValue = null;
            try {
                HashEntry<Pointer> e;
                for (e = entryForHash(this, hash); e != null; e = e.next) {

                    MdbPointer mp = (MdbPointer) e.value;
                    byte[] nkey = (byte[]) key;
                    String sizeKey = mdbManager.getSizeType(mp.getLength());
                    DBAssembleManager manager = mdbManager.getDBAssembleManager(sizeKey);
                    boolean verify = manager.verifyKey(nkey, mp);
                    if (e.hash == hash && verify) {
                        oldValue = e.value;
                        e.value = value;
                        ++modCount;
                        break;
                    }
                }
            } finally {
                unlock();
            }
            return oldValue;
        }

        final void clear() {
            lock();
            try {
                HashEntry<Pointer>[] tab = table;
                for (int i = 0; i < tab.length; i++)
                    setEntryAt(tab, i, null);
                ++modCount;
                count = 0;
            } finally {
                unlock();
            }
        }
    }

    @SuppressWarnings("unchecked")
    static final <K, V> HashEntry<V> entryForHash(Segment<K, V> seg, int h) {
        HashEntry<V>[] tab;
        return (seg == null || (tab = seg.table) == null) ? null : (HashEntry<V>) UNSAFE
            .getObjectVolatile(tab, ((long) (((tab.length - 1) & h)) << TSHIFT) + TBASE);
    }

    static final class HashEntry<Pointer> {
        final int hash;
        volatile Pointer value;
        volatile HashEntry<Pointer> next;

        HashEntry(int hash, Pointer value, HashEntry<Pointer> next) {
            this.hash = hash;
            this.value = value;
            this.next = next;
        }

        /**
         * Sets next field with volatile write semantics.  (See above
         * about use of putOrderedObject.)
         */
        final void setNext(HashEntry<Pointer> n) {
            UNSAFE.putOrderedObject(this, nextOffset, n);
        }

        // Unsafe mechanics
        static final sun.misc.Unsafe UNSAFE;
        static final long nextOffset;
        static {
            try {
                //UNSAFE = sun.misc.Unsafe.getUnsafe();
                Field uField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                uField.setAccessible(true);
                UNSAFE = (sun.misc.Unsafe) uField.get(null);
                Class k = HashEntry.class;
                nextOffset = UNSAFE.objectFieldOffset(k.getDeclaredField("next"));
            } catch (Exception e) {
                throw new Error(e);
            }
        }
    }

    // Unsafe mechanics
    private static final sun.misc.Unsafe UNSAFE;
    private static final long SBASE;
    private static final int SSHIFT;
    private static final long TBASE;
    private static final int TSHIFT;
    /*private static final long HASHSEED_OFFSET;*/
    private static final long SEGSHIFT_OFFSET;
    private static final long SEGMASK_OFFSET;
    private static final long SEGMENTS_OFFSET;

    static {
        int ss, ts;
        try {
            //UNSAFE = sun.misc.Unsafe.getUnsafe();
            Field uField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            uField.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) uField.get(null);

            Class tc = HashEntry[].class;
            Class sc = Segment[].class;
            TBASE = UNSAFE.arrayBaseOffset(tc);
            SBASE = UNSAFE.arrayBaseOffset(sc);
            ts = UNSAFE.arrayIndexScale(tc);
            ss = UNSAFE.arrayIndexScale(sc);
            /*   HASHSEED_OFFSET = UNSAFE.objectFieldOffset(
                   ConcurrentHashMap.class.getDeclaredField("hashSeed"));*/
            SEGSHIFT_OFFSET = UNSAFE.objectFieldOffset(OffHeapMap.class.getDeclaredField("segmentShift"));
            SEGMASK_OFFSET = UNSAFE.objectFieldOffset(OffHeapMap.class.getDeclaredField("segmentMask"));
            SEGMENTS_OFFSET = UNSAFE.objectFieldOffset(OffHeapMap.class.getDeclaredField("segments"));
        } catch (Exception e) {
            throw new Error(e);
        }
        if ((ss & (ss - 1)) != 0 || (ts & (ts - 1)) != 0)
            throw new Error("data type scale not a power of two");
        SSHIFT = 31 - Integer.numberOfLeadingZeros(ss);
        TSHIFT = 31 - Integer.numberOfLeadingZeros(ts);
    }

    public void setMdbManager(MdbManager mdbManager) {
        this.mdbManager = mdbManager;
    }

    @Override
    public boolean isEmpty() {
        /*
         * Sum per-segment modCounts to avoid mis-reporting when
         * elements are concurrently added and removed in one segment
         * while checking another, in which case the table was never
         * actually empty at any point. (The sum ensures accuracy up
         * through at least 1<<31 per-segment modifications before
         * recheck.)  Methods size() and containsValue() use similar
         * constructions for stability checks.
         */
        long sum = 0L;
        final Segment<K, Pointer>[] segments = this.segments;
        for (int j = 0; j < segments.length; ++j) {
            Segment<K, Pointer> seg = segmentAt(segments, j);
            if (seg != null) {
                if (seg.count != 0)
                    return false;
                sum += seg.modCount;
            }
        }
        if (sum != 0L) { // recheck unless no modifications
            for (int j = 0; j < segments.length; ++j) {
                Segment<K, Pointer> seg = segmentAt(segments, j);
                if (seg != null) {
                    if (seg.count != 0)
                        return false;
                    sum -= seg.modCount;
                }
            }
            if (sum != 0L)
                return false;
        }
        return true;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof byte[]) {
            Segment<K, Pointer> s; // same as get() except no need for volatile value read
            HashEntry<Pointer>[] tab;
            int h = hash(key);
            long u = (((h >>> segmentShift) & segmentMask) << SSHIFT) + SBASE;
            if ((s = (Segment<K, Pointer>) UNSAFE.getObjectVolatile(segments, u)) != null && (tab = s.table) != null) {
                for (HashEntry<Pointer> e = (HashEntry<Pointer>) UNSAFE.getObjectVolatile(tab, ((long) (((tab.length - 1) & h)) << TSHIFT) + TBASE); e != null; e = e.next) {

                    MdbPointer mp = (MdbPointer) e.value;
                    byte[] nKey = (byte[]) key;
                    String sizeKey = mdbManager.getSizeType(mp.getLength());
                    DBAssembleManager manager = mdbManager.getDBAssembleManager(sizeKey);

                    boolean verify = manager.verifyKey(nKey, mp);
                    if (e.hash == h && verify) {
                        return true;
                    }
                }
            }
            return false;
        } else {
            return false;
        }
    }

    @Override
    @Deprecated
    public Set<K> keySet() {
        throw new UnsupportedOperationException("keySet() | This method not suppouted in this version!");
    }

    @Override
    @Deprecated
    public Collection<Pointer> values() {
        throw new UnsupportedOperationException("values() | This method not suppouted in this version!");
    }

    @Override
    @Deprecated
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("containsValue(Object value) | This method not suppouted in this version!");
    }

    @Override
    public void clear() {
        final Segment<K, Pointer>[] segments = this.segments;
        for (int j = 0; j < segments.length; ++j) {
            Segment<K, Pointer> s = segmentAt(segments, j);
            if (s != null)
                s.clear();
        }
    }
}
