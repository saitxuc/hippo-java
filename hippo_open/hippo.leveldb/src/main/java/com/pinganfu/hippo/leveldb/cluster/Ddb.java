package com.pinganfu.hippo.leveldb.cluster;

import static com.google.common.base.Charsets.UTF_8;
import static com.pinganfu.hippo.leveldb.cluster.Ddb.FileType.LOG;
import static com.pinganfu.hippo.leveldb.cluster.Ddb.FileType.SST;
import static com.pinganfu.hippo.leveldb.impl.ValueType.DELETION;
import static com.pinganfu.hippo.leveldb.impl.ValueType.VALUE;
import static com.pinganfu.hippo.leveldb.util.Slices.readLengthPrefixedBytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.pinganfu.hippo.leveldb.DBException;
import com.pinganfu.hippo.leveldb.Options;
import com.pinganfu.hippo.leveldb.Snapshot;
import com.pinganfu.hippo.leveldb.WriteOptions;
import com.pinganfu.hippo.leveldb.impl.DbImpl;
import com.pinganfu.hippo.leveldb.impl.FileMetaData;
import com.pinganfu.hippo.leveldb.impl.Filename;
import com.pinganfu.hippo.leveldb.impl.InternalKey;
import com.pinganfu.hippo.leveldb.impl.LogMonitor;
import com.pinganfu.hippo.leveldb.impl.SnapshotImpl;
import com.pinganfu.hippo.leveldb.impl.ValueType;
import com.pinganfu.hippo.leveldb.impl.Version;
import com.pinganfu.hippo.leveldb.impl.VersionSet;
import com.pinganfu.hippo.leveldb.impl.WriteBatchImpl;
import com.pinganfu.hippo.leveldb.table.BlockEntry;
import com.pinganfu.hippo.leveldb.util.InternalTableIterator;
import com.pinganfu.hippo.leveldb.util.Slice;
import com.pinganfu.hippo.leveldb.util.SliceInput;
import com.pinganfu.hippo.leveldb.util.Slices;

/**
 * @author yangxin
 */
public class Ddb extends DbImpl {
	private final static Logger logger = LoggerFactory.getLogger(Ddb.class);
	/**
	 * 日志文件Reader
	 * key:standby + "#" + bucket + "#" + fileNumber
	 */
	private final Map<String, LogReader0> logCache;
	
	/**
	 *  缓存每个bucket当前最大的seq，最多维持128个桶, 
	 *  当服务重启时，有可能导致某些桶的seq无法加载，比如某个桶的最新数据不在历史的log文件中，因为变量没有持久化存储
	 */
	private final LruLinkedHashMap<Integer, Long> seqs = new LruLinkedHashMap<Integer, Long>(128);
	public final static Integer GLOBAL_BUCKET = Integer.valueOf(-1);
	
	public Ddb(Options options, File databaseDir) throws IOException {
		super(options, databaseDir);
		seqs.put(GLOBAL_BUCKET, versions.getLastSequence());

		this.logCache = new ChannelCache<String, LogReader0>().removalListener(new RemovalListener<String, LogReader0>() {
			public void onRemoval(String fileNumber, LogReader0 logReader) {
				if (logReader != null) {
					logReader.close();
				}
				// log
			}
		}).loader(new CacheLoader<String, LogReader0>() {
			public LogReader0 load(String key) {
				File file = new File(Ddb.this.databaseDir, Filename.logFileName(Long.parseLong(key.split("#")[2])));
				FileChannel channel = null;
				try {
					channel = new FileInputStream(file).getChannel();
					LogMonitor logMonitor = new LogMonitor() {
						
						@Override
						public void corruption(long bytes, Throwable reason) {
							if (logger.isDebugEnabled()) {
								logger.debug(String.format("corruption of %s bytes", bytes), reason);
							}
						}
						
						@Override
						public void corruption(long bytes, String reason) {
							if (logger.isDebugEnabled()) {
								logger.debug("corruption of {} bytes: {}", bytes, reason);
							}
						}
					};
					return new LogReader0(channel, logMonitor, true, 0);
				} catch (FileNotFoundException e) {
					throw new IfileNotFoundException("Log file does not exist.", e);
				} catch (Exception e) {
					if (channel != null) {
						try {
							channel.close();
						} catch (IOException ioe) {
							ioe.printStackTrace();
						}
					}
					return null;
				}
			}
		});
	}
	
	/**
	 * 文件不存在
	 */
	static public class IfileNotFoundException extends RuntimeException {
		// inner use
		private static final long serialVersionUID = 1L;

		public IfileNotFoundException() {
			super();
		}

		public IfileNotFoundException(String s) {
			super(s);
		}
		
		public IfileNotFoundException(String message, Throwable cause) {
			super(message, cause);
		}
	}
	
	@Override
	public Snapshot writeInternal(WriteBatchImpl updates, WriteOptions options) throws DBException {
		checkBackgroundException();
		mutex.lock();
		try {
			long sequenceEnd;
			if (updates.size() != 0) {
				makeRoomForWrite(false);

				// Get sequence numbers for this change set
				final long sequenceBegin = versions.getLastSequence() + 1;
				sequenceEnd = sequenceBegin + updates.size() - 1;

				// Reserve this sequence in the version set
				versions.setLastSequence(sequenceEnd);

				// Log write
				Slice record = writeWriteBatch(updates, sequenceBegin);
				try {
					log.addRecord(record, options.sync());
				} catch (IOException e) {
					throw Throwables.propagate(e);
				}

				// Update memtable
				updates.forEach(new InsertIntoHandler(memTable, sequenceBegin));
			} else {
				sequenceEnd = versions.getLastSequence();
			}

			seqs.put(options.bucket(), sequenceEnd);
			seqs.put(GLOBAL_BUCKET, sequenceEnd);//全局seq
			
			if (options.snapshot()) {
				return new SnapshotImpl(versions.getCurrent(), sequenceEnd);
			} else {
				return null;
			}
		} finally {
			mutex.unlock();
		}
	}

	public long getMaxSeq(int bucket) {
		Long seq = seqs.get(bucket);
		if (seq != null) {
			return seq;
		} else {
			return -1;
		}
	}
	
	@Override
	protected void maybeDeleteCachedLogFiles(long fileNumber) {
		if (logCache == null) {
			return;
		}

		Iterator<Entry<String, LogReader0>> its = logCache.entrySet().iterator();
		while (its.hasNext()) {
			Entry<String, LogReader0> e = its.next();
			if (e.getKey().endsWith("#" + String.valueOf(fileNumber))) {
				its.remove();
			}
		}
	}
	
	/**
	 * 获取小于指定批次大小的数据，
	 * 该方法会修改参数属性<code>Position</code>
	 * @return
	 */
	public Entrys get(String standby, int bucket, Position pos, int batchSize) {
		Entrys entrys = new Entrys();
		
		if (SST.equals(pos.fileType())) {
			Version v = null;
			if (!pos.hasVersion()) {
				v = versions.getCurrent();
				v.retain();
				pos.version(v.hashCode());
			} else {
				Set<Version> active = versions.activeVersions();
				if (active != null) {
					for (Version av : active) {
						if (av.hashCode() == pos.version()) {
							v = av;
						}
					}
				}
			}

			if (v == null) {
				throw new IfileNotFoundException("There is no corresponding version, bucket= " + bucket + " Hashcode=" + pos.version());
			}
			FileQueue queue = new FileQueue(bucket, v, pos.maxFileNumber());
			readSst(standby, bucket, pos, batchSize, v, queue, entrys, true, 1);
		} else if (LOG.equals(pos.fileType())) {
			readLog(standby, bucket, pos, batchSize, entrys, true, 1);
			if (entrys.size() < batchSize) {
				pos.hasNext(false);
			}
		} else {
			//TODO
		}
		return entrys;
	}

	private void readLog(String standby, int bucket, Position pos, int batchSize, 
			Entrys entrys, boolean availableRollback, int deep) {
		if (deep > 50) {
			return;
		}
		
		long fileNumber = pos.fileNumber();
		if (!logQ.contains(fileNumber)) {
			throw new IfileNotFoundException(bucket + "-bucket log file[" + fileNumber + "] does not exist!");
		}

		String key = generateKey(standby, bucket, fileNumber);
		LogReader0 logReader = logCache.get(key);
		if (logReader == null) {
			return;
		}
		
		if (availableRollback) {
			if (pos.pointer() > 0) {
				logger.warn("{}-bucket log happen rollback, position={}.", bucket, pos);
				logReader.setPosition(pos.pointer());
			} else {
				// 设置回滚点
				pos.pointer(logReader.pointer());
			}
		}

		boolean full = false;
		for (Slice record = logReader.readRecord(); record != null; record = logReader.readRecord()) {
			SliceInput sliceInput = record.input();
			// read header
			if (sliceInput.available() < 12) {
				logReader.reportCorruption(sliceInput.available(), "log record too small");
				continue;
			}
			long sequenceBegin = sliceInput.readLong();
			int updateSize = sliceInput.readInt();

			// read entries
			try {
				int c = readWriteBatch(bucket, sliceInput, updateSize, entrys);
				if (c < 1) {
					continue;
				}
			} catch (IOException e) {
				Throwables.propagate(e);
			}

			// update the maxSequence
			pos.curMaxSeq(sequenceBegin + updateSize - 1);
			pos.maxSeq(pos.curMaxSeq());
		
			if (entrys.size() >= batchSize) {
				full = true;
				break;
			}
		}
		
		
		if (!full && logReader.eof(logsOffset.get(fileNumber))) {
			boolean next = false;
			for (long n : logQ) {
				if (next) {
					pos.fileNumber(n);
					break;
				}
				if (n == fileNumber) {
					next = true;
				}
			}

			if (fileNumber != pos.fileNumber()) {
				if (entrys.size() < batchSize) {
					readLog(standby, bucket, pos, batchSize, entrys, false, ++deep);
				}
			}
		}

	}

	private int readWriteBatch(int bucket, SliceInput record, int updateSize, Entrys entries) throws IOException {
		List<Entry<Slice, Slice>> list = Lists.newArrayListWithCapacity(updateSize);
		int count = 0;
		while (record.isReadable()) {
			count++;
			ValueType valueType = ValueType.getValueTypeByPersistentId(record.readByte());
			if (valueType == VALUE) {
				Slice key = readLengthPrefixedBytes(record);
				Slice value = readLengthPrefixedBytes(record);
				list.add(new BlockEntry(key, value));
			} else if (valueType == DELETION) {
				Slice key = readLengthPrefixedBytes(record);
				list.add(new BlockEntry(key, Slices.EMPTY_SLICE));
			} else {
				throw new IllegalStateException("Unexpected value type " + valueType);
			}
		}

		if (count != updateSize) {
			throw new IOException(String.format("Expected %d entries in log record but found %s entries", updateSize, entries));
		}
		
		int available = 0;
		for (Entry<Slice, Slice> e : list) {
			InternalKey iKey = new InternalKey(e.getKey(), 0, e.getValue().length() != 0 ? VALUE : DELETION);
			if (bucket == iKey.bucket()) {
				entries.add(e);
				available++;
			}
		}
		return available;
	}
	
	private void readSst(String standby, int bucket, Position pos, int batchSize, Version v, 
			FileQueue queue, Entrys entrys, boolean availableRollback, int deep) {
		// 防止调用超时，和栈溢出异常
		if (deep > 50) {
			return;
		}
		
		FileMetaData file = null;
		if (pos.fileNumber() < 0) {
			file = queue.poll();
		} else {
			file = queue.poll(pos.fileNumber());
		}

		// 当file为null时，意味着当前的version所有sst文件已经迁移完成，
		// 如果存在新的version，就切换到新的version，否则切换到log模式，开始去拉去log文件的数据
		if (file == null) {
			v.release();

			pos.switchToNew();
			Version snapshot = versions.getCurrent();
			if (v != snapshot) {
				v = snapshot;
				v.retain();
				pos.version(v.hashCode());
				pos.fileNumber(-1);
				readSst(standby, bucket, pos, batchSize, v, new FileQueue(bucket, v, pos.maxFileNumber()), entrys, availableRollback, ++deep);
			} else {
				// 日志模式不锁定version,只锁定log文件
				// log文件的锁定逻辑见DBImpl的logQ属性
				pos.fileNumber(log.getFileNumber());
				pos.pointer(0);
				pos.fileType(LOG);
			}

			return;
		} else {
			pos.fileNumber(file.getNumber());
		}

		// 读取大于maxSeq的记录
		String key = generateKey(standby, bucket);
		SlaveIterator si = v.iterators.get(key);
		if (si == null) {
			si = new SlaveIterator(file);
		} else {
			if (si.getFileNumber() != file.getNumber()) {
				si = new SlaveIterator(file);
			}
		}
		v.iterators.put(key, si);
		
		if (availableRollback) {
			if (pos.pointer() > 0) {
				logger.warn("{}-bucket sst happen rollback, position={}.", bucket, pos);
				si.seekToFirst();
			} else {
				pos.pointer(file.getNumber());
			}
		}

		long maxSeq = pos.maxSeq();
		while (si.hasNext()) {
			Entry<InternalKey, Slice> e = si.next(bucket, maxSeq);
			if (e != null) {
				InternalKey ikey = e.getKey();
				pos.curMaxSeq(ikey.getSequenceNumber());
				entrys.add(new BlockEntry(ikey.getUserKey(), ValueType.VALUE.equals(ikey.getValueType()) ? e.getValue() : Slices.EMPTY_SLICE));
			}
			if (entrys.size() > batchSize) {
				break;
			}
		}

		// 判断文件是否已经读取完成
		if (!si.hasNext()) {
			v.iterators.remove(key);
			FileMetaData fmd = queue.peek();
			if (fmd != null) {
				pos.fileNumber(fmd.getNumber());
			} else {
				pos.fileNumber(pos.curMaxFileNumber + 1);// +1意味着该version不存在该文件
			}
		}

		// 继续读取下一个文件的数据
		if (entrys.size() < batchSize) {
			readSst(standby, bucket, pos, batchSize, v, queue, entrys, false, ++deep);
		}
	}

	private String generateKey(String standby, int bucket) {
		return standby + "#" + bucket;
	}

	private String generateKey(String standby, int bucket, long fileNumber) {
		return standby + "#" + bucket + "#" + fileNumber;
	}

	static class ChannelCache<K, V> extends ConcurrentHashMap<K, V> {
		private static final long serialVersionUID = -3475088096430710901L;

		private CacheLoader<K, V> loader;
		private RemovalListener<K, V> removalListener;

		@SuppressWarnings("unchecked")
		@Override
		public synchronized V get(@Nonnull Object key) throws RuntimeException {
			V v = super.get(key);
			if (v == null) {
				try {
					v = loader.load((K) key);
					super.put((K) key, v);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			return v;
		}

		@SuppressWarnings("unchecked")
		@Override
		public synchronized V remove(Object key) {
			removalListener.onRemoval((K) key, get(key));
			return super.remove(key);
		}

		public ChannelCache<K, V> loader(CacheLoader<K, V> loader) {
			this.loader = loader;
			return this;
		}

		public ChannelCache<K, V> removalListener(RemovalListener<K, V> removalListener) {
			this.removalListener = removalListener;
			return this;
		}
	}

	static interface RemovalListener<K, V> {
		void onRemoval(K k, V v);
	}

	public void close() {
	}

	public class SlaveIterator {
		private InternalTableIterator iterator;
		private long fileNumber;

		public SlaveIterator(FileMetaData file) {
			this.fileNumber = file.getNumber();
			// open the iterator
			this.iterator = tableCache.newIterator(file);
		}

		public long getFileNumber() {
			return fileNumber;
		}

		public boolean hasNext() {
			return iterator.hasNext();
		}
		
		public void seekToFirst() {
			iterator.seekToFirst();
		}

		public Entry<InternalKey, Slice> next(int bucket, long minSeq) {
			if (hasNext()) {
				// parse the key in the block
				Entry<InternalKey, Slice> entry = iterator.next();
				InternalKey internalKey = entry.getKey();
				Preconditions.checkState(internalKey != null, "Corrupt key for %s", internalKey.getUserKey().toString(UTF_8));

				if (internalKey.bucket() == bucket && internalKey.getSequenceNumber() > minSeq) {
					return entry;
				}
			}
			return null;
		}
	}
	
	public static enum FileType {
		LOG, SST
	}

	
	@Override
	protected void fireSweep(int bucket) {
		seqs.remove(bucket);
	}

	/**
	 * 当前拉去位置的描述信息
	 */
	static public class Position implements Serializable {
		private static final long serialVersionUID = 5921696490192005455L;
		private int version = -1;
		private FileType fileType = FileType.SST;
		private long pointer = -1;
		private long maxSeq = -1;
		private volatile long curMaxSeq = -1;
		private long fileNumber = -1;
		private long maxFileNumber = -1;
		private volatile long curMaxFileNumber = -1;
		private boolean hasNext = true;

		public void switchToNew() {
			maxSeq = curMaxSeq;
			maxFileNumber = curMaxFileNumber;
			version = -1;
		}
		
		public void reset() {
			version = -1;
			fileType = FileType.SST;
			pointer = -1;
//			maxSeq = -1;//通过maxSeq过滤
			curMaxSeq = maxSeq;
			fileNumber = -1;
			maxFileNumber = -1;
			curMaxFileNumber = -1;
			hasNext = true;
		}
		
		public long curMaxSeq() {
			return curMaxSeq;
		}

		public void curMaxSeq(long q) {
			if (q > curMaxSeq) {
				this.curMaxSeq = q;
			}
		}

		public long maxSeq() {
			return maxSeq;
		}

		public void maxSeq(long maxSeq) {
			this.maxSeq = maxSeq;
		}

		public long maxFileNumber() {
			return maxFileNumber;
		}

		public boolean hasVersion() {
			return version != -1;
		}

		public int version() {
			return version;
		}

		public void version(int version) {
			this.version = version;
		}

		public long fileNumber() {
			return fileNumber;
		}

		public void fileNumber(long fileNumber) {
			if (fileNumber > curMaxFileNumber) {
				this.curMaxFileNumber = fileNumber;
			}
			this.fileNumber = fileNumber;
		}

		public long pointer() {
			return pointer;
		}

		public void pointer(long pointer) {
			this.pointer = pointer;
		}
		
		public FileType fileType() {
			return fileType;
		}

		public void fileType(FileType fileType) {
			this.fileType = fileType;
		}

		public boolean hasNext() {
			return hasNext;
		}

		public void hasNext(boolean hasNext) {
			this.hasNext = hasNext;
		}
		
		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("Position");
			sb.append("{version=").append(version);
			sb.append(", pointer=").append(pointer);
			sb.append(", maxSeq=").append(maxSeq);
			sb.append(", curMaxSeq=").append(curMaxSeq);
			sb.append(", fileNumber=").append(fileNumber);
			sb.append(", maxFileNumber=").append(maxFileNumber);
			sb.append(", curMaxFileNumber=").append(curMaxFileNumber);
			sb.append(", hasNext=").append(hasNext);
			sb.append('}');
			return sb.toString();
		}
	}

	private volatile long curCapacity = 0;
	private volatile Version snapshot;

	@Override
	protected void backgroundCompaction() throws IOException {
		super.backgroundCompaction();
		if (snapshot != versions.getCurrent()) {
			snapshot = versions.getCurrent();
			curCapacity = approximateSize();
		}
	}

	public long approximateSize() {
		Version version = versions.getCurrent();
		Multimap<Integer, FileMetaData> multimap = version.getFiles();
		Collection<FileMetaData> coll = multimap.values();
		long size = 0;
		size += ((long)coll.size() * VersionSet.TARGET_FILE_SIZE);

		// level0层的文件大小为#options.writeBufferSize()
		size += (version.numberOfFilesInLevel(0) * (options.writeBufferSize() - VersionSet.TARGET_FILE_SIZE));
		if (immutableMemTable != null)
			size += options.writeBufferSize();

		if (memTable != null) {
			size += memTable.approximateMemoryUsage();
		}
		return size;
	}

	public long getCurCapacity() {
		return curCapacity;
	}

	/**
	 * lru简单的实现
	 */
	public static class LruLinkedHashMap<K, V> extends AbstractMap<K, V> {

		private static final float hashTableLoadFactor = 0.75f;
		private LinkedHashMap<K, V> map;
		private int cacheSize;
		private Map<K, V> weakMap;

		public LruLinkedHashMap(int cacheSize) {
			this.cacheSize = cacheSize;
			int hashTableCapacity = (int) Math.ceil(cacheSize / hashTableLoadFactor) + 1;
			this.map = new LinkedHashMap<K, V>(hashTableCapacity, hashTableLoadFactor, true) {
				// (an anonymous inner class)  
				private static final long serialVersionUID = 1;

				@Override
				protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
					boolean remove = size() > LruLinkedHashMap.this.cacheSize;
					if (remove) {
						LruLinkedHashMap.this.weakMap.put(eldest.getKey(), eldest.getValue());
					}
					return remove;
				}
			};
			this.weakMap = new MapMaker().weakKeys().makeMap();
		}

		@Override
		public synchronized V get(Object key) {
			V val = map.get(key);
			if (val == null) {
				val = weakMap.get(key);
			}
			return val;
		}

		@Override
		public synchronized V put(K key, V value) {
			return map.put(key, value);
		}

		@Override
		public synchronized V remove(Object key) {
			weakMap.remove(key);
			return map.remove(key);
		}


		public synchronized void clear() {
			weakMap.clear();
			map.clear();
		}

		@Override
		public synchronized Set<java.util.Map.Entry<K, V>> entrySet() {
			Set<java.util.Map.Entry<K, V>> es = Sets.newHashSet(map.entrySet());
			es.addAll(weakMap.entrySet());
			return es;
		}

	}
	
}
