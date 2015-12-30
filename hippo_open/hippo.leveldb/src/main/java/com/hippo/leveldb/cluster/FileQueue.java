package com.hippo.leveldb.cluster;

import static com.hippo.leveldb.impl.DbConstants.NUM_LEVELS;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import com.hippo.leveldb.impl.FileMetaData;
import com.hippo.leveldb.impl.Version;

/**
 * @author yangxin
 */
public class FileQueue {
	private final int bucket;
	private final Queue<SstFile> sstFiles;
	private final Version snapshot;
	private final long maxFileNumber;

	public FileQueue(int bucket, Version snapshot, long maxFileNumber) {
		this.bucket = bucket;
		this.snapshot = snapshot;
		this.maxFileNumber = maxFileNumber;
		this.sstFiles = new PriorityQueue<SstFile>(1000);
		
		refresh();
	}

	public FileMetaData poll() {
		SstFile s = sstFiles.poll();
		if (s != null) {
			return s.getFileMetaData();
		}
		return null;
	}
	
	public FileMetaData peek() {
		SstFile s = sstFiles.peek();
		if (s != null) {
			return s.getFileMetaData();
		}
		return null;
	}

	public FileMetaData poll(long fileNumber) {
		while (sstFiles.size() > 0) {
			SstFile s = sstFiles.poll();
			if (fileNumber == s.getFileMetaData().getNumber()) {
				return s.getFileMetaData();
			}
		}
		return null;
	}
	
	public int size() {
		return sstFiles.size();
	}

	public void refresh() {
		for (int i = 0; i < NUM_LEVELS; i++) {
			List<FileMetaData> list = snapshot.getFiles(i);
			if (list != null) {
				for (FileMetaData fmd : list) {
					if (fmd.getBuckets().contains(bucket) && fmd.getNumber() > maxFileNumber) {
						SstFile f = new SstFile(i, fmd);
						if (!sstFiles.contains(f)) {
							sstFiles.add(f);
						}
					}
				}
			}
		}
	}

	/**
	 * 按照sst的生鲜度倒序排列</br>
	 * 对于不同层级的文件，从大到小排序，对相同层级的文件从小到大排序，比如:
	 * <p>
	 * level-0 sst1, level-0 sst3, level-2 sst2,排序之后的顺序为</br>
	 * level-2 sst2 -> level-0 sst1 -> level-0 sst1</br>
	 * </p>
	 */
	static public class SstFile implements Comparable<SstFile> {
		private final int level;
		private final FileMetaData fileMetaData;

		public SstFile(int level, FileMetaData fileMetaData) {
			this.level = level;
			this.fileMetaData = fileMetaData;
		}

		public int getLevel() {
			return level;
		}

		public FileMetaData getFileMetaData() {
			return fileMetaData;
		}

		@Override
		public int hashCode() {
			int result = 17;
			result = 31 * result + level;
			result = 31 * result
					+ (fileMetaData == null ? 0 : (int) (fileMetaData.getNumber() ^ (fileMetaData.getNumber() >>> 32)));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this)
				return true;

			if (obj == null)
				return false;

			if (!(obj instanceof SstFile))
				return false;

			final SstFile sstFile = (SstFile) obj;
			if (sstFile.getLevel() != level || sstFile.getFileMetaData().getNumber() != fileMetaData.getNumber())
				return false;

			return true;
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("SstFile");
			sb.append("{level=").append(level);
			sb.append(", fileMetaData=").append(fileMetaData);
			sb.append('}');
			return sb.toString();
		}

		@Override
		public int compareTo(SstFile that) {
			return -(int) (getLevel() - that.getLevel() > 0 ? 1 : (this.getLevel() - that.getLevel() < 0 ? -1 : that
					.getFileMetaData().getNumber() - this.getFileMetaData().getNumber()));
		}
	}
	
}
