package com.pinganfu.hippo.leveldb.cluster;


/**
 * 桶和文件的映射关系
 * @author yangxin
 */
public class BucketMetaData {
	private int bucket;
	private int level;
	private long fileNumber;

	public BucketMetaData(int bucket, int level, long fileNumber) {
		this.bucket = bucket;
		this.level = level;
		this.fileNumber = fileNumber;
	}

	public int getBucket() {
		return bucket;
	}

	public void setBucket(int bucket) {
		this.bucket = bucket;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public long getFileNumber() {
		return fileNumber;
	}

	public void setFileNumber(long fileNumber) {
		this.fileNumber = fileNumber;
	}

	@Override
	public int hashCode() {
		int result = 17;
		result = 31 * result + bucket;
		result = 31 * result + level;
		result = 31 * result + (int) (fileNumber ^ (fileNumber >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;

		if (obj == null)
			return false;

		if (!(obj instanceof BucketMetaData))
			return false;

		final BucketMetaData bucketMetaData = (BucketMetaData) obj;
		if (bucketMetaData.getBucket() != bucket || bucketMetaData.getLevel() != level
				|| bucketMetaData.getFileNumber() != fileNumber)
			return false;

		return true;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("BucketMetaData");
		sb.append("{bucket=").append(bucket);
		sb.append(", level=").append(level);
		sb.append(", fileNumber=").append(fileNumber);
		sb.append('}');
		return sb.toString();
	}

}
