/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinganfu.hippo.leveldb.impl;

import static com.google.common.base.Charsets.UTF_8;
import static com.pinganfu.hippo.leveldb.cluster.BConstansts.KEY_EXPIRE_TIME;
import static com.pinganfu.hippo.leveldb.util.SizeOf.SIZE_OF_LONG;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.pinganfu.hippo.leveldb.ReadOptions;
import com.pinganfu.hippo.leveldb.WriteOptions;
import com.pinganfu.hippo.leveldb.util.Slice;
import com.pinganfu.hippo.leveldb.util.SliceOutput;
import com.pinganfu.hippo.leveldb.util.Slices;

public class InternalKey {
	// 格式为bucketNo + appNo + keylen + userkey + version + expires time
	private final Slice userKey;
	private final long sequenceNumber;
	private final ValueType valueType;

	public InternalKey(Slice userKey, long sequenceNumber, ValueType valueType) {
		Preconditions.checkNotNull(userKey, "userKey is null");
		Preconditions.checkArgument(sequenceNumber >= 0, "sequenceNumber is negative");
		Preconditions.checkNotNull(valueType, "valueType is null");

		this.userKey = userKey;
		this.sequenceNumber = sequenceNumber;
		this.valueType = valueType;
	}

	public InternalKey(Slice data) {
		Preconditions.checkNotNull(data, "data is null");
		Preconditions.checkArgument(data.length() >= SIZE_OF_LONG, "data must be at least %s bytes", SIZE_OF_LONG);
		this.userKey = getUserKey(data);
		long packedSequenceAndType = data.getLong(data.length() - SIZE_OF_LONG);
		this.sequenceNumber = SequenceNumber.unpackSequenceNumber(packedSequenceAndType);
		this.valueType = SequenceNumber.unpackValueType(packedSequenceAndType);
	}

	public InternalKey(byte[] data) {
		this(Slices.wrappedBuffer(data));
	}

	public Slice getUserKey() {
		return userKey;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public ValueType getValueType() {
		return valueType;
	}

	public Slice encode() {
		Slice slice = Slices.allocate(userKey.length() + SIZE_OF_LONG);
		SliceOutput sliceOutput = slice.output();
		sliceOutput.writeBytes(userKey);
		sliceOutput.writeLong(SequenceNumber.packSequenceAndValueType(sequenceNumber, valueType));
		return slice;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		InternalKey that = (InternalKey) o;

		if (sequenceNumber != that.sequenceNumber) {
			return false;
		}
		if (userKey != null ? !userKey.equals(that.userKey) : that.userKey != null) {
			return false;
		}
		if (valueType != that.valueType) {
			return false;
		}

		return true;
	}

	private int hash = 0;

	@Override
	public int hashCode() {
		if (hash == 0) {
			int result = userKey != null ? userKey.hashCode() : 0;
			result = 31 * result + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
			result = 31 * result + (valueType != null ? valueType.hashCode() : 0);
			if (result == 0) {
				result = 1;
			}
			hash = result;
		}
		return hash;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("InternalKey");
		sb.append("{bucket=").append(bucket());
		sb.append(", bizApp=").append(bizApp());
		sb.append(", key=").append(new String(realKey(), UTF_8)); // todo don't print the real
		sb.append(", version=").append(version() >= 0 ? version() : "");
		// sb.append(", expireTime=").append(getExpireTime());
		sb.append(", sequenceNumber=").append(getSequenceNumber());
		sb.append(", valueType=").append(getValueType());
		sb.append('}');
		return sb.toString();
	}

	// todo find new home for these

	public static final Function<InternalKey, Slice> INTERNAL_KEY_TO_SLICE = new InternalKeyToSliceFunction();

	public static final Function<Slice, InternalKey> SLICE_TO_INTERNAL_KEY = new SliceToInternalKeyFunction();

	public static final Function<InternalKey, Slice> INTERNAL_KEY_TO_USER_KEY = new InternalKeyToUserKeyFunction();

	public static Function<Slice, InternalKey> createUserKeyToInternalKeyFunction(final long sequenceNumber) {
		return new UserKeyInternalKeyFunction(sequenceNumber);
	}

	private static class InternalKeyToSliceFunction implements Function<InternalKey, Slice> {
		@Override
		public Slice apply(InternalKey internalKey) {
			return internalKey.encode();
		}
	}

	private static class InternalKeyToUserKeyFunction implements Function<InternalKey, Slice> {
		@Override
		public Slice apply(InternalKey internalKey) {
			return internalKey.getUserKey();
		}
	}

	private static class SliceToInternalKeyFunction implements Function<Slice, InternalKey> {
		@Override
		public InternalKey apply(Slice bytes) {
			return new InternalKey(bytes);
		}
	}

	private static class UserKeyInternalKeyFunction implements Function<Slice, InternalKey> {
		private final long sequenceNumber;

		public UserKeyInternalKeyFunction(long sequenceNumber) {
			this.sequenceNumber = sequenceNumber;
		}

		@Override
		public InternalKey apply(Slice userKey) {
			return new InternalKey(userKey, sequenceNumber, ValueType.VALUE);
		}
	}

	private static Slice getUserKey(Slice data) {
		return data.slice(0, data.length() - SIZE_OF_LONG);
	}

	public boolean isSameBucket(InternalKey other) {
		if (other == null)
			return false;

		return userKey.getInt(0) == other.getUserKey().getInt(0);
	}

	public int bucket() {
		return userKey.getShort(0);
	}

	public int bizApp() {
		return userKey.getShort(2);
	}

	private int keyLength() {
		return userKey.getShort(4);
	}

	public byte[] realKey() {
		return userKey.getBytes(6, keyLength());
	}

	public int version() {
		if (!simplified()) {
			return userKey.getShort(6 + keyLength());
		}
		return -1;
	}

	public long expireTime() {
		return userKey.getLong(8 + keyLength());
	}

	public boolean expire() {
		long expireTime = expireTime();
		if (expireTime == KEY_EXPIRE_TIME) {
			return false;
		}

		return expireTime <= System.currentTimeMillis();
	}

	/**
	 * 用于写
	 * @return
	 */
	public static Slice packageKey_w(byte[] key, WriteOptions options) {
		Slice slice = Slices.allocate(16 + key.length);
		SliceOutput sliceOutput = slice.output();
		sliceOutput.writeShort(options.bucket());
		sliceOutput.writeShort(options.bizApp());
		sliceOutput.writeShort(key.length);
		sliceOutput.writeBytes(key);
		sliceOutput.writeShort(options.version());
		sliceOutput.writeLong(options.expireTime());
		return slice;
	}

	/**
	 * 用于删除
	 * @return
	 */
	public static Slice packageKey_d(byte[] key, WriteOptions options) {
		Slice slice = Slices.allocate(8 + key.length);
		SliceOutput sliceOutput = slice.output();
		sliceOutput.writeShort(options.bucket());
		sliceOutput.writeShort(options.bizApp());
		sliceOutput.writeShort(key.length);
		sliceOutput.writeBytes(key);
		sliceOutput.writeShort(options.version());
		return slice;
	}

	/**
	 * 用于查询
	 * @return
	 */
	public static Slice packageKey_q(byte[] key, ReadOptions options) {
		Slice slice = Slices.allocate(8 + key.length);
		SliceOutput sliceOutput = slice.output();
		sliceOutput.writeShort(options.bucket());
		sliceOutput.writeShort(options.bizApp());
		sliceOutput.writeShort(key.length);
		sliceOutput.writeBytes(key);
		sliceOutput.writeShort(options.version());
		return slice;
	}

	/**
	 * 剔除掉version和expireTime这些变化的字段
	 * @return
	 */
	private static Slice packageKey_s(byte[] key, ReadOptions options) {
		Slice slice = Slices.allocate(6 + key.length);
		SliceOutput sliceOutput = slice.output();
		sliceOutput.writeShort(options.bucket());
		sliceOutput.writeShort(options.bizApp());
		sliceOutput.writeShort(key.length);
		sliceOutput.writeBytes(key);
		return slice;
	}

	public boolean simplified() {
		return userKey.length() == 6 + keyLength();
	}
	
	public boolean original() {
		if (userKey.length() < 6) {
			return false;
		} else if (userKey.length() < 6 + keyLength() + 2) {
			return false;
		} else {
			return true;
		}
	}

	public InternalKey simplify() {
		return new InternalKey(packageKey_s(realKey(), new ReadOptions().bucket(bucket()).bizApp(bizApp())), sequenceNumber,
				valueType);
	}
}
