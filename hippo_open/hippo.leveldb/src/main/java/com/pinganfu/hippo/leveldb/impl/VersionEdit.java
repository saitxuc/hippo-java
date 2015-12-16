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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.pinganfu.hippo.leveldb.cluster.BucketMetaData;
import com.pinganfu.hippo.leveldb.util.DynamicSliceOutput;
import com.pinganfu.hippo.leveldb.util.Slice;
import com.pinganfu.hippo.leveldb.util.SliceInput;
import com.pinganfu.hippo.leveldb.util.VariableLengthQuantity;

public class VersionEdit {
	private String comparatorName;
	private Long logNumber;
	private Long nextFileNumber;
	private Long previousLogNumber;
	private Long lastSequenceNumber;
	private final Map<Integer, InternalKey> compactPointers = Maps.newTreeMap();
	private final Multimap<Integer, FileMetaData> newFiles = ArrayListMultimap.create();
	private final Multimap<Integer, Long> deletedFiles = ArrayListMultimap.create();
	// bucket
	private final Multimap<Integer, BucketMetaData> deletedBFiles = HashMultimap.create();
	private final Multimap<Integer, BucketMetaData> newBFiles = HashMultimap.create();

	public VersionEdit() {
	}

	public VersionEdit(Slice slice) {
		SliceInput sliceInput = slice.input();
		while (sliceInput.isReadable()) {
			int i = VariableLengthQuantity.readVariableLengthInt(sliceInput);
			VersionEditTag tag = VersionEditTag.getValueTypeByPersistentId(i);
			tag.readValue(sliceInput, this);
		}
	}

	public String getComparatorName() {
		return comparatorName;
	}

	public void setComparatorName(String comparatorName) {
		this.comparatorName = comparatorName;
	}

	public Long getLogNumber() {
		return logNumber;
	}

	public void setLogNumber(long logNumber) {
		this.logNumber = logNumber;
	}

	public Long getNextFileNumber() {
		return nextFileNumber;
	}

	public void setNextFileNumber(long nextFileNumber) {
		this.nextFileNumber = nextFileNumber;
	}

	public Long getPreviousLogNumber() {
		return previousLogNumber;
	}

	public void setPreviousLogNumber(long previousLogNumber) {
		this.previousLogNumber = previousLogNumber;
	}

	public Long getLastSequenceNumber() {
		return lastSequenceNumber;
	}

	public void setLastSequenceNumber(long lastSequenceNumber) {
		this.lastSequenceNumber = lastSequenceNumber;
	}

	public Map<Integer, InternalKey> getCompactPointers() {
		return ImmutableMap.copyOf(compactPointers);
	}

	public void setCompactPointer(int level, InternalKey key) {
		compactPointers.put(level, key);
	}

	public void setCompactPointers(Map<Integer, InternalKey> compactPointers) {
		this.compactPointers.putAll(compactPointers);
	}

	public Multimap<Integer, FileMetaData> getNewFiles() {
		return ImmutableMultimap.copyOf(newFiles);
	}

	public Multimap<Integer, BucketMetaData> getNewBFiles() {
		return ImmutableMultimap.copyOf(newBFiles);
	}

	// Add the specified file at the specified level.
	// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
	// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
	public void addFile(int level, long fileNumber, long fileSize, InternalKey smallest, InternalKey largest,
			Set<Integer> buckets) {

		FileMetaData fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest, buckets);
		addFile(level, fileMetaData);
	}

	public void addFile(int level, FileMetaData fileMetaData) {
		newFiles.put(level, fileMetaData);

		// 添加bucket
		Set<Integer> buckets = fileMetaData.getBuckets();
		for (Integer b : buckets) {
			addBfile(b, level, fileMetaData.getNumber());
		}
	}

	private void addBfile(int bucket, int level, long fileNumber) {
		BucketMetaData bucketMetaData = new BucketMetaData(bucket, level, fileNumber);
		addBfile(level, bucketMetaData);
	}

	private void addBfile(int level, BucketMetaData bucketMetaData) {
		newBFiles.put(level, bucketMetaData);
	}

	public void addFiles(Multimap<Integer, FileMetaData> files) {
		newFiles.putAll(files);
	}

	public Multimap<Integer, Long> getDeletedFiles() {
		return ImmutableMultimap.copyOf(deletedFiles);
	}
	
	public Multimap<Integer, BucketMetaData> getDeletedBFiles() {
		return ImmutableMultimap.copyOf(deletedBFiles);
	}

	// Delete the specified "file" from the specified "level".
	public void deleteFile(int level, long fileNumber) {
		deletedFiles.put(level, fileNumber);
		
		//删除bucket
		Collection<BucketMetaData> bFiles = newBFiles.asMap().get(level);
		if (bFiles != null && bFiles.size() > 0) {
			for (BucketMetaData bucketMetaData : bFiles) {
				if (fileNumber == bucketMetaData.getFileNumber()) {
					deletedBFiles.put(level, bucketMetaData);
				}
			}
		}
	}

	public Slice encode() {
		DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(4096);
		for (VersionEditTag versionEditTag : VersionEditTag.values()) {
			versionEditTag.writeValue(dynamicSliceOutput, this);
		}
		return dynamicSliceOutput.slice();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("VersionEdit");
		sb.append("{comparatorName='").append(comparatorName).append('\'');
		sb.append(", logNumber=").append(logNumber);
		sb.append(", previousLogNumber=").append(previousLogNumber);
		sb.append(", lastSequenceNumber=").append(lastSequenceNumber);
		sb.append(", compactPointers=").append(compactPointers);
		sb.append(", newFiles=").append(newFiles);
		sb.append(", deletedFiles=").append(deletedFiles);
		sb.append('}');
		return sb.toString();
	}
	
}