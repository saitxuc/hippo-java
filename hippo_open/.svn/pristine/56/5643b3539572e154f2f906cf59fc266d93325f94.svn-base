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
package com.pinganfu.hippo.leveldb.cluster;

import static com.pinganfu.hippo.leveldb.impl.LogChunkType.BAD_CHUNK;
import static com.pinganfu.hippo.leveldb.impl.LogChunkType.EOF;
import static com.pinganfu.hippo.leveldb.impl.LogChunkType.UNKNOWN;
import static com.pinganfu.hippo.leveldb.impl.LogChunkType.ZERO_TYPE;
import static com.pinganfu.hippo.leveldb.impl.LogChunkType.getLogChunkTypeByPersistentId;
import static com.pinganfu.hippo.leveldb.impl.LogConstants.BLOCK_SIZE;
import static com.pinganfu.hippo.leveldb.impl.LogConstants.HEADER_SIZE;
import static com.pinganfu.hippo.leveldb.impl.Logs.getChunkChecksum;

import java.io.IOException;
import java.nio.channels.FileChannel;

import com.google.common.base.Throwables;
import com.pinganfu.hippo.leveldb.impl.LogChunkType;
import com.pinganfu.hippo.leveldb.impl.LogMonitor;
import com.pinganfu.hippo.leveldb.util.DynamicSliceOutput;
import com.pinganfu.hippo.leveldb.util.Slice;
import com.pinganfu.hippo.leveldb.util.SliceInput;
import com.pinganfu.hippo.leveldb.util.SliceOutput;
import com.pinganfu.hippo.leveldb.util.Slices;

public class LogReader0 {
	private final FileChannel fileChannel;

	private final LogMonitor monitor;

	private final boolean verifyChecksums;

	/**
	 * Offset at which to start looking for the first record to return
	 */
	private final long initialOffset;

	/**
	 * Have we read to the end of the file?
	 */
	private boolean eof;

	/**
	 * Offset of the last record returned by readRecord.
	 */
	private long lastRecordOffset;

	/**
	 * 下条记录的物理偏移量
	 */
	private long newRecordOffset;

	/**
	 * Offset of the first location past the end of buffer.
	 */
	private long endOfBufferOffset;

	/**
	 * Scratch buffer in which the next record is assembled.
	 */
	private final DynamicSliceOutput recordScratch = new DynamicSliceOutput(BLOCK_SIZE);

	/**
	 * Scratch buffer for current block.  The currentBlock is sliced off the underlying buffer.
	 */
	private final SliceOutput blockScratch = Slices.allocate(BLOCK_SIZE).output();

	/**
	 * The current block records are being read from.
	 */
	private SliceInput currentBlock = Slices.EMPTY_SLICE.input();

	/**
	 * Current chunk which is sliced from the current block.
	 */
	private Slice currentChunk = Slices.EMPTY_SLICE;

	public LogReader0(FileChannel fileChannel, LogMonitor monitor, boolean verifyChecksums, long initialOffset) {
		this.fileChannel = fileChannel;
		this.monitor = monitor;
		this.verifyChecksums = verifyChecksums;
		this.initialOffset = initialOffset;
	}

	public long getLastRecordOffset() {
		return lastRecordOffset;
	}

	/**
	 * Skips all blocks that are completely before "initial_offset_".
	 * <p/>
	 * Handles reporting corruption
	 *
	 * @return true on success.
	 */
	private boolean skipToInitialBlock() {
		int offsetInBlock = (int) (initialOffset % BLOCK_SIZE);
		long blockStartLocation = initialOffset - offsetInBlock;

		// Don't search a block if we'd be in the trailer
		if (offsetInBlock > BLOCK_SIZE - 6) {
			blockStartLocation += BLOCK_SIZE;
		}

		endOfBufferOffset = blockStartLocation;

		// Skip to start of first block that can contain the initial record
		if (blockStartLocation > 0) {
			try {
				fileChannel.position(blockStartLocation);
			} catch (IOException e) {
				reportDrop(blockStartLocation, e);
				return false;
			}
		}

		return true;
	}

	public Slice readRecord() {
		Snapshot global = new Snapshot();
		global.mark();

		eof = false;
		recordScratch.reset();

		// advance to the first record, if we haven't already
		if (lastRecordOffset < initialOffset) {
			if (!skipToInitialBlock()) {
				return null;
			}
		}

		// Record offset of the logical record that we're reading
		long prospectiveRecordOffset = 0;

		boolean inFragmentedRecord = false;
		while (true) {
			long physicalRecordOffset = endOfBufferOffset - currentChunk.length();
			LogChunkType chunkType = readNextChunk();
			switch (chunkType) {
			case FULL:
				if (inFragmentedRecord) {
					reportCorruption(recordScratch.size(), "Partial record without end");
					// simply return this full block
				}
				recordScratch.reset();
				prospectiveRecordOffset = physicalRecordOffset;
				lastRecordOffset = prospectiveRecordOffset;
				newRecordOffset = endOfBufferOffset - currentBlock.available();
				return currentChunk.copySlice();

			case FIRST:
				if (inFragmentedRecord) {
					reportCorruption(recordScratch.size(), "Partial record without end");
					// clear the scratch and start over from this chunk
					recordScratch.reset();

					global.recover();
					return null;
				}
				prospectiveRecordOffset = physicalRecordOffset;
				recordScratch.writeBytes(currentChunk);
				inFragmentedRecord = true;
				break;

			case MIDDLE:
				if (!inFragmentedRecord) {
					reportCorruption(recordScratch.size(), "Missing start of fragmented record");

					// clear the scratch and skip this chunk
					recordScratch.reset();

					global.recover();
					return null;
				} else {
					recordScratch.writeBytes(currentChunk);
				}
				break;

			case LAST:
				if (!inFragmentedRecord) {
					reportCorruption(recordScratch.size(), "Missing start of fragmented record");

					// clear the scratch and skip this chunk
					recordScratch.reset();

					global.recover();
					return null;
				} else {
					recordScratch.writeBytes(currentChunk);
					lastRecordOffset = prospectiveRecordOffset;
					newRecordOffset = endOfBufferOffset - currentBlock.available();
					return recordScratch.slice().copySlice();
				}
			case EOF:
				if (inFragmentedRecord) {
					reportCorruption(recordScratch.size(), "Partial record without end");

					// clear the scratch and return
					recordScratch.reset();
				}
				return null;

			case BAD_CHUNK:
				if (inFragmentedRecord) {
					reportCorruption(recordScratch.size(), "Error in middle of record");
					inFragmentedRecord = false;
					recordScratch.reset();
				}
				global.recover();
				return null;
				
			default:
				int dropSize = currentChunk.length();
				if (inFragmentedRecord) {
					dropSize += recordScratch.size();
				}
				reportCorruption(dropSize, String.format("Unexpected chunk type %s", chunkType));
				inFragmentedRecord = false;
				recordScratch.reset();

				global.recover();
				return null;
			}
		}
	}

	/**
	 * Return type, or one of the preceding special values
	 */
	private LogChunkType readNextChunk() {
		// clear the current chunk
		currentChunk = Slices.EMPTY_SLICE;
		
		// read the next block if necessary
		if (currentBlock.available() < HEADER_SIZE) {
			if (!readNextBlock()) {
				if (eof) {
					return EOF;
				}
			}
		}

		// parse header
		int expectedChecksum = currentBlock.readInt();
		int length = currentBlock.readUnsignedByte();
		length = length | currentBlock.readUnsignedByte() << 8;
		byte chunkTypeId = currentBlock.readByte();
		LogChunkType chunkType = getLogChunkTypeByPersistentId(chunkTypeId);

		// verify length
		if (length > currentBlock.available()) {
			int dropSize = currentBlock.available() + HEADER_SIZE;
			reportCorruption(dropSize, "Invalid chunk length");
			currentBlock = Slices.EMPTY_SLICE.input();
			return BAD_CHUNK;
		}

		// skip zero length records
		if (chunkType == ZERO_TYPE && length == 0) {
			// Skip zero length record without reporting any drops since
			// such records are produced by the writing code.
			currentBlock = Slices.EMPTY_SLICE.input();
			return BAD_CHUNK;
		}

		// Skip physical record that started before initialOffset
		if (endOfBufferOffset - HEADER_SIZE - length < initialOffset) {
			currentBlock.skipBytes(length);
			return BAD_CHUNK;
		}

		// read the chunk
		currentChunk = currentBlock.readBytes(length);

		if (verifyChecksums) {
			int actualChecksum = getChunkChecksum(chunkTypeId, currentChunk);
			if (actualChecksum != expectedChecksum) {
				// Drop the rest of the buffer since "length" itself may have
				// been corrupted and if we trust it, we could find some
				// fragment of a real log record that just happens to look
				// like a valid log record.
				int dropSize = length + HEADER_SIZE;
				currentBlock = Slices.EMPTY_SLICE.input();
				reportCorruption(dropSize, "Invalid chunk checksum offset=" + newRecordOffset);
				return BAD_CHUNK;
			}
		}

		// Skip unknown chunk types
		// Since this comes last s o we the, know it is a valid chunk, and is just a type we don't understand
		if (chunkType == UNKNOWN) {
			reportCorruption(length, String.format("Unknown chunk type %d", chunkType.getPersistentId()));
			return BAD_CHUNK;
		}

		return chunkType;
	}

	private int offsetInBlock;
	
	public boolean readNextBlock() {
		if (eof) {
			return false;
		}

		// clear the block
		blockScratch.reset();

		// 读取块内剩余的字节
		int writableBytes;
		int curOffsetInBlock = offsetInBlock;
		while ((writableBytes = BLOCK_SIZE - curOffsetInBlock) > 0) {
			try {
				int bytesRead = blockScratch.writeBytes(fileChannel, writableBytes);
				if (bytesRead < 0) {
					// no more bytes to read
					eof = true;
					break;
				}
				
				curOffsetInBlock += bytesRead;
				endOfBufferOffset += bytesRead;
			} catch (IOException e) {
				currentBlock = Slices.EMPTY_SLICE.input();
				reportDrop(BLOCK_SIZE, e);
				eof = true;
				return false;
			}
		}
		
		offsetInBlock = curOffsetInBlock % BLOCK_SIZE;
		currentBlock = blockScratch.slice().input();
		return currentBlock.isReadable();
	}

	/**
	 * Reports corruption to the monitor.
	 * The buffer must be updated to remove the dropped bytes prior to invocation.
	 */
	void reportCorruption(long bytes, String reason) {
		if (monitor != null) {
			monitor.corruption(bytes, reason);
		}
	}

	/**
	 * Reports dropped bytes to the monitor.
	 * The buffer must be updated to remove the dropped bytes prior to invocation.
	 */
	void reportDrop(long bytes, Throwable reason) {
		if (monitor != null) {
			monitor.corruption(bytes, reason);
		}
	}
	
	boolean eof(Long size) {
		if (size == null) 
			return false;
		
		if (size < 0)
			return eof;
		else
			return endOfBufferOffset >= size;
	}
	
	void close() {
		try {
			fileChannel.close();
		} catch (IOException e) {
			Throwables.propagate(e);
		}
	}
	
	public void setPosition(long index) {
		new Snapshot(index).recover();
	}

	private class Snapshot {
		private long position;//记录在文件中的位置

		public Snapshot() {
		}

		public Snapshot(long position) {
			this.position = position;
		}
		
		public void mark() {
			position = LogReader0.this.newRecordOffset;
		}

		public void recover() {
			LogReader0.this.eof = false;
			LogReader0.this.lastRecordOffset = position;// ?
			LogReader0.this.endOfBufferOffset = position;
			LogReader0.this.offsetInBlock = (int)(position % BLOCK_SIZE);
			LogReader0.this.currentBlock = Slices.EMPTY_SLICE.input();
			try {
				LogReader0.this.fileChannel.position(position);
			} catch (IOException e) {
				Throwables.propagate(e);
			}
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append("Snapshot");
			sb.append("{lastRecordOffset=").append(lastRecordOffset);
			sb.append('}');
			return sb.toString();
		}
	}
	
	public long pointer() {
		return newRecordOffset;
	}
	
	public static void main(String[] args) {
		System.out.println(9727112 % BLOCK_SIZE);
		System.out.println(12668112 % BLOCK_SIZE);
	}
}
