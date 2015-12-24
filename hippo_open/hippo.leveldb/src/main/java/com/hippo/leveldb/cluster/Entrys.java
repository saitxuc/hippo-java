package com.hippo.leveldb.cluster;

import static com.hippo.leveldb.util.SizeOf.SIZE_OF_INT;
import static com.hippo.leveldb.util.Slices.readLengthPrefixedBytes;
import static com.hippo.leveldb.util.Slices.writeLengthPrefixedBytes;
import static com.hippo.leveldb.util.VariableLengthQuantity.readVariableLengthInt;
import static com.hippo.leveldb.util.VariableLengthQuantity.writeVariableLengthInt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.google.common.base.Preconditions;
import com.hippo.leveldb.table.BlockEntry;
import com.hippo.leveldb.util.Slice;
import com.hippo.leveldb.util.SliceInput;
import com.hippo.leveldb.util.SliceOutput;
import com.hippo.leveldb.util.Slices;

/**
 * @author yangxin
 */
public class Entrys implements Serializable {
	/**
	 * inner use
	 */
	private static final long serialVersionUID = 1L;
	private final List<Entry<Slice, Slice>> entries = new ArrayList<Entry<Slice,Slice>>();
	private long size;
	
	public List<Entry<Slice, Slice>> getData() {
		return entries;
	}

	public static Entrys empty() {
		return new Entrys();
	}
	
	public void add(Entry<Slice, Slice> e) {
		size += e.getKey().length() + e.getValue().length();
		entries.add(e);
	}
	
	public long size() {
		return size;
	}

	public Entrys() {
	}

	/**
	 * 解码
	 * @param slice
	 */
	public Entrys(Slice slice) {
		Preconditions.checkNotNull(slice, "slice is null");

		SliceInput sliceInput = slice.input();
		// entrys
		int size = readVariableLengthInt(sliceInput);
		if (size > 0) {
			for (int i = 0; i < size; i++) {
				Slice k = readLengthPrefixedBytes(sliceInput);
				Slice v = readLengthPrefixedBytes(sliceInput);
				entries.add(new BlockEntry(k,v));
			}
		}
		this.size = size;
	}

	/**
	 * 编码
	 * @return
	 */
	public Slice encode() {
		int len = 0;
		
		// entrys
		// entrys'length
		len += SIZE_OF_INT;
		if (entries != null) {
			for (Entry<Slice, Slice> e : entries) {
				len += e.getKey().length() + e.getValue().length() + SIZE_OF_INT * 2;
			}
		}

		Slice slice = Slices.allocate(len);
		final SliceOutput sliceOutput = slice.output();

		// entrys
		if (entries == null) {
			writeVariableLengthInt(0, sliceOutput);
		} else {
			writeVariableLengthInt(entries.size(), sliceOutput);

			for (Entry<Slice, Slice> e : entries) {
				Slice k = e.getKey();
				Slice v = e.getValue();
				if (v == null) {
					v = Slices.EMPTY_SLICE;
				}
				writeLengthPrefixedBytes(sliceOutput, k);
				writeLengthPrefixedBytes(sliceOutput, v);
			}
		}
		return slice.slice(0, sliceOutput.size());
	}
}
