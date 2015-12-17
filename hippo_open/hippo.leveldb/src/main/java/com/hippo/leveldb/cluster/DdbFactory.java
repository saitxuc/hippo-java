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
package com.hippo.leveldb.cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;

import com.google.common.collect.Lists;
import com.hippo.leveldb.DB;
import com.hippo.leveldb.DBFactory;
import com.hippo.leveldb.Options;
import com.hippo.leveldb.util.FileUtils;

/**
 * @author yangxin
 */
public class DdbFactory implements DBFactory {

	public static final int CPU_DATA_MODEL = Integer.getInteger("sun.arch.data.model");

	// We only use MMAP on 64 bit systems since it's really easy to run out of
	// virtual address space on a 32 bit system when all the data is getting mapped
	// into memory. If you really want to use MMAP anyways, use -Dleveldb.mmap=true
	public static final boolean USE_MMAP = Boolean.parseBoolean(System.getProperty("leveldb.mmap", "" + (CPU_DATA_MODEL > 32)));
//	public static final boolean USE_MMAP = false;
	public static final String VERSION;
	static {
		String v = "unknown";
		InputStream is = DdbFactory.class.getResourceAsStream("version.txt");
		try {
			v = new BufferedReader(new InputStreamReader(is, "UTF-8")).readLine();
		} catch (Throwable e) {
		} finally {
			try {
				is.close();
			} catch (Throwable e) {
			}
		}
		VERSION = v;
	}

	public static final DdbFactory factory = new DdbFactory();
	private List<DB> opened = Lists.newArrayListWithCapacity(1);
	private final static String dataDir = System.getProperty("user.home") + File.separator + "leveldb";
	private File databaseDir;
	
	@Override
	public synchronized DB open(File path, Options options) throws IOException {
		if (opened.size() > 0) {
			return opened.get(0);
		}
		System.out.println(dataDir);
		DB db = new Ddb(options, path);
		opened.add(db);
		return db;
	}
	
	public synchronized DB open(Options options) throws IOException {
		if (opened.size() > 0) {
			return opened.get(0);
		}
		
		if (databaseDir == null) {
			databaseDir = new File(dataDir);
		}
		DB db = new Ddb(options, databaseDir);
		opened.add(db);
		return db;
	}
	
	public synchronized DB open() throws IOException {
		if (opened.size() > 0) {
			return opened.get(0);
		}
		
		if (databaseDir == null) {
			databaseDir = new File(dataDir);
		}
		
		Options options = new Options().createIfMissing(true);
		DB db = new Ddb(options, databaseDir);
		opened.add(db);
		return db;
	}

	@Override
	public void destroy(File path, Options options) throws IOException {
		// TODO: This should really only delete leveldb-created files.
		FileUtils.deleteRecursively(path);
	}

	public void close(DB db) throws IOException {
		db.close();
		opened.remove(db);
	}

	public void close() throws IOException {
		for (DB db : opened) {
			db.close();
		}
		opened.clear();
	}

	@Override
	public void repair(File path, Options options) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return String.format("hippo leveldb version %s", VERSION);
	}

	public static byte[] bytes(String value) {
		if (value == null) {
			return null;
		}
		try {
			return value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String asString(byte value[]) {
		if (value == null) {
			return null;
		}
		try {
			return new String(value, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
	
}
