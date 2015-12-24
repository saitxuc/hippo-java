package com.hippo.leveldb.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Throwables;

/**
 * @author yangxin
 */
public class FileTest {
	static class LogWriter implements Runnable {
		private File logFile = null;

		public LogWriter(File logFile) {
			this.logFile = logFile;
		}

		public void run() {
			FileOutputStream randomFile = null;
			try {
				randomFile = new FileOutputStream(logFile);
			} catch (FileNotFoundException e) {
				Throwables.propagate(e);
			}
			int i = 0;
			while (true) {
				try {
//					randomFile.writeBytes(i++ + "sss");
					randomFile.write((i++ + "sss").getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	static class LogReader implements Runnable {
		private File logFile = null;
		private long lastTimeFileSize = 0; // 上次文件大小

		public LogReader(File logFile) {
			this.logFile = logFile;
			lastTimeFileSize = logFile.length();
		}

		/**
		 * 实时输出日志信息
		 */
		public void run() {//RandomAccessFile
			FileChannel channel = null;
			try {
				channel = new FileInputStream(logFile).getChannel();
				channel.position(lastTimeFileSize);
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			while (true) {
				try {
					ByteBuffer buffer = ByteBuffer.allocate(10);
					while (channel.read(buffer) != -1) {
						lastTimeFileSize += buffer.remaining();
						System.out.println(new String(buffer.array()));
						System.out.println("=============" + lastTimeFileSize);
						buffer.clear();
					}
//					lastTimeFileSize = randomFile.length();
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
	
	public static void main(String[] args) {
		File logFile = new File(System.getProperty("user.home") + File.separator + "leveldb" + File.separator + "mock.log");
		Thread wthread = new Thread(new LogWriter(logFile));
		wthread.start();
		Thread rthread = new Thread(new LogReader(logFile));
		rthread.start();
	}
}
