package com.pinganfu.hippo.common.util;

/**
 * 
 * @author saitxuc
 * 2015-4-16
 */
public class LimitUtils {
	
	public static final long calculationLimit(String climit) {
		long rlimit  = -1;
		climit = climit.trim();
		if(climit.endsWith("kb") ) {
			String shortlimit = climit.substring(0, climit.length()-2);
			rlimit = Long.parseLong(shortlimit.trim()) * 1024;
		}
		if(climit.endsWith("k")) {
			String shortlimit = climit.substring(0, climit.length()-1);
			rlimit = Long.parseLong(shortlimit.trim()) * 1024;
		}
		if(climit.endsWith("mb") ) {
			String shortlimit = climit.substring(0, climit.length()-2);
			rlimit = Long.parseLong(shortlimit.trim()) * 1024 * 1024;
		}
		if(climit.endsWith("m")) {
			String shortlimit = climit.substring(0, climit.length()-1);
			rlimit = Long.parseLong(shortlimit.trim()) * 1024 * 1024;
		}
		if(climit.endsWith("gb") ) {
			String shortlimit = climit.substring(0, climit.length()-2);
			rlimit = Long.parseLong(shortlimit.trim()) * 1024 * 1024 * 1024;
		}
		if(climit.endsWith("g")) {
			String shortlimit = climit.substring(0, climit.length()-1);
			rlimit = Long.parseLong(shortlimit.trim()) * 1024 * 1024 * 1024;
		}
		return rlimit;
	}
	
}
