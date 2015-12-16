package com.pinganfu.hippo.common;

import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * 
 * @author saitxuc
 * write 2014-7-7
 */
public class InetAddressUtil {

	public static String getLocalHostName() throws UnknownHostException {
		try {
			return (InetAddress.getLocalHost()).getHostName();
		} catch (UnknownHostException uhe) {
			String host = uhe.getMessage(); // host = "hostname: hostname"
			if (host != null) {
				int colon = host.indexOf(':');
				if (colon > 0) {
					return host.substring(0, colon);
				}
			}
			throw uhe;
		}
	}
}