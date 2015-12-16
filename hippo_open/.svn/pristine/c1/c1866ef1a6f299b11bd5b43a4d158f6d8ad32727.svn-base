package com.pinganfu.hippo.network.transport.nio.client;

import java.util.UUID;

import Numbers.UuidNumber;

/**
 * 
 * @author saitxuc
 * 
 */
public class UuidSequenceGenerator {

	
	private String digits(long val, int digits) {  
	    long hi = 1L << (digits * 4);  
	    return UuidNumber.toString(hi | (val & (hi - 1)), UuidNumber.MAX_RADIX)  
	            .substring(1);  
	} 
	
	public synchronized String getUuidNextSequence() {
		UUID uuid = UUID.randomUUID();  
	    StringBuilder sb = new StringBuilder();  
	    sb.append(digits(uuid.getMostSignificantBits() >> 32, 8));  
	    sb.append(digits(uuid.getMostSignificantBits() >> 16, 4));  
	    sb.append(digits(uuid.getMostSignificantBits(), 4));  
	    sb.append(digits(uuid.getLeastSignificantBits() >> 48, 4));  
	    sb.append(digits(uuid.getLeastSignificantBits(), 12));  
	    return sb.toString(); 
	}

}
