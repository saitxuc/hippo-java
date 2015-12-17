package com.hippo.client.exception;

import com.hippo.common.exception.HippoException;

public class HippoClientException extends HippoException {
	
	private static final long serialVersionUID = -3268650089685988510L;

	public HippoClientException(String reason, String errorCode) {
		super(reason, errorCode);
	}
	
}
