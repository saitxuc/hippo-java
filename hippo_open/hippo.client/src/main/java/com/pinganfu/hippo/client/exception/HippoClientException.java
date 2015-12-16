package com.pinganfu.hippo.client.exception;

import com.pinganfu.hippo.common.exception.HippoException;

public class HippoClientException extends HippoException {
	
	private static final long serialVersionUID = -3268650089685988510L;

	public HippoClientException(String reason, String errorCode) {
		super(reason, errorCode);
	}
	
}
