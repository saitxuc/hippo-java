package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:39
 *
 */
public abstract class AbstractHessianResolver implements HessianRemoteResolver {
	/**
	 * Looks up a proxy object.
	 */
	public Object lookup(String type, String url) throws IOException {
		return new HessianRemote(type, url);
	}
}
