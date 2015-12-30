package com.hippo.common.hessian.io;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 14:20
 *
 */
public class HessianRemote {
	private String type;
	private String url;

	/**
	 * Creates a new Hessian remote object.
	 * 
	 * @param type
	 *            the remote stub interface
	 * @param url
	 *            the remote url
	 */
	public HessianRemote(String type, String url) {
		this.type = type;
		this.url = url;
	}

	/**
	 * Creates an uninitialized Hessian remote.
	 */
	public HessianRemote() {
	}

	/**
	 * Returns the remote api class name.
	 */
	public String getType() {
		return type;
	}

	/**
	 * Returns the remote URL.
	 */
	public String getURL() {
		return url;
	}

	/**
	 * Sets the remote URL.
	 */
	public void setURL(String url) {
		this.url = url;
	}

	/**
	 * Defines the hashcode.
	 */
	public int hashCode() {
		return url.hashCode();
	}

	/**
	 * Defines equality
	 */
	public boolean equals(Object obj) {
		if (!(obj instanceof HessianRemote))
			return false;

		HessianRemote remote = (HessianRemote) obj;

		return url.equals(remote.url);
	}

	/**
	 * Readable version of the remote.
	 */
	public String toString() {
		return "[HessianRemote " + url + "]";
	}
}
