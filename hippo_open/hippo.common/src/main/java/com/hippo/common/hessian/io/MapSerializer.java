package com.hippo.common.hessian.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:41
 *
 */
public class MapSerializer extends AbstractSerializer {
	private boolean _isSendJavaType = true;

	/**
	 * Set true if the java type of the collection should be sent.
	 */
	public void setSendJavaType(boolean sendJavaType) {
		_isSendJavaType = sendJavaType;
	}

	/**
	 * Return true if the java type of the collection should be sent.
	 */
	public boolean getSendJavaType() {
		return _isSendJavaType;
	}

	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		if (out.addRef(obj))
			return;

		Map map = (Map) obj;

		Class cl = obj.getClass();

		if (cl.equals(HashMap.class) || !_isSendJavaType
				|| !(obj instanceof java.io.Serializable))
			out.writeMapBegin(null);
		else
			out.writeMapBegin(obj.getClass().getName());

		Iterator iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();

			out.writeObject(entry.getKey());
			out.writeObject(entry.getValue());
		}
		out.writeMapEnd();
	}
}
