package com.hippo.common.hessian.io;

import java.io.IOException;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:26
 *
 */
public class StringValueSerializer extends AbstractSerializer {
	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		if (obj == null)
			out.writeNull();
		else {
			if (out.addRef(obj))
				return;

			Class cl = obj.getClass();

			int ref = out.writeObjectBegin(cl.getName());

			if (ref < -1) {
				out.writeString("value");
				out.writeString(obj.toString());
				out.writeMapEnd();
			} else {
				if (ref == -1) {
					out.writeInt(1);
					out.writeString("value");
					out.writeObjectBegin(cl.getName());
				}

				out.writeString(obj.toString());
			}
		}
	}
}
