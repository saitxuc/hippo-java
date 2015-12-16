package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.util.Date;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:32
 *
 */
public class SqlDateSerializer extends AbstractSerializer {
	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		if (obj == null)
			out.writeNull();
		else {
			Class cl = obj.getClass();

			if (out.addRef(obj))
				return;

			int ref = out.writeObjectBegin(cl.getName());

			if (ref < -1) {
				out.writeString("value");
				out.writeUTCDate(((Date) obj).getTime());
				out.writeMapEnd();
			} else {
				if (ref == -1) {
					out.writeInt(1);
					out.writeString("value");
					out.writeObjectBegin(cl.getName());
				}

				out.writeUTCDate(((Date) obj).getTime());
			}
		}
	}
}
