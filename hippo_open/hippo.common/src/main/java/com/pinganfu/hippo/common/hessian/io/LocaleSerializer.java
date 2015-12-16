package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.util.Locale;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 13:45
 *
 */
public class LocaleSerializer extends AbstractSerializer {
	private static LocaleSerializer SERIALIZER = new LocaleSerializer();

	public static LocaleSerializer create() {
		return SERIALIZER;
	}

	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		if (obj == null)
			out.writeNull();
		else {
			Locale locale = (Locale) obj;

			out.writeObject(new LocaleHandle(locale.toString()));
		}
	}
}
