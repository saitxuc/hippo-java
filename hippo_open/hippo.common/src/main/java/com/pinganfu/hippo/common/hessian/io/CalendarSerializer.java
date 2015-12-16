package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.util.Calendar;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:15
 *
 */
public class CalendarSerializer extends AbstractSerializer {
	private static CalendarSerializer SERIALIZER = new CalendarSerializer();

	public static CalendarSerializer create() {
		return SERIALIZER;
	}

	public void writeObject(Object obj, AbstractHessianOutput out)
			throws IOException {
		if (obj == null)
			out.writeNull();
		else {
			Calendar cal = (Calendar) obj;

			out.writeObject(new CalendarHandle(cal.getClass(), cal
					.getTimeInMillis()));
		}
	}
}
