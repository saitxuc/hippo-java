package com.pinganfu.hippo.common.hessian.io;

import java.util.logging.*;
import java.io.*;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 14:20
 *
 */
public class HessianInputFactory {
	public static final Logger log = Logger.getLogger(HessianInputFactory.class
			.getName());

	private SerializerFactory _serializerFactory;

	public void setSerializerFactory(SerializerFactory factory) {
		_serializerFactory = factory;
	}

	public SerializerFactory getSerializerFactory() {
		return _serializerFactory;
	}

	public AbstractHessianInput open(InputStream is) throws IOException {
		int code = is.read();

		int major = is.read();
		int minor = is.read();

		switch (code) {
		case 'c':
		case 'C':
		case 'r':
		case 'R':
			if (major >= 2) {
				AbstractHessianInput in = new Hessian2Input(is);
				in.setSerializerFactory(_serializerFactory);
				return in;
			} else {
				AbstractHessianInput in = new HessianInput(is);
				in.setSerializerFactory(_serializerFactory);
				return in;
			}

		default:
			throw new IOException((char) code
					+ " is an unknown Hessian message code.");
		}
	}
}
