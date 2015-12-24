package com.hippo.common.hessian.io;

import java.util.*;
import java.util.zip.*;

import java.io.*;
/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:10
 *
 */
public class Deflation extends HessianEnvelope {
	public Deflation() {
	}

	public Hessian2Output wrap(Hessian2Output out) throws IOException {
		OutputStream os = new DeflateOutputStream(out);

		Hessian2Output filterOut = new Hessian2Output(os);

		filterOut.setCloseStreamOnClose(true);

		return filterOut;
	}

	public Hessian2Input unwrap(Hessian2Input in) throws IOException {
		int version = in.readEnvelope();

		String method = in.readMethod();

		if (!method.equals(getClass().getName()))
			throw new IOException("expected hessian Envelope method '"
					+ getClass().getName() + "' at '" + method + "'");

		return unwrapHeaders(in);
	}

	public Hessian2Input unwrapHeaders(Hessian2Input in) throws IOException {
		InputStream is = new DeflateInputStream(in);

		Hessian2Input filter = new Hessian2Input(is);

		filter.setCloseStreamOnClose(true);

		return filter;
	}

	static class DeflateOutputStream extends OutputStream {
		private Hessian2Output _out;
		private OutputStream _bodyOut;
		private DeflaterOutputStream _deflateOut;

		DeflateOutputStream(Hessian2Output out) throws IOException {
			_out = out;

			_out.startEnvelope(Deflation.class.getName());

			_out.writeInt(0);

			_bodyOut = _out.getBytesOutputStream();

			_deflateOut = new DeflaterOutputStream(_bodyOut);
		}

		public void write(int ch) throws IOException {
			_deflateOut.write(ch);
		}

		public void write(byte[] buffer, int offset, int length)
				throws IOException {
			_deflateOut.write(buffer, offset, length);
		}

		public void close() throws IOException {
			Hessian2Output out = _out;
			_out = null;

			if (out != null) {
				_deflateOut.close();
				_bodyOut.close();

				out.writeInt(0);

				out.completeEnvelope();

				out.close();
			}
		}
	}

	static class DeflateInputStream extends InputStream {
		private Hessian2Input _in;

		private InputStream _bodyIn;
		private InflaterInputStream _inflateIn;

		DeflateInputStream(Hessian2Input in) throws IOException {
			_in = in;

			int len = in.readInt();

			if (len != 0)
				throw new IOException("expected no headers");

			_bodyIn = _in.readInputStream();

			_inflateIn = new InflaterInputStream(_bodyIn);
		}

		public int read() throws IOException {
			return _inflateIn.read();
		}

		public int read(byte[] buffer, int offset, int length)
				throws IOException {
			return _inflateIn.read(buffer, offset, length);
		}

		public void close() throws IOException {
			Hessian2Input in = _in;
			_in = null;

			if (in != null) {
				_inflateIn.close();
				_bodyIn.close();

				int len = in.readInt();

				if (len != 0)
					throw new IOException("Unexpected footer");

				in.completeEnvelope();

				in.close();
			}
		}
	}

}
