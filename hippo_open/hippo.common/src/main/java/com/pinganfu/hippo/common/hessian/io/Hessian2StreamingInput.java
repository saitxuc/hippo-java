package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 14:40
 *
 */
public class Hessian2StreamingInput {
	private Hessian2Input _in;

	/**
	 * Creates a new Hessian input stream, initialized with an underlying input
	 * stream.
	 * 
	 * @param is
	 *            the underlying output stream.
	 */
	public Hessian2StreamingInput(InputStream is) {
		_in = new Hessian2Input(new StreamingInputStream(is));
	}

	/**
	 * Read the next object
	 */
	public Object readObject() throws IOException {
		return _in.readStreamingObject();
	}

	/**
	 * Close the output.
	 */
	public void close() throws IOException {
		_in.close();
	}

	static class StreamingInputStream extends InputStream {
		private InputStream _is;
		private int _length;

		StreamingInputStream(InputStream is) {
			_is = is;
		}

		public int read() throws IOException {
			InputStream is = _is;

			while (_length == 0) {
				int code = is.read();

				if (code < 0)
					return -1;
				else if (code != 'p' && code != 'P')
					throw new HessianProtocolException(
							"expected streaming packet at 0x"
									+ Integer.toHexString(code & 0xff));

				int d1 = is.read();
				int d2 = is.read();

				if (d2 < 0)
					return -1;

				_length = (d1 << 8) + d2;
			}

			_length--;
			return is.read();
		}

		public int read(byte[] buffer, int offset, int length)
				throws IOException {
			InputStream is = _is;

			while (_length == 0) {
				int code = is.read();

				if (code < 0)
					return -1;
				else if (code != 'p' && code != 'P') {
					throw new HessianProtocolException(
							"expected streaming packet at 0x"
									+ Integer.toHexString(code & 0xff) + " ("
									+ (char) code + ")");
				}

				int d1 = is.read();
				int d2 = is.read();

				if (d2 < 0)
					return -1;

				_length = (d1 << 8) + d2;
			}

			int sublen = _length;
			if (length < sublen)
				sublen = length;

			sublen = is.read(buffer, offset, sublen);

			if (sublen < 0)
				return -1;

			_length -= sublen;

			return sublen;
		}
	}
}
