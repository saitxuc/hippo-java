package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
/**
 * 
 * @author sait.xuc
 *
 */
abstract public class HessianEnvelope {
	/**
	 * Wrap the Hessian output stream in an envelope.
	 */
	abstract public Hessian2Output wrap(Hessian2Output out) throws IOException;

	/**
	 * Unwrap the Hessian input stream with this envelope. It is an error if the
	 * actual envelope does not match the expected envelope class.
	 */
	abstract public Hessian2Input unwrap(Hessian2Input in) throws IOException;

	/**
	 * Unwrap the envelope after having read the envelope code ('E') and the
	 * envelope method. Called by the EnvelopeFactory for dynamic reading of the
	 * envelopes.
	 */
	abstract public Hessian2Input unwrapHeaders(Hessian2Input in)
			throws IOException;
}
