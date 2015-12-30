package com.hippoconsoleweb.util;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import com.sun.jndi.url.rmi.rmiURLContext;

public class RMIContextFactory implements InitialContextFactory {

    /* (non-Javadoc)
     * @see javax.naming.spi.InitialContextFactory#getInitialContext(java.util.Hashtable)
     */
    @Override
    public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
        return new rmiURLContext(environment);
    }

}
