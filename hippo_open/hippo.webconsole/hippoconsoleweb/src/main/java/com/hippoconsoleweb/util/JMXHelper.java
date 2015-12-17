package com.hippoconsoleweb.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/26
 * Time: 13:39
 *
 */
public class JMXHelper {
	
	private static final Logger log = LoggerFactory.getLogger(JMXHelper.class);
	
	private static final int DEFAULT_TIME_OUT = 2;
    private static final ThreadFactory daemonThreadFactory = new DaemonThreadFactory();
    
    public static JMXConnector getConnection(final String host, final int port) throws Exception {
        JMXServiceURL address = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");
        log.info(address.toString());
        return connectWithTimeout(address, DEFAULT_TIME_OUT, TimeUnit.SECONDS);
    }
    
	public static JMXConnector connectWithTimeout2(final JMXServiceURL url,
			long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<JMXConnector> future = executor
				.submit(new Callable<JMXConnector>() {
					public JMXConnector call() throws IOException {
						return JMXConnectorFactory.connect(url);
					}
				});
		JMXConnector connect = future.get(timeout, unit);
		executor.shutdown();
		return connect;
	}
    
	public static Object executeMethod(final JMXServiceURL url,
			ObjectName name, String operationName, Object params[],
			String signature[]) throws RemoteException {
		JMXConnector jmxConnector = null;
		try {
			jmxConnector = connectWithTimeout(url, DEFAULT_TIME_OUT,
					TimeUnit.SECONDS);
			MBeanServerConnection connection = jmxConnector
					.getMBeanServerConnection();
			return connection.invoke(name, operationName, params, signature);
		} catch (Exception e) {
			if (e instanceof RemoteException) {
				throw (RemoteException) e;
			} else {
				throw new RemoteException("IOException at call  jmx.", e);
			}
		} finally {
			if (jmxConnector != null) {
				try {
					jmxConnector.close();
				} catch (IOException e) {
					log.error(e.getMessage(), e);
				}
			}
		}
	}
	
	public static Object getAttribute(final JMXServiceURL url, ObjectName name,
			String attributeName) throws RemoteException {
		JMXConnector jmxConnector = null;
		try {
			jmxConnector = connectWithTimeout(url, DEFAULT_TIME_OUT,
					TimeUnit.SECONDS);
			MBeanServerConnection connection = jmxConnector
					.getMBeanServerConnection();
			return connection.getAttribute(name, attributeName);
		} catch (Exception e) {
			if (e instanceof RemoteException) {
				throw (RemoteException) e;
			} else {
				throw new RemoteException("IOException at call  jmx.", e);
			}
		} finally {
			try {
				if (jmxConnector != null) {
					jmxConnector.close();
				}
			} catch (IOException e) {
				log.error("error when close JMXConnector: " + e);
			}
		}
	}

	public static Object getAttribute(final JMXConnector jmxConnector,
			ObjectName name, String attributeName) throws RemoteException {
		try {
			MBeanServerConnection connection = jmxConnector
					.getMBeanServerConnection();
			return connection.getAttribute(name, attributeName);
		} catch (Exception e) {
			if (e instanceof RemoteException) {
				throw (RemoteException) e;
			} else {
				throw new RemoteException("IOException at call  jmx.", e);
			}
		}
	}

	public AttributeList getAttributes(final JMXServiceURL url,
			ObjectName name, String[] attributeNames) throws RemoteException {
		JMXConnector jmxConnector = null;
		try {
			jmxConnector = connectWithTimeout(url, DEFAULT_TIME_OUT,
					TimeUnit.SECONDS);
			MBeanServerConnection connection = jmxConnector
					.getMBeanServerConnection();
			return connection.getAttributes(name, attributeNames);
		} catch (Exception e) {
			if (e instanceof RemoteException) {
				throw (RemoteException) e;
			} else {
				throw new RemoteException("IOException at call  jmx.", e);
			}
		} finally {
			try {
				if (jmxConnector != null) {
					jmxConnector.close();
				}
			} catch (IOException e) {
				log.error("error when close JMXConnector: " + e);
			}
		}
	}

	public void close(JMXConnector jmxConnector) {
		if (jmxConnector != null) {
			try {
				jmxConnector.close();
			} catch (IOException e) {
				log.error("the connection cannot be closed cleanly.", e);
			}
		}
	}
	
    
    public static JMXConnector connectWithTimeout(
            final JMXServiceURL url, long timeout, TimeUnit unit)
            throws IOException {
        final BlockingQueue<Object> mailbox = new ArrayBlockingQueue<Object>(1);
        ExecutorService executor = Executors.newSingleThreadExecutor(daemonThreadFactory);
        executor.submit(new Runnable() {
            public void run() {
                try {
                    Map<String, String> env = new HashMap<String, String>();
                    env.put(InitialContext.INITIAL_CONTEXT_FACTORY, RMIContextFactory.class.getName());
                    JMXConnector connector = JMXConnectorFactory.connect(url, env);
                    
                    if (!mailbox.offer(connector))
                        connector.close();
                } catch (Throwable t) {
                    mailbox.offer(t);
                }
            }
        });
        Object result;
        try {
            result = mailbox.poll(timeout, unit);
            if (result == null) {
                if (!mailbox.offer(""))
                    result = mailbox.take();
            }
        } catch (InterruptedException e) {
            throw initCause(new InterruptedIOException(e.getMessage()), e);
        } finally {
            executor.shutdown();
        }
        if (result == null)
            throw new SocketTimeoutException("Connect timed out: " + url);
        if (result instanceof JMXConnector)
            return (JMXConnector) result;
        try {
            throw (Throwable) result;
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            // In principle this can't happen but we wrap it anyway
            throw new IOException(e.toString(), e);
        }
    }

    private static <T extends Throwable> T initCause(T wrapper, Throwable wrapped) {
        wrapper.initCause(wrapped);
        return wrapper;
    }

    private static class DaemonThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }
    }
    
    public static void main(String[] args) throws Exception {
        try {
            System.out.println("connect to: " + args[1]);
            JMXConnector jmxConnector = JMXHelper.getConnection(args[1], 1099);
            System.out.println("connect id:" + jmxConnector.getConnectionId());
            System.out.println("sleep 10s");
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
