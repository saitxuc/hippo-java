package com.hippo.client.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yangxin
 */
public abstract class PropertiesUtil {

	private final static Map<String, Properties> propMap = new ConcurrentHashMap<String, Properties>();
	private static final String XML_FILE_EXTENSION = ".xml";

	private static Properties loadAllProperties(String resourceName, ClassLoader classLoader) throws IOException {
		ClassLoader clToUse = classLoader;
		if (clToUse == null) {
			clToUse = getDefaultClassLoader();
		}
		Properties props = new Properties();
		Enumeration<URL> urls = clToUse.getResources(resourceName);
		while (urls.hasMoreElements()) {
			URL url = (URL) urls.nextElement();
			URLConnection con = url.openConnection();
			useCachesIfNecessary(con);
			InputStream is = con.getInputStream();
			try {
				if (resourceName != null && resourceName.endsWith(XML_FILE_EXTENSION)) {
					props.loadFromXML(is);
				} else {
					props.load(is);
				}
			} finally {
				is.close();
			}
		}
		return props;
	}

	public static ClassLoader getDefaultClassLoader() {
		ClassLoader cl = null;
		try {
			cl = Thread.currentThread().getContextClassLoader();
		} catch (Throwable ex) {
		}
		if (cl == null) {
			cl = PropertiesUtil.class.getClassLoader();
		}
		return cl;
	}

	private static void useCachesIfNecessary(URLConnection con) {
		con.setUseCaches(con.getClass().getSimpleName().startsWith("JNLP"));
	}

	/**
	 * @param resourceName
	 * @return
	 * @throws LoadPropertiesException 文件加载或读取失败
	 */
	public static synchronized Properties getProperties(String resourceName, ClassLoader classLoader) {
		Properties prop = propMap.get(resourceName);
		if (prop != null) {
			return prop;
		}

		try {
			prop = loadAllProperties(resourceName, classLoader);
		} catch (IOException e) {
//			throw new LoadPropertiesException("load properties file error", e);
		}
		propMap.put(resourceName, prop);
		return prop;
	}

	/**
	 * @return
	 * @throws LoadPropertiesException 文件加载或读取失败
	 */
	public static Properties getProperties(String resourceName) {
		return getProperties(resourceName, null);
	}

	/**
	 * 读取指定jar包中的文件，如果本地存在相同的文件，则替换成本地的
	 * @return
	 * @throws LoadPropertiesException 文件加载或读取失败
	 */
//	public static Properties getLocalProperties(String resourceName) {
//		Properties p = getProperties(resourceName);
//		Properties lp = EnvironmentUtils.readLocalEnv();
//		p.putAll(lp);
//		return p;
//	}
}
