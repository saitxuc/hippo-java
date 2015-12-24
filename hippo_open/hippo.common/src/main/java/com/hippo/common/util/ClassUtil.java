package com.hippo.common.util;

import java.lang.reflect.Constructor;

/**
 * 
 * @author saitxuc
 *
 */
public class ClassUtil {
	
	public static Object intanceByClass(Class<?> aClass, Class<?>[] types, Object[] objects) {
		try {
			Constructor<?> con = aClass.getConstructor(types);
			return con.newInstance(objects);
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static Class<?> classByClassName(String className) {
		Class<?> clazz = null;
		try {  
            clazz = Class.forName(className);  
        } catch (Exception e) {  
            // TODO: handle exception  
        } 
		return clazz;
	}
	
}
