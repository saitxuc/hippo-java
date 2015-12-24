package com.hippo.jmx;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.StandardMBean;
import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.manager.ManagementContext;

/**
 * 
 * @author saitxuc
 * write 2014-8-12
 */
public class AnnotatedMBean extends StandardMBean {

    private static final Map<String, Class<?>> primitives = new HashMap<String, Class<?>>();

    private static final Logger LOG = LoggerFactory.getLogger("com.hippo.audit");

    private static boolean audit;
    
    static {
        Class<?>[] p = { byte.class, short.class, int.class, long.class, float.class, double.class, char.class, boolean.class, };
        for (Class<?> c : p) {
            primitives.put(c.getName(), c);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void registerMBean(ManagementContext context, Object object, ObjectName objectName) throws Exception {

        String mbeanName = object.getClass().getName() + "MBean";

        for (Class c : object.getClass().getInterfaces()) {
            if (mbeanName.equals(c.getName())) {
                context.registerMBean(new AnnotatedMBean(object, c), objectName);
                return;
            }
        }

        context.registerMBean(object, objectName);
    }

    /** Instance where the MBean interface is implemented by another object. */
    public <T> AnnotatedMBean(T impl, Class<T> mbeanInterface) throws NotCompliantMBeanException {
        super(impl, mbeanInterface);
    }

    /** Instance where the MBean interface is implemented by this object. */
    protected AnnotatedMBean(Class<?> mbeanInterface) throws NotCompliantMBeanException {
        super(mbeanInterface);
    }

    /** {@inheritDoc} */
    @Override
    protected String getDescription(MBeanAttributeInfo info) {

        String descr = info.getDescription();
        Method m = getMethod(getMBeanInterface(), "get" + info.getName().substring(0, 1).toUpperCase() + info.getName().substring(1));
        if (m == null)
            m = getMethod(getMBeanInterface(), "is" + info.getName().substring(0, 1).toUpperCase() + info.getName().substring(1));
        if (m == null)
            m = getMethod(getMBeanInterface(), "does" + info.getName().substring(0, 1).toUpperCase() + info.getName().substring(1));

        if (m != null) {
            MBeanInfo d = m.getAnnotation(MBeanInfo.class);
            if (d != null)
                descr = d.value();
        }
        return descr;
    }

    /** {@inheritDoc} */
    @Override
    protected String getDescription(MBeanOperationInfo op) {

        String descr = op.getDescription();
        Method m = getMethod(op);
        if (m != null) {
            MBeanInfo d = m.getAnnotation(MBeanInfo.class);
            if (d != null)
                descr = d.value();
        }
        return descr;
    }

    /** {@inheritDoc} */
    @Override
    protected String getParameterName(MBeanOperationInfo op, MBeanParameterInfo param, int paramNo) {
        String name = param.getName();
        Method m = getMethod(op);
        if (m != null) {
            for (Annotation a : m.getParameterAnnotations()[paramNo]) {
                if (MBeanInfo.class.isInstance(a))
                    name = MBeanInfo.class.cast(a).value();
            }
        }
        return name;
    }

    /**
     * Extracts the Method from the MBeanOperationInfo
     *
     * @param op
     * @return
     */
    private Method getMethod(MBeanOperationInfo op) {
        final MBeanParameterInfo[] params = op.getSignature();
        final String[] paramTypes = new String[params.length];
        for (int i = 0; i < params.length; i++)
            paramTypes[i] = params[i].getType();

        return getMethod(getMBeanInterface(), op.getName(), paramTypes);
    }

    /**
     * Returns the Method with the specified name and parameter types for the
     * given class, null if it doesn't exist.
     *
     * @param mbean
     * @param method
     * @param params
     * @return
     */
    private static Method getMethod(Class<?> mbean, String method, String... params) {
        try {
            final ClassLoader loader = mbean.getClassLoader();
            final Class<?>[] paramClasses = new Class<?>[params.length];
            for (int i = 0; i < params.length; i++) {
                paramClasses[i] = primitives.get(params[i]);
                if (paramClasses[i] == null)
                    paramClasses[i] = Class.forName(params[i], false, loader);
            }
            return mbean.getMethod(method, paramClasses);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Object invoke(String s, Object[] objects, String[] strings) throws MBeanException, ReflectionException {
        if (audit) {
            Subject subject = Subject.getSubject(AccessController.getContext());
            String caller = "anonymous";
            if (subject != null) {
                caller = "";
                for (Principal principal : subject.getPrincipals()) {
                    caller += principal.getName() + " ";
                }
            }
        }
        return super.invoke(s, objects, strings);
    }
}
