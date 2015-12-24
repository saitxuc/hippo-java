package com.hippo.jmx;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;

import com.hippo.jmx.filter.MBeansAttributeQueryFilter;
import com.hippo.jmx.filter.MBeansObjectNameQueryFilter;

/**
 * 
 * @author saitxuc
 * 2015-3-20
 */
public class JmxMBeansUtil {
	
    public static List getAllBrokers(MBeanServerConnection jmxConnection) throws Exception {
        return (new MBeansObjectNameQueryFilter(jmxConnection)).query("type=Broker,brokerName=*");
    }
	
    public static List getBrokersByName(MBeanServerConnection jmxConnection, String brokerName) throws Exception {
        return (new MBeansObjectNameQueryFilter(jmxConnection)).query("type=Broker,brokerName=" + brokerName);
    }
	
    public static List getBrokersByName(MBeanServerConnection jmxConnection, String brokerName, Set attributes) throws Exception {
        return (new MBeansAttributeQueryFilter(jmxConnection, attributes, new MBeansObjectNameQueryFilter(jmxConnection))).query("type=Broker,brokerName=" + brokerName);
    }
	
}
