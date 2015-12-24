package com.hippo.jmx;

import java.util.Hashtable;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.hippo.common.util.JMXSupport;

public class BrokerMBeanSupport {
	public static String StoreEngineAdapterObjectName ;
    // MBean Name Creation

    public static ObjectName createBrokerObjectName(String jmxDomainName, String brokerName) throws MalformedObjectNameException  {
        String objectNameStr = jmxDomainName + ":type=Broker,brokerName=";
        objectNameStr += JMXSupport.encodeObjectNamePart(brokerName);
        return new ObjectName(objectNameStr);
    }

    public static ObjectName createConnectorName(ObjectName brokerObjectName, String type, String name) throws MalformedObjectNameException {
        return createConnectorName(brokerObjectName.toString(), type, name);
    }

    public static ObjectName createConnectorName(String brokerObjectName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",connector=" + type + ",connectorName="+ JMXSupport.encodeObjectNamePart(name);
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createConnectionViewByType(ObjectName connectorName, String type, String name) throws MalformedObjectNameException {
        String objectNameStr = connectorName.toString();
        objectNameStr += ",connectionViewType=" + JMXSupport.encodeObjectNamePart(type);
        objectNameStr += ",connectionName="+ JMXSupport.encodeObjectNamePart(name);
        return new ObjectName(objectNameStr);
    }

    public static ObjectName createHealthServiceName(ObjectName brokerObjectName) throws MalformedObjectNameException {
        return createHealthServiceName(brokerObjectName.toString());
    }

    public static ObjectName createHealthServiceName(String brokerObjectName) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;
        objectNameStr += ",service=Health";
        ObjectName objectName = new ObjectName(objectNameStr);
        return objectName;
    }

    public static ObjectName createStoreEngineAdapterName(String brokerObjectName, String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;

        objectNameStr += "," + "Service=StoreEngineAdapter";
        objectNameStr += "," + "InstanceName=" + JMXSupport.encodeObjectNamePart(name);
        StoreEngineAdapterObjectName = objectNameStr;
        return new ObjectName(objectNameStr);
    }

}
