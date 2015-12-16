package com.pinganfu.hippo.spring.xbean;

import java.beans.PropertyEditorManager;
import java.net.MalformedURLException;
import java.net.URI;

import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.apache.xbean.spring.context.impl.URIEditor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.pinganfu.hippo.broker.BrokerFactoryHandler;
import com.pinganfu.hippo.broker.BrokerService;
import com.pinganfu.hippo.common.util.IntrospectionSupport;
import com.pinganfu.hippo.common.util.URISupport;
import com.pinganfu.hippo.spring.Utils;

import org.springframework.core.io.Resource;


/**
 * 
 * @author saitxuc
 * 2015-4-2
 */
public class XBeanBrokerFactory implements BrokerFactoryHandler {
	
	protected static final Logger LOG = LoggerFactory.getLogger(XBeanBrokerFactory.class);
	
	private boolean validate = true;
	
    static {
        PropertyEditorManager.registerEditor(URI.class, URIEditor.class);
    }
	
	@Override
	public BrokerService createBroker(URI config) throws Exception {
		String uri = config.getSchemeSpecificPart();
        if (uri.lastIndexOf('?') != -1) {
            IntrospectionSupport.setProperties(this, URISupport.parseQuery(uri));
            uri = uri.substring(0, uri.lastIndexOf('?'));
        }

        ApplicationContext context = createApplicationContext(uri);

        BrokerService broker = null;
        try {
            broker = (BrokerService)context.getBean("broker");
        } catch (BeansException e) {
        }

        if (broker == null) {
            // lets try find by type
            String[] names = context.getBeanNamesForType(BrokerService.class);
            for (int i = 0; i < names.length; i++) {
                String name = names[i];
                broker = (BrokerService)context.getBean(name);
                if (broker != null) {
                    break;
                }
            }
        }
        if (broker == null) {
            throw new IllegalArgumentException("The configuration has no BrokerService instance for resource: " + config);
        }
        
        if (broker instanceof ApplicationContextAware) {
        	((ApplicationContextAware)broker).setApplicationContext(context);
        }
        
        // TODO warning resources from the context may not be closed down!

        return broker;
	}
	
    
    public boolean isValidate() {
        return validate;
    }

    public void setValidate(boolean validate) {
        this.validate = validate;
    }
	
	protected ApplicationContext createApplicationContext(String uri) throws MalformedURLException {
        Resource resource = Utils.resourceFromString(uri);
        LOG.debug("Using " + resource + " from " + uri);
        try {
            return new ResourceXmlApplicationContext(resource) {
                @Override
                protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
                    reader.setValidating(isValidate());
                }
            };
        } catch (FatalBeanException errorToLog) {
            LOG.error("Failed to load: " + resource + ", reason: " + errorToLog.getLocalizedMessage(), errorToLog);
            throw errorToLog;
        }
    }
	
}
