package com.hippo.spring.xbean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.CachedIntrospectionResults;

import com.hippo.broker.BrokerFactory;
import com.hippo.broker.BrokerService;

/**
 * 
 * @author saitxuc
 * 2015-4-2
 */
public class XBeanBrokerService extends BrokerService {
	
	private boolean start;

    public XBeanBrokerService() {
        start = BrokerFactory.getStartDefault();
    }

    /**
     *
     * @throws Exception
     * @org.apache.xbean.InitMethod
     */
    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        if (shouldAutostart()) {
            start();
        }
    }

    protected boolean shouldAutostart() {
        return start;
    }

    /**
     *
     * @throws Exception
     * @org.apache.xbean.DestroyMethod
     */
    @PreDestroy
    public void destroy() throws Exception {
        stop();
    }

    @Override
    public void stop()  {
        // must clear this Spring cache to avoid any memory leaks
        CachedIntrospectionResults.clearClassLoader(getClass().getClassLoader());
        super.stop();
    }


    public void setStart(boolean start) {
        this.start = start;
    }
	
}
