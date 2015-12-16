package com.pinganfu.hippo.spring;

import java.io.File;
import java.net.MalformedURLException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

/**
 * 
 * @author saitxuc
 * 2015-4-2
 */
public class Utils {
	
    public static Resource resourceFromString(String uri) throws MalformedURLException {
        Resource resource;
        File file = new File(uri);
        if (file.exists()) {
            resource = new FileSystemResource(uri);
        } else if (ResourceUtils.isUrl(uri)) {
            resource = new UrlResource(uri);
        } else {
            resource = new ClassPathResource(uri);
        }
        return resource;
    }
	
}
