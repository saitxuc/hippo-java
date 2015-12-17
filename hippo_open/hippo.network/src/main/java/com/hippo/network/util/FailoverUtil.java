package com.hippo.network.util;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class FailoverUtil{

	public static String[] FetchUrls(String failoverUrl) throws Exception {
		URI uri = new URI(failoverUrl);
		String ssp = stripPrefix(uri.getRawSchemeSpecificPart().trim(), "//").trim();
		ssp = ssp.substring(1,ssp.length()-1);
		String[] urls = splitComponents(ssp);
		return urls;
	}
	
	public static String stripPrefix(String value, String prefix) {
        if (value.startsWith(prefix)) {
            return value.substring(prefix.length());
        }
        return value;
 }

 
 private static String[] splitComponents(String str) {
        List<String> l = new ArrayList<String>();

        int last = 0;
        int depth = 0;
        char chars[] = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            switch (chars[i]) {
            case '(':
                depth++;
                break;
            case ')':
                depth--;
                break;
            case ',':
                if (depth == 0) {
                    String s = str.substring(last, i);
                    l.add(s);
                    last = i + 1;
                }
                break;
            default:
            }
        }

        String s = str.substring(last);
        if (s.length() != 0) {
            l.add(s);
        }

        String rc[] = new String[l.size()];
        l.toArray(rc);
        return rc;
    }
 

}
