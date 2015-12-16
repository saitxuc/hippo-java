package com.pinganfu.hippo.common.util;

/**
 * 
 * @author saitxuc
 *
 */
import java.util.regex.Pattern;

public final class JMXSupport {

    private static final Pattern PART_1 = Pattern.compile("[\\:\\,\\'\\\"]");
    private static final Pattern PART_2 = Pattern.compile("\\?");
    private static final Pattern PART_3 = Pattern.compile("=");
    private static final Pattern PART_4 = Pattern.compile("\\*");

    private JMXSupport() {
    }

    public static String encodeObjectNamePart(String part) {
        String answer = PART_1.matcher(part).replaceAll("_");
        answer = PART_2.matcher(answer).replaceAll("&qe;");
        answer = PART_3.matcher(answer).replaceAll("&amp;");
        answer = PART_4.matcher(answer).replaceAll("&ast;");
        return answer;
    }

}
