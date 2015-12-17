package com.hippo.common.store;

import java.util.Comparator;

import org.apache.commons.lang.StringUtils;

public class ZSetEntryComparator implements Comparator<ReZSetEntry> {
    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(ReZSetEntry a, ReZSetEntry b) {
        Object aScore = a.getScore();
        Object bScore = b.getScore();
        if (StringUtils.isNumeric(aScore.toString()) && StringUtils.isNumeric(bScore.toString())) {
            double aDouble = Double.parseDouble(aScore.toString());
            double bDouble = Double.parseDouble(bScore.toString());
            if (aDouble > bDouble) {
                return -1;
            } else if (aDouble < bDouble) {
                return 1;
            } else {
                if (a.getKey() != null && b.getKey() != null) {
                    if (a.getKey() instanceof String && b.getKey() instanceof String) {
                        return ((String) a.getKey()).compareTo((String) b.getKey());
                    }
                }
                return 0;
            }
        } else {
            return 0;
        }
    }
}
