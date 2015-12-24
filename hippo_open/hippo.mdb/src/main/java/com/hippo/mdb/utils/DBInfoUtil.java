package com.hippo.mdb.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import com.hippo.mdb.obj.OffsetInfo;

public class DBInfoUtil {
    public static void sortOffsets(List<OffsetInfo> list) {
        //System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        Collections.sort(list, new Comparator<OffsetInfo>() {
            public int compare(OffsetInfo fo, OffsetInfo to) {
                if (null == fo || null == to) {
                    return 0;
                }
                long expire = fo.getExpireTime() - to.getExpireTime();
                if (expire == 0) {
                    return 0;
                } else {
                    if (expire > 0) {
                        return 1;
                    } else {
                        return -1;
                    }
                }

            }
        });
    }
}
