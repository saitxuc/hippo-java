package com.pinganfu.hippo.common.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.pinganfu.hippo.common.domain.BucketInfo;

public class ListUtils {
    public static List<String> getDiffItemsFromSource(List<String> sourceList, List<String> checkList) {
        List<String> result = new ArrayList<String>();

        if (sourceList == null) {
            sourceList = new ArrayList<String>();
        }

        if (checkList == null) {
            checkList = new ArrayList<String>();
        }

        for (String str : sourceList) {
            if (!checkList.contains(str)) {
                result.add(str);
            }
        }

        return result;
    }

    public static List<BucketInfo> getDiffItemsFromSourceBucket(List<BucketInfo> sourceList, List<BucketInfo> checkList) {
        List<BucketInfo> result = new ArrayList<BucketInfo>();

        if (sourceList == null) {
            sourceList = new ArrayList<BucketInfo>();
        }

        if (checkList == null) {
            checkList = new ArrayList<BucketInfo>();
        }

        for (BucketInfo sBucket : sourceList) {
            boolean contain = false;
            for (BucketInfo checkBucket : checkList) {
                if (checkBucket.getBucketNo() == sBucket.getBucketNo()) {
                    contain = true;
                    break;
                }
            }
            if (!contain) {
                result.add(sBucket);
            }
        }

        return result;
    }

    public static List setToList(Set set) {
        List list = new ArrayList();
        if (set != null) {
            list.addAll(set);
        }
        return list;
    }

    public static Set listToSet(List list) {
        Set set = new HashSet();
        if (list != null) {
            set.addAll(list);
        }
        return set;
    }
    
    public static Set<String> copySet(Set<String> set) {
        Set<String> target = new HashSet<String>();
        Iterator<String> iter = set.iterator();
        while(iter.hasNext()) {
            target.add(iter.next());
        }
        return target;
    }
}
