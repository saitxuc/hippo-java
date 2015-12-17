package com.hippo.common.domain;

import java.util.HashSet;
import java.util.Set;

public class BucketInfo implements Comparable<BucketInfo> {
    private Integer bucketNo;
    private boolean isSlave;

    public BucketInfo(Integer bucketNo, boolean isSlave) {
        super();
        this.bucketNo = bucketNo;
        this.isSlave = isSlave;
    }

    public Integer getBucketNo() {
        return bucketNo;
    }

    public void setBucketNo(Integer bucketNo) {
        this.bucketNo = bucketNo;
    }

    public boolean isSlave() {
        return isSlave;
    }

    public void setSlave(boolean isSlave) {
        this.isSlave = isSlave;
    }
    
    @Override
    public String toString() {
        return "bucket: " + bucketNo + ", isSlave: " + isSlave;
    }

    @Override
    public int compareTo(BucketInfo o) {
        if(o == null) {
            return -1;
        }
        if(this.bucketNo == o.getBucketNo() && this.isSlave == o.isSlave) {
            return 0;
        }
        return -1;
    }
    
    @Override
    public boolean equals(Object obj) {
        if(obj != null && obj instanceof BucketInfo) {
            BucketInfo target = (BucketInfo) obj;
            if(this.bucketNo == target.getBucketNo() && this.isSlave == target.isSlave) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        if(this.isSlave) {
            return this.bucketNo.hashCode() * -1;
        } else {
            return this.bucketNo.hashCode();
        }
    }
    
    public static void main(String[] args) {
        BucketInfo bi = new BucketInfo(1, false);
        Set<BucketInfo> set = new HashSet<BucketInfo>();
        set.add(bi);
        BucketInfo bi2 = new BucketInfo(1, false);
        //set.add(bi2);
        System.out.println(set.size());
        System.out.println(set.contains(bi2));
        
    }
}
