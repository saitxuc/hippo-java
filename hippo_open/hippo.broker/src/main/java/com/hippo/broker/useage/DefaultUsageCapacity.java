package com.hippo.broker.useage;

/**
 * 
 * @author saitxuc
 * write 2014-8-13
 */
public class DefaultUsageCapacity implements UsageCapacity{

   private long limit;
   

   public boolean isLimit(long size) {
       return size >= limit;
   }

   
   /**
    * @return the limit
    */
   public final long getLimit(){
       return this.limit;
   }

   
   /**
    * @param limit the limit to set
    */
   public final void setLimit(long limit){
       this.limit=limit;
   }
}
