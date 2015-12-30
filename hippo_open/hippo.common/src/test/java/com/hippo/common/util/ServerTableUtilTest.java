package com.hippo.common.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import junit.framework.TestCase;

import com.hippo.common.domain.BucketInfo;

public class ServerTableUtilTest extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }
    
    public void testGetMySlaveBucketMap() {
        Map<Integer, Vector<String>> mtable = new HashMap<Integer, Vector<String>>();
        Vector<String> nodeV = new Vector<String>();
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("A");
        mtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("A");
        nodeV.add("B");
        nodeV.add("B");
        mtable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("C");
        mtable.put(2, nodeV);
        
        System.out.println("mtable: " + mtable);
        System.out.println("A's slave bucket map: " + ServerTableUtil.getSlaveBucketMapOfServer("A", mtable));
    }
    
    public void testGetMigrateBucketMapOfServer() {

        Map<Integer, Vector<String>> dtable = new HashMap<Integer, Vector<String>>();
        Vector<String> nodeV = new Vector<String>();
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("A");
        dtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("A");
        nodeV.add("B");
        nodeV.add("B");
        dtable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("C");
        dtable.put(2, nodeV);
        

        /*
        Map<Integer, Vector<String>> mtable = new HashMap<Integer, Vector<String>>();
        nodeV = new Vector<String>();
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("A");
        mtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("0");
        nodeV.add("C");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("B");
        mtable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("B");
        nodeV.add("0");
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("0");
        mtable.put(2, nodeV);
        */
        Map<Integer, Vector<String>> mtable = new HashMap<Integer, Vector<String>>();
        nodeV = new Vector<String>();
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        mtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        mtable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        mtable.put(2, nodeV);
        
        System.out.println(mtable);
        System.out.println(dtable);
        
        System.out.println("A need mig from: " + ServerTableUtil.getNeedMigrateBucketMapOfServer("A", mtable, dtable));
    }

    public void testGetMigrateMachineBuckets2() {
        Map<Integer, Vector<String>> dtable = new HashMap<Integer, Vector<String>>();
        Vector<String> nodeV = new Vector<String>();
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("B");
        dtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("B");
        nodeV.add("A");
        dtable.put(1, nodeV);
        

        Map<Integer, Vector<String>> mtable = new HashMap<Integer, Vector<String>>();
        nodeV = new Vector<String>();
        nodeV.add("A");
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("B");
        mtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("B");
        nodeV.add("A");
        mtable.put(1, nodeV);
        
        System.out.println("m: " + mtable);
        System.out.println("d: " + dtable);
        System.out.println("B: " + ServerTableUtil.getNeedMigrateBucketMapOfServer("B", mtable, dtable));
        System.out.println("A: " + ServerTableUtil.getNeedMigrateBucketMapOfServer("A", mtable, dtable));
    }

    public void testGetMigrateMachineBuckets() {
        Map<Integer, Vector<String>> dtable = new HashMap<Integer, Vector<String>>();
        Vector<String> nodeV = new Vector<String>();
        nodeV.add("D");
        nodeV.add("B");
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("A");
        dtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("A");
        nodeV.add("B");
        nodeV.add("B");
        dtable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("C");
        dtable.put(2, nodeV);
        

        Map<Integer, Vector<String>> mtable = new HashMap<Integer, Vector<String>>();
        nodeV = new Vector<String>();
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("A");
        mtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        mtable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        nodeV.add("0");
        mtable.put(2, nodeV);
        
        /*
        Map<Integer, Vector<String>> mtable = new HashMap<Integer, Vector<String>>();
        nodeV = new Vector<String>();
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("A");
        mtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("A");
        nodeV.add("B");
        nodeV.add("B");
        mtable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("C");
        mtable.put(2, nodeV);
        */
        
        Map<String, Set<Integer>> migrateMachine = ServerTableUtil.getMasterToBeMigratedBucketMap(mtable, dtable);
        System.out.println(mtable);
        System.out.println(dtable);
        System.out.println(migrateMachine);
        
        System.out.println(ServerTableUtil.getNeedMigrateBucketMapOfServer("D", mtable, dtable));
    }
    
    public void testGetMigrateBucketMap() {

        Map<Integer, Vector<String>> dtable = new HashMap<Integer, Vector<String>>();
        Vector<String> nodeV = new Vector<String>();
        nodeV.add("0");
        nodeV.add("B");
        nodeV.add("C");
        nodeV.add("B");
        nodeV.add("A");
        dtable.put(0, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("A");
        nodeV.add("B");
        nodeV.add("B");
        dtable.put(1, nodeV);
        nodeV = new Vector<String>();
        nodeV.add("B");
        nodeV.add("A");
        nodeV.add("A");
        nodeV.add("C");
        nodeV.add("C");
        dtable.put(2, nodeV);

        List<BucketInfo> slaveBuckets = new ArrayList<BucketInfo>();
        slaveBuckets.add(new BucketInfo(0, true));
        slaveBuckets.add(new BucketInfo(1, true));
        slaveBuckets.add(new BucketInfo(2, true));
        System.out.println(ServerTableUtil.getMigrateBucketMap(slaveBuckets, dtable));
    }

}
