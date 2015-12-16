package com.pinganfu.hippo.common.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.domain.BucketInfo;

public class ServerTableUtil {
    private static final Logger log = LoggerFactory.getLogger(ServerTableUtil.class);

    public final static String INVALID_FLAG = "0";
    
    public static Map<Integer, Vector<String>> copyTable(Map<Integer, Vector<String>> hash_table_src) {
        if (null == hash_table_src) {
            return null;
        }
        Map<Integer, Vector<String>> hash_table_dest = new HashMap<Integer, Vector<String>>();
        Iterator<Entry<Integer, Vector<String>>> iter = hash_table_src.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Integer, Vector<String>> next = iter.next();
            Vector<String> v = hash_table_dest.get(next.getKey());
            if (null == v) {
                v = new Vector<String>();
            }
            v.addAll(next.getValue());
            hash_table_dest.put(next.getKey(), v);
        }
        return hash_table_dest;
    }

    /**
     * My slaved buckets
     * @param server
     * @return
     */
    public static Set<Integer> getSlavedBuckets(String server, Map<Integer, Vector<String>> table) {
        Set<Integer> slaveBuckets = new HashSet<Integer>();
        if (null == table || null == server) {
            return slaveBuckets;
        }
        int copyCnt = table.keySet().size();
        int hashCnt = table.get(0).size();
        for (int i = 1; i < copyCnt; i++) {
            Vector<String> v = table.get(i);
            for (int j = 0; j < hashCnt; j++) {
                if (server.equals(v.get(j))) {
                    slaveBuckets.add(j);
                }
            }
        }
        return slaveBuckets;
    }

    /**
     * Get slave buckets map of server, key: master url; value: bucket list
     * @param server
     * @param table
     * @return
     */
    public static Map<String, Set<String>> getSlaveBucketMapOfServer(String server, Map<Integer, Vector<String>> table) {
        Map<String, Set<String>> slavedBucketMap = new HashMap<String, Set<String>>();
        Set<Integer> slavedBuckets = getSlavedBuckets(server, table);
        Iterator<Integer> bucketIter = slavedBuckets.iterator();
        while (bucketIter.hasNext()) {
            Integer bucket = bucketIter.next();
            String master = getMasterOfBucket(bucket, table);
            Set<String> buckets = slavedBucketMap.get(master);
            if (null == buckets) {
                buckets = new HashSet<String>();
                slavedBucketMap.put(master, buckets);
            }
            buckets.add(bucket + "");
        }
        return slavedBucketMap;
    }

    /**
     * Get new migrate buckets map
     * @param slave
     * @param mtable
     * @param dtable
     * @return map of key: master; value: bucket set
     */
    public static Map<String, Set<String>> getNewMigrateBucketMapOfServer(String slave, Map<Integer, Vector<String>> mtable, Map<Integer, Vector<String>> dtable) {
        Map<String, Set<String>> needMigMap = ServerTableUtil.getNeedMigrateBucketMapOfServer(slave, mtable, dtable);

        // 20150515
        Iterator<Set<String>> migBucketsCollIter = needMigMap.values().iterator();
        Set<String> migBuckets = new HashSet<String>();
        while (migBucketsCollIter.hasNext()) {
            migBuckets.addAll(migBucketsCollIter.next());
        }

        Map<String, Set<String>> slaveMap = ServerTableUtil.getSlaveBucketMapOfServer(slave, dtable);
        Iterator<Entry<String, Set<String>>> slaveIter = slaveMap.entrySet().iterator();
        while (slaveIter.hasNext()) {
            Entry<String, Set<String>> entry = slaveIter.next();
            String master = entry.getKey();
            Set<String> addBuckets = entry.getValue();

            // 20150515
            addBuckets.removeAll(migBuckets);
            if (0 == addBuckets.size()) {
                continue;
            }

            Set<String> allBuckets = needMigMap.get(master);
            if (null == allBuckets) {
                needMigMap.put(master, addBuckets);
            } else {
                allBuckets.addAll(addBuckets);
            }
        }
        return needMigMap;
    }

    /**
     * 
     * @param slaveBuckets
     * @param table
     * @return Map of key: master server; value: bucket list
     */
    public static Map<String, List<String>> getMigrateBucketMap(List<BucketInfo> slaveBuckets, Map<Integer, Vector<String>> table) {
        Map<String, List<String>> migBucketMap = new HashMap<String, List<String>>();

        Iterator<BucketInfo> iter = slaveBuckets.iterator();
        int copyCnt = table.size();
        while (iter.hasNext()) {
            int bucket = iter.next().getBucketNo();
            for (int i = 0; i < copyCnt; i++) {
                Vector<String> masterV = table.get(i);
                String master = masterV.get(bucket);
                if (null != master && !INVALID_FLAG.equals(master)) {
                    List<String> buckets = migBucketMap.get(master);
                    if (null == buckets) {
                        buckets = new ArrayList<String>();
                        migBucketMap.put(master, buckets);
                    }
                    buckets.add(bucket + "");
                    break;
                }
            }
        }

        return migBucketMap;
    }

    /**
     * Get server's need migrate bucket map, key: destination server, value: buckets
     * @param server
     * @return
     */
    public static Map<String, Set<String>> getNeedMigrateBucketMapOfServer(String server, Map<Integer, Vector<String>> mtable, Map<Integer, Vector<String>> dtable) {
        if (null == server || null == mtable || null == dtable) {
            return null;
        }

        // TODO: check if mtable is all 0

        int copyCnt = dtable.keySet().size();
        int hashCnt = dtable.get(0).size();

        Set<String> myOwnedBuckets = new HashSet<String>();
        for (int i = 0; i < copyCnt; i++) {
            for (int j = 0; j < hashCnt; j++) {
                if (server.equals(mtable.get(i).get(j))) {
                    myOwnedBuckets.add(j + "");
                }
            }
        }

        Map<Integer, Set<String>> mtableBucketServersMap = new HashMap<Integer, Set<String>>();
        for (int j = 0; j < hashCnt; j++) {
            Set<String> servers = new HashSet<String>();
            mtableBucketServersMap.put(j, servers);
            for (int i = 1; i < copyCnt; i++) {
                servers.add(mtable.get(i).get(j));
            }
        }

        Set<String> needMigBuckets = new HashSet<String>();
        for (int j = 0; j < hashCnt; j++) {
            for (int i = 0; i < copyCnt; i++) {
                // if server exist in dtable and not found in mtable, need migrate
                if (server.equals(dtable.get(i).get(j)) && !mtableBucketServersMap.get(j).contains(server)) {
                    needMigBuckets.add(j + "");
                }
            }
        }
        needMigBuckets.removeAll(myOwnedBuckets);

        Map<String, Set<String>> needMigBucketMap = new HashMap<String, Set<String>>();

        Map<String, Set<Integer>> machineMigBuckets = ServerTableUtil.getMasterToBeMigratedBucketMap(mtable, dtable);

        /*
        if(needMigBuckets.size() > 0 && (null == machineMigBuckets || 0 == machineMigBuckets.size())) {
            // Migrate from myself
            List<String> needMigBucketList = new ArrayList<String>();
            needMigBucketList.addAll(needMigBuckets);
            needMigBucketMap.put(server, needMigBucketList);
            return needMigBucketMap;
        }*/

        Iterator<String> iter = needMigBuckets.iterator();
        while (iter.hasNext()) {
            int bucket = Integer.parseInt(iter.next());
            Iterator<Entry<String, Set<Integer>>> iterMigMachineBuckets = machineMigBuckets.entrySet().iterator();
            String foundServer = null;
            while (iterMigMachineBuckets.hasNext()) {
                Entry<String, Set<Integer>> entry = iterMigMachineBuckets.next();
                Iterator<Integer> buckets = entry.getValue().iterator();
                while (buckets.hasNext()) {
                    if (buckets.next().intValue() == bucket) {
                        foundServer = entry.getKey();
                        break;
                    }
                }
                if (foundServer != null) {
                    break;
                }
            }

            if (foundServer != null) {
                Set<String> buckets = needMigBucketMap.get(foundServer);
                if (null == buckets) {
                    buckets = new HashSet<String>();
                    needMigBucketMap.put(foundServer, buckets);
                }
                buckets.add(bucket + "");
            }
        }

        return needMigBucketMap;
    }

    public static Set<String> getNeedMigrateSlaves(int bucket, Map<Integer, Vector<String>> mtable, Map<Integer, Vector<String>> dtable) {
        if (null == mtable || null == dtable) {
            return null;
        }
        int copyCnt = dtable.keySet().size();
        int hashCnt = dtable.get(0).size();
        if (bucket > hashCnt) {
            log.error(" invalid bucket: {}", bucket);
            return null;
        }
        Set<String> mServers = new HashSet<String>();
        Set<String> dServers = new HashSet<String>();
        for (int i = 0; i < copyCnt; i++) {
            mServers.add(mtable.get(i).get(bucket));
            dServers.add(dtable.get(i).get(bucket));
        }
        dServers.removeAll(mServers);
        return dServers;
    }

    public static Set<BucketInfo> getMyMasterBuckets(String server, Map<Integer, Vector<String>> table) {
        Set<BucketInfo> mBuckets = new HashSet<BucketInfo>();
        if (table != null) {
            Vector<String> buckets = table.get(0);
            for (int i = 0; i < buckets.size(); i++) {
                if (server.equals(buckets.get(i))) {
                    BucketInfo info = new BucketInfo(i, false);
                    mBuckets.add(info);
                }
            }
        }
        return mBuckets;
    }

    public static Set<BucketInfo> getMySlaveBuckets(String server, Map<Integer, Vector<String>> table) {
        Set<BucketInfo> sBuckets = new HashSet<BucketInfo>();
        int copyCnt = table.size();
        int bucketCnt = table.get(0).size();
        for (int j = 1; j < copyCnt; j++) {
            for (int i = 0; i < bucketCnt; i++) {
                if (server.equals(table.get(j).get(i))) {
                    BucketInfo info = new BucketInfo(i, true);
                    sBuckets.add(info);
                }
            }
        }
        return sBuckets;
    }

    public static String getMasterOfBucket(int bucket, Map<Integer, Vector<String>> table) {
        if (table != null) {
            return table.get(0).get(bucket);
        } else {
            return null;
        }
    }

    /**
     * Get map of machine's migrate buckets
     * @param mtable
     * @param dtable
     * @return Map of key: master server id, value: host buckets need to be migrated
     */
    public static Map<String, Set<Integer>> getMasterToBeMigratedBucketMap(Map<Integer, Vector<String>> mtable, Map<Integer, Vector<String>> dtable) {
        if (null == mtable || null == dtable) {
            return null;
        }
        int copyCnt = mtable.keySet().size();
        int hashCnt = mtable.get(0).size();
        int migrateCnt = 0;
        Map<String, Integer> migrateMachine = new HashMap<String, Integer>();
        Map<String, Set<Integer>> migrateMachineBuckets = new HashMap<String, Set<Integer>>();
        for (int i = 0; i < hashCnt; i++) {
            boolean migrateThisBucket = false;
            // "0" means invalid server
            if (!INVALID_FLAG.equals(mtable.get(0).get(i))) {
                // equals
                if (!mtable.get(0).get(i).equals(dtable.get(0).get(i))) {
                    migrateThisBucket = true;
                } else {
                    for (int j = 1; j < copyCnt; j++) {
                        boolean found = false;
                        for (int k = 1; k < copyCnt; k++) {
                            if (dtable.get(j).get(i).equals(mtable.get(k).get(i))) {
                                found = true;
                                break;
                            }
                        }
                        if (found == false) {
                            migrateThisBucket = true;
                            break;
                        }
                    }
                }
                // 20150703: special case: servers are same just sorted different
                if(migrateThisBucket) {
                    Set<String> allServers = new HashSet<String>();
                    for (int z = 0; z < copyCnt; z++) {
                        allServers.add(dtable.get(z).get(i));
                    }
                    for (int z = 0; z < copyCnt; z++) {
                        allServers.remove(mtable.get(z).get(i));
                    }
                    if(0 == allServers.size()) {
                        migrateThisBucket = false;
                    }
                }
                // 20150703: end
                if (migrateThisBucket) {
                    String machine = mtable.get(0).get(i);
                    Object machineCnt = migrateMachine.get(machine);
                    if (null == machineCnt) {
                        migrateMachine.put(machine, 1);
                    } else {
                        int cnt = (Integer) machineCnt;
                        cnt++;
                        migrateMachine.put(machine, cnt);
                    }
                    migrateCnt++;
                    String server = mtable.get(0).get(i);

                    // log.info("added migrating machine: {} bucket {} ", server, i);

                    Set<Integer> buckets = migrateMachineBuckets.get(server);
                    if (buckets == null) {
                        buckets = new HashSet<Integer>();
                        migrateMachineBuckets.put(server, buckets);
                    }
                    buckets.add(i);
                }
            }
        }
        return migrateMachineBuckets;
    }

    public static boolean isEmptyTable(Map<Integer, Vector<String>> table) {
        if (null == table) {
            return true;
        }
        int copyCnt = table.size();
        if (0 == copyCnt) {
            return true;
        }
        int hashCnt = table.get(0).size();
        for (int i = 0; i < copyCnt; i++) {
            for (int j = 0; j < hashCnt; j++) {
                if (!INVALID_FLAG.equals(table.get(i).get(j))) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public static Map<Integer, Vector<String>> cutForCtable(final Map<Integer, Vector<String>> table) {
        Map<Integer, Vector<String>> ctable = copyTable(table);
        int copyCnt = ctable.size();
        int hashCnt = ctable.get(0).size();
        for(int i=0; i<copyCnt; i++) {
            for(int j=0; j<hashCnt; j++) {
                String serverId = ctable.get(i).get(j);
                if(serverId == null || INVALID_FLAG.equals(serverId)) {
                    continue;
                }
                int firstIdx = serverId.indexOf(':');
                int lastIdx = serverId.lastIndexOf(':');
                try {
                    serverId = serverId.substring(0, firstIdx) + serverId.substring(lastIdx);
                } catch(Exception e) {
                }
                ctable.get(i).set(j, serverId);
            }
        }
        return ctable;
    }
}
