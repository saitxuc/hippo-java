package com.pinganfu.hippoconsoleweb.tablebuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippoconsoleweb.service.ConsoleConstants;

public class GroupInfo {
    boolean debug = true;

    private static final Logger log = LoggerFactory.getLogger(GroupInfo.class);
    
    public Map<Integer, Vector<String>> hash_table;
    public Map<Integer, Vector<String>> m_hash_table;
    public Map<Integer, Vector<String>> d_hash_table;

    private int server_copy_count;
    private int server_bucket_count;

    private Map<String, Set<Integer>> migrateMachineBuckets = new HashMap<String, Set<Integer>>();
    private Map<String, Integer> migrate_machine = new HashMap<String, Integer>();
    
    
    public GroupInfo(int server_copy_count, int server_bucket_count) {
        this.server_copy_count = server_copy_count;
        this.server_bucket_count = server_bucket_count;
        hash_table = this.buildEmptyMap();
        m_hash_table = this.buildEmptyMap();
        d_hash_table = this.buildEmptyMap();
    }

    public Map<Integer, Vector<String>> buildEmptyMap() {
        Map<Integer, Vector<String>> tableMap = new HashMap<Integer, Vector<String>>();
        for (int i = 0; i < get_copy_count(); i++) {
            Vector<String> servers = new Vector<String>();
            tableMap.put(i, servers);
            for (int j = 0; j < get_server_bucket_count(); j++) {
                servers.add(ConsoleConstants.INVALID_FLAG);
            }
        }
        return tableMap;
    }
    
    public int fillMigrateMachine() {
        int migrate_count = 0;
        migrate_machine.clear();
        migrateMachineBuckets.clear();
        for (int i = 0; i < get_server_bucket_count(); i++) {
            boolean migrate_this_bucket = false;
            /// TODO: check
            if (!ConsoleConstants.INVALID_FLAG.equals(m_hash_table.get(0).get(i))) {
                // equals
                if (!m_hash_table.get(0).get(i).equals(d_hash_table.get(0).get(i))) {
                    migrate_this_bucket = true;
                } else {
                    for (int j = 1; j < get_copy_count(); j++) {
                        boolean found = false;
                        for (int k = 1; k < get_copy_count(); k++) {
                            if (d_hash_table.get(j).get(i).equals(m_hash_table.get(k).get(i))) {
                                found = true;
                                break;
                            }
                        }
                        if (found == false) {
                            migrate_this_bucket = true;
                            break;
                        }
                    }
                }
                if (migrate_this_bucket) {
                    String machine = m_hash_table.get(0).get(i);
                    Object machineCnt = migrate_machine.get(machine);
                    if (null == machineCnt) {
                        migrate_machine.put(machine, 1);
                    } else {
                        int cnt = (Integer) machineCnt;
                        cnt++;
                        migrate_machine.put(machine, cnt);
                    }
                    migrate_count++;
                    String server = m_hash_table.get(0).get(i);
                    log.info("added migrating machine: {} bucket {} ", server, i);
                    
                    Set<Integer> buckets = migrateMachineBuckets.get(server);
                    if(buckets == null) {
                        buckets = new HashSet<Integer>();
                        migrateMachineBuckets.put(server, buckets);
                    }
                    buckets.add(i);
                }
            }
        }
        return migrate_count;
    }
    
    public Map<String, Set<Integer>> getMigrateMachineBuckets() {
        return migrateMachineBuckets;
    }

    public void setMigrateMachineBuckets(Map<String, Set<Integer>> migrateMachineBuckets) {
        this.migrateMachineBuckets = migrateMachineBuckets;
    }

    public Map<String, Integer> getMigrate_machine() {
        return migrate_machine;
    }

    public void setMigrate_machine(Map<String, Integer> migrate_machine) {
        this.migrate_machine = migrate_machine;
    }
    

    public int get_server_bucket_count() {
        return server_bucket_count != 0 ? server_bucket_count : 0;
    }

    public int get_copy_count() {
        return server_copy_count != 0 ? server_copy_count : 0;
    }
    
}
