package com.hippoconsoleweb.tablebuilder;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippoconsoleweb.service.ConsoleConstants;

public class LbFirstTableBuilder extends AbstractTableBuilder {

    private static final Logger log = LoggerFactory.getLogger(LbFirstTableBuilder.class);

    private int tokens_per_node_min; //try my bset to make every data server handles tokens_per_node_min
    //or tokens_per_node_min + 1 buckets
    private int tokens_per_node_max_count; // how many data server handle tokenPerNode_min + 1 buckets
    private int tokens_per_node_min_count; // how many data server handles tokenPerNode_min buckets
    private int master_tokens_per_node_min;
    private int master_tokens_per_node_max_count;
    private int master_tokens_per_node_min_count;
    
    private int maxBucketCnt = Integer.MAX_VALUE;

    /**
     * @param dLostFlag: default is 0, not allow data lost. if 1: if one bucket lost all of copies, configserver will force rebuild table, and assign this bucket to alive dataservers
     * @param bPlaceFlag: default is 0, if 1: force buckets are not one to one correspondence in ds at first run
     */
    public LbFirstTableBuilder(int bucket_count, int copy_count) {
        this.setBucket_count(bucket_count);
        this.setCopy_count(copy_count);
    }

    @Override
    protected int is_this_node_OK(String node_id, int line_num, int node_idx, Map<Integer, Vector<String>> hash_table_dest, int option_level, boolean node_in_use) {
        if (is_node_availble(node_id) == false)
            return ConsoleConstants.INVALID_NODE;
        int turn = 0;
        if (node_in_use == true)
            turn = 1;
        if (line_num == 0) {
            if (mtokens_count_in_node.get(node_id) >= master_server_capable.get(node_id) + turn) {
                return ConsoleConstants.TOOMANY_MASTER;
            }
        } else if (option_level == ConsoleConstants.CONSIDER_ALL) { //line_num != 0
            if (tokens_count_in_node.get(node_id) >= server_capable.get(node_id) + turn) {
                return ConsoleConstants.TOOMANY_BUCKET;
            }
        } else { //line_num != 0 && option_level != CONSIDER_ALL
            if (option_level != ConsoleConstants.CONSIDER_FORCE && tokens_count_in_node_now.get(node_id) >= tokens_per_node_min + turn && max_count_now >= tokens_per_node_max_count + turn) {
                return ConsoleConstants.TOOMANY_BUCKET;
            }
        }
        if (line_num == 0 && option_level == ConsoleConstants.CONSIDER_BASE) {
            return ConsoleConstants.NODE_OK;
        }
        for (int i = 0; i < copy_count; i++) {
            if ((int) i != line_num) {
                if (hash_table_dest.get(i).elementAt(node_idx).equals(node_id)) {
                    return ConsoleConstants.SAME_NODE;
                }
                if (option_level < ConsoleConstants.CONSIDER_BASE) { //CONSIDER_ALL
                    // Always consider in same position
                    //if(hash_table_dest[i][node_idx].second == node_id.second) {
                    return ConsoleConstants.SAME_POS;
                    //}
                }
            }
        }
        return ConsoleConstants.NODE_OK;
    }

    @Override
    protected void caculate_capable() {
        int S = available_server.size();

        tokens_per_node_min = bucket_count * copy_count / S;
        tokens_per_node_max_count = bucket_count * copy_count - tokens_per_node_min * S;
        tokens_per_node_min_count = S - tokens_per_node_max_count;

        master_tokens_per_node_min = bucket_count / S;
        master_tokens_per_node_max_count = bucket_count - master_tokens_per_node_min * S;
        master_tokens_per_node_min_count = S - master_tokens_per_node_max_count;

        // log.debug("bucket_count: {}\n copy_count: {}\n tokens_per_node_min: {}\n tokens_per_node_max_count: {}\n " + "tokens_per_node_min_count: {}\n master_tokens_per_node_min: {}\n master_tokens_per_node_max_count: {}\n " + "master_tokens_per_node_min_count: {}", bucket_count, copy_count, tokens_per_node_min, tokens_per_node_max_count, tokens_per_node_min_count, master_tokens_per_node_min, master_tokens_per_node_max_count, master_tokens_per_node_min_count);

        server_capable.clear();
        master_server_capable.clear();
        int max_s = 0;
        int mmax_s = 0;
        int i = 0;
        int pre_node_count_min = 0;
        int pre_mnode_count_min = 0;
        int size = 0;
        int sum = 0;

        Iterator<Entry<String, Integer>> it = tokens_count_in_node.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Integer> entry = it.next();
            if (entry.getValue() != 0) {
                size++;
                sum += entry.getValue();
            }
        }
        if (size > 0) {
            pre_node_count_min = sum / size;
        }
        size = sum = 0;
        it = mtokens_count_in_node.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Integer> entry = it.next();
            if (entry.getValue() != 0) {
                size++;
                sum += entry.getValue();
            }
        }
        if (size > 0) {
            pre_mnode_count_min = sum / size;
        }

        log.debug("pre_node_count_min: {}, pre_mnode_count_min: {}", pre_node_count_min, pre_mnode_count_min);

        Iterator<String> iter = available_server.iterator();
        while (iter.hasNext()) {
            String server = iter.next();
            int last_node_count = tokens_count_in_node.get(server);
            //log.debug("will caculate server %s:%d last token is %d ",
            //    tbsys::CNetUtil::addrToString(it->first).c_str(), it->first, last_node_count);

            //log.debug("pre_node_count_min = %d pre_mnode_count_min=%d",pre_node_count_min, pre_mnode_count_min);
            if (last_node_count <= pre_node_count_min) { //try my best to make every data sever handle tokenPerNode_min buckets.
                //log_debug("try to make ist min");
                int min_s = i - max_s;
                if (min_s < tokens_per_node_min_count) {
                    server_capable.put(server, tokens_per_node_min);
                } else {
                    server_capable.put(server, tokens_per_node_min + 1);
                    max_s++;
                }
            } else {
                //log_debug("try to make ist max");
                if (max_s < tokens_per_node_max_count) {
                    server_capable.put(server, tokens_per_node_min + 1);
                    max_s++;
                } else {
                    server_capable.put(server, tokens_per_node_min);
                }
            }

            int last_mnode_count = mtokens_count_in_node.get(server);
            if (last_mnode_count <= pre_mnode_count_min) { //try my best to make every data sever handle tokenPerNode_min buckets.
                int mmin_s = i - mmax_s;
                if (mmin_s < master_tokens_per_node_min_count) {
                    master_server_capable.put(server, master_tokens_per_node_min);
                } else {
                    master_server_capable.put(server, master_tokens_per_node_min + 1);
                    mmax_s++;
                }
            } else {
                if (mmax_s < master_tokens_per_node_max_count) {
                    master_server_capable.put(server, master_tokens_per_node_min + 1);;
                    mmax_s++;
                } else {
                    master_server_capable.put(server, master_tokens_per_node_min);
                }
            }
            i++;
        }
        
        /// DPJ: all server capable must NOT larger than maxBucketCnt
        Iterator<Entry<String, Integer>> capIter = server_capable.entrySet().iterator();
        while(capIter.hasNext()) {
            Entry<String, Integer> entry = capIter.next();
            if(entry.getValue() > maxBucketCnt) {
                entry.setValue(maxBucketCnt);
            }
        }
        capIter = master_server_capable.entrySet().iterator();
        while(capIter.hasNext()) {
            Entry<String, Integer> entry = capIter.next();
            if(entry.getValue() > maxBucketCnt) {
                entry.setValue(maxBucketCnt);
            }
        }
        /// DPJ end
    }

    @Override
    protected int get_tokens_per_node(String server_id_type) {
        return tokens_per_node_min;
    }

    public int getMaxBucketCnt() {
        return maxBucketCnt;
    }

    public void setMaxBucketCnt(int maxBucketCnt) {
        this.maxBucketCnt = maxBucketCnt;
    }

}
