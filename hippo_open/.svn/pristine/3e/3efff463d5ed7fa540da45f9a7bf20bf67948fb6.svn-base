package com.pinganfu.hippoconsoleweb.tablebuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippoconsoleweb.model.ServerInfoBean;
import com.pinganfu.hippoconsoleweb.service.ConsoleConstants;

abstract public class AbstractTableBuilder {

    private static final Logger log = LoggerFactory.getLogger(AbstractTableBuilder.class);
    private boolean TDEBUG = true;

    // table_builder.hpp
    ///// typedef
    /*
    typedef pair<uint64_t, uint32_t> server_id_type;    // TODO: check: uint64_t: pos_mask_id;  uint32_t: server_id??
    ServerIdType server_id_type; -> String server_id    // ServerIdType.pos_mask_id: server_id_type.first; ServerIdType.server_id:��server_id_typ.second
    Map<Integer, Vector<ServerIdType>> hash_table_type; // map<copy_id, vector<ServerIdType> > : copy_id-id of master/slaves, vector-a line(horizontal line, not a vertical line) whose sequence number means bucket number 
    Set<ServerIdType> server_list_type;
    Map<ServerIdType, Integer> server_capable_type;
    Vector<ServerIdType> hash_table_line_type;
    ServerIdType -> String: server_id
    */

    // TAIR_POS_MASK & serverid  is  rack_id, so change this to fit your rack
    protected final long TAIR_POS_MASK = 0x000000000000ffffL;

    // protected parameters
    protected Map<String, Integer> tokens_count_in_node = new HashMap<String, Integer>();
    protected int max_count_now;

    // count of buckets the server hold
    protected Map<String, Integer> tokens_count_in_node_now = new HashMap<String, Integer>();
    protected Map<Integer, Set<String>> count_server = new HashMap<Integer, Set<String>>();

    // count of master buckets the server hold
    protected Map<String, Integer> mtokens_count_in_node = new HashMap<String, Integer>();
    // Map<master bucket count, server>
    protected Map<Integer, Set<String>> mcount_server = new HashMap<Integer, Set<String>>();

    // Map<(now_count - capable_count), server>
    protected Map<Integer, Set<String>> scandidate_node = new HashMap<Integer, Set<String>>();
    protected Map<Integer, Set<String>> mcandidate_node = new HashMap<Integer, Set<String>>();

    protected Set<String> available_server = new HashSet<String>();
    /** Server's capable of master+slave bucket  */
    protected Map<String, Integer> server_capable = new HashMap<String, Integer>();
    /** Server's capable of master bucket  */
    protected Map<String, Integer> master_server_capable = new HashMap<String, Integer>();

    protected int bucket_count;
    /** Include master & salve */
    protected int copy_count;

    protected long pos_mask = TAIR_POS_MASK; // Reserved parameter
    /** default is 0, not allow data lost. if 1: if one bucket lost all of copies, configserver will force rebuild table, and assign this bucket to alive dataservers */
    protected DataLostToleranceFlag d_lost_flag = DataLostToleranceFlag.NO_DATA_LOST_FLAG;
    /** default is 0, if 1: force buckets are not one to one correspondence in ds at first run  */
    protected int b_place_flag = 0;

    abstract int is_this_node_OK(String node_id, int line_num, int node_idx, Map<Integer, Vector<String>> hash_table_dest, int option_level, boolean node_in_use); //boolean node_in_us=false

    abstract void caculate_capable(); // =0

    abstract int get_tokens_per_node(String server_id_type); // =0

    public AbstractTableBuilder() {
        pos_mask = TAIR_POS_MASK;
        d_lost_flag = DataLostToleranceFlag.NO_DATA_LOST_FLAG;
        b_place_flag = 0;
    }

    public void set_data_lost_flag(final DataLostToleranceFlag data_lost_flag) {
        d_lost_flag = data_lost_flag;
    }

    public void set_bucket_place_flag(int bucket_place_flag) {
        b_place_flag = bucket_place_flag;
        if (0 != b_place_flag) {
            // Initial random serial
            ///srandom(time(NULL));
        }
    }

    protected void init_token_count(Map<String, Integer> collector) {
        collector.clear();
        Iterator<String> it = available_server.iterator();
        while (it.hasNext()) {
            String server = it.next();
            collector.put(server, 0);
        }
    }

    protected boolean update_node_count(String node_id, Map<String, Integer> collector) {
        Integer i = collector.get(node_id);
        if (i != null) {
            // Trick: add ++ into map
            // i++;
            collector.put(node_id, ++i);
            return true;
        }
        return false;

    }

    protected void build_index(final Map<String, Integer> collector, Map<Integer, Set<String>> indexer) {
        indexer.clear();
        Iterator<Entry<String, Integer>> it = collector.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Integer> entry = it.next();
            Set<String> set = indexer.get(entry.getValue());
            if (null == set) {
                set = new HashSet<String>();
                indexer.put(entry.getValue(), set);
            }
            set.add(entry.getKey());
        }
    }

    protected boolean is_node_availble(String node_id) {
        if (node_id.equals(ConsoleConstants.INVALID_FLAG))
            return false;
        return available_server.contains(node_id);
    }

    protected void change_tokens_count_in_node(Map<String, Integer> count_in_node, final String node_id, Map<Integer, Set<String>> count_server_map, Map<Integer, Set<String>> candidate_node_info, Map<String, Integer> server_capable_info, boolean minus) {

        // master bucket count the node hold
        // Trick: int &token_count_in_node = count_in_node[node_id]; // it's &, a reference
        int token_count_in_node = count_in_node.get(node_id);
        count_server_map.get(token_count_in_node).remove(node_id);
        Set<String> candidates = candidate_node_info.get(token_count_in_node - server_capable_info.get(node_id));
        if (candidates != null) {
            candidates.remove(node_id);
        }
        if (minus)
            token_count_in_node--;
        else
            token_count_in_node++;
        // Deal trick:
        count_in_node.put(node_id, token_count_in_node);

        Set<String> serverSet = count_server_map.get(token_count_in_node);
        if (null == serverSet) {
            serverSet = new HashSet<String>();
            count_server_map.put(token_count_in_node, serverSet);
        }
        serverSet.add(node_id);

        int cadidateKey = token_count_in_node - server_capable_info.get(node_id);

        /* DPJ: candidate key should always < 0
        if (cadidateKey >= 0) {
            return;
        }*/

        serverSet = candidate_node_info.get(cadidateKey);
        if (null == serverSet) {
            serverSet = new HashSet<String>();
            candidate_node_info.put(cadidateKey, serverSet);
        }
        serverSet.add(node_id);
    }

    protected void invaliad_node(int line_num, int node_idx, Map<Integer, Vector<String>> hash_table_data) {
        String node_id = hash_table_data.get(line_num).elementAt(node_idx);
        if (is_node_availble(node_id)) {
            change_tokens_count_in_node(tokens_count_in_node, node_id, count_server, scandidate_node, server_capable, true);

            if (line_num == 0) {
                change_tokens_count_in_node(mtokens_count_in_node, node_id, mcount_server, mcandidate_node, master_server_capable, true);
            }
        }
        hash_table_data.get(line_num).setElementAt(ConsoleConstants.INVALID_FLAG, node_idx);
    }

    protected boolean change_master_node(int idx, Map<Integer, Vector<String>> hash_table_dest, boolean force_flag) {
        int chosen_line_num = -1;
        int min_node_count = -1;
        // choose the server which hold min count of master bucket
        for (int next_line = 1; next_line < copy_count; next_line++) {
            String node_id = hash_table_dest.get(next_line).elementAt(idx);
            if (is_node_availble(node_id)) {
                int mtoken_count_in_node = mtokens_count_in_node.get(node_id);
                if (min_node_count == -1 || min_node_count > mtoken_count_in_node) {
                    chosen_line_num = next_line;
                    min_node_count = mtoken_count_in_node;
                }
            }
        }
        // no available node
        if (min_node_count == -1) {
            return false; // we lost all copies of this bucket
        }
        String choosen_node_id = hash_table_dest.get(chosen_line_num).elementAt(idx);
        if (force_flag == false) {
            if (mtokens_count_in_node.get(choosen_node_id) >= master_server_capable.get(choosen_node_id)) {
                return false;
            }
        }
        String org_node_id = hash_table_dest.get(0).elementAt(idx);
        String new_master_node = choosen_node_id;
        // master of this bucket is chanegd, so we must turn the stat info
        hash_table_dest.get(0).setElementAt(new_master_node, idx);
        // make ++ into the map
        Integer tokensCnt = tokens_count_in_node_now.get(new_master_node);
        tokensCnt++;
        tokens_count_in_node_now.put(new_master_node, tokensCnt);
        // change token count in mtoken, mcount_server
        change_tokens_count_in_node(mtokens_count_in_node, new_master_node, mcount_server, mcandidate_node, master_server_capable, false);

        if (is_node_availble(org_node_id)) {
            choosen_node_id = org_node_id;
            change_tokens_count_in_node(mtokens_count_in_node, org_node_id, mcount_server, mcandidate_node, master_server_capable, true);
        } else {
            choosen_node_id = ConsoleConstants.INVALID_FLAG;
        }
        return true;
    }

    protected void update_node(int line_num, int node_idx, final String suitable_node, Map<Integer, Vector<String>> hash_table_dest) {
        hash_table_dest.get(line_num).setElementAt(suitable_node, node_idx);
        if (is_node_availble(suitable_node)) {
            change_tokens_count_in_node(tokens_count_in_node, suitable_node, count_server, scandidate_node, server_capable, false);
            if (line_num == 0) {
                change_tokens_count_in_node(mtokens_count_in_node, suitable_node, mcount_server, mcandidate_node, master_server_capable, false);
            }
        }
    }

    protected void init_candidate(Map<Integer, Set<String>> candidate_node, Map<String, Integer> pcapable, Map<Integer, Set<String>> pcount_server) {
        candidate_node.clear();
        Iterator<Entry<Integer, Set<String>>> it = pcount_server.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Set<String>> entry = it.next();
            Iterator<String> server_it = entry.getValue().iterator();
            while (server_it.hasNext()) {
                String server = server_it.next();
                // Candidate count = current count - capable
                int candidateCnt = entry.getKey() - pcapable.get(server);
                Set<String> nodeV = candidate_node.get(candidateCnt);
                if (null == nodeV) {
                    nodeV = new HashSet<String>();
                    candidate_node.put(candidateCnt, nodeV);
                }
                nodeV.add(server);
            }
        }
    }

    protected String get_suitable_node(int line_num, int node_idx, Map<Integer, Vector<String>> hash_table_dest, String original_node) {
        //      Map <String, Integer>*ptokens_node;
        //      Map <Integer, Set<String>> *pcount_server;
        //      Map<String, Integer> *pcapable;
        Map<Integer, Set<String>> pcandidate_node;
        if (line_num == 0) {
            //        ptokens_node = mtokens_count_in_node;
            //        pcount_server = mcount_server;
            //        pcapable = master_server_capable;
            pcandidate_node = mcandidate_node;
        } else {
            //        ptokens_node = tokens_count_in_node;
            //        pcount_server = count_server;
            //        pcapable = server_capable;
            pcandidate_node = scandidate_node;
        }

        String suitable_node = ConsoleConstants.INVALID_FLAG;
        for (int i = ConsoleConstants.CONSIDER_ALL; i <= ConsoleConstants.CONSIDER_FORCE && suitable_node.equals(ConsoleConstants.INVALID_FLAG); i++) {
            int s = ConsoleConstants.CONSIDER_ALL;
            Iterator<Entry<Integer, Set<String>>> it = pcandidate_node.entrySet().iterator();
            while (it.hasNext() && s < i + 1 && suitable_node.equals(ConsoleConstants.INVALID_FLAG)) {
                Entry<Integer, Set<String>> entry = it.next();
                int candidate_size = entry.getValue().size();
                if (0 != candidate_size) {
                    Iterator<String> server_it = entry.getValue().iterator();
                    // Trick:
                    /*
                     server_list_type::const_iterator server_it = it->second.begin();
                    // force buckets are not one to one correspondence in ds
                    if (0 != b_place_flag) {
                    int start_index = random() % candidate_size;
                    for (int i = 0; i < start_index; ++i) {
                      if (server_it != it->second.end()) {
                        ++server_it;
                      }
                      if (server_it == it->second.end()) {
                        server_it = it->second.begin();
                      }
                    }
                    }
                     */
                    // force buckets are not one to one correspondence in ds if != 0
                    if (0 != b_place_flag) {
                        int start_index = Math.abs((int) (Math.random())) % candidate_size;
                        for (int j = 0; j < start_index; ++j) {
                            if (server_it.hasNext()) {
                                server_it.next();
                            }
                            if (!server_it.hasNext()) {
                                server_it = entry.getValue().iterator();
                            }
                        }
                    }

                    for (int j = 0; j < candidate_size; ++j) {
                        String server_id = server_it.next();
                        if (is_this_node_OK(server_id, line_num, node_idx, hash_table_dest, i, false) == ConsoleConstants.NODE_OK) {
                            suitable_node = server_id;
                            // TODO: bug
                            /// DPJ: it should always been true, because in C++: it just equals the pos_mask_id but not server_id
                            if (suitable_node.equals(original_node) || true)
                                break;

                        }

                        /* Trick: 
                        ++server_it;
                        if (server_it == it->second.end()) {
                          server_it = it->second.begin();
                        }*/
                        if (!server_it.hasNext()) {
                            server_it = entry.getValue().iterator();
                        }
                    }
                }

                if (i < ConsoleConstants.CONSIDER_BASE)
                    s++;
            }
        }

        /* DPJ: never let it be INVALID_FLAG, but it will when it comes to the last position of hash_table
        if (ConsoleConstants.INVALID_FLAG.equals(suitable_node)) {
            Iterator<String> serverIt = available_server.iterator();
            while (serverIt.hasNext()) {
                String server_id = serverIt.next();
                if (is_this_node_OK(server_id, line_num, node_idx, hash_table_dest, 999, false) == ConsoleConstants.NODE_OK) {
                    suitable_node = server_id;
                }
            }
        }
        */ 

        return suitable_node;
    }

    public boolean build_quick_table(Map<Integer, Vector<String>> hash_table_dest) {
        Vector<String> line = hash_table_dest.get(0);
        for (int idx = 0; idx < bucket_count; idx++) {
            //log.debug("quick table check server %s", tbsys::CNetUtil::addrToString(line[idx].first).c_str() );
            if (is_node_availble(line.elementAt(idx)) == false) { // this will make some unbalance, but that's ok. another balance will be reache when migrating is done
                //log.debug("will change server %s",tbsys::CNetUtil::addrToString(line[idx].first).c_str());
                if (change_master_node(idx, hash_table_dest, true) == false) {
                    log.error("bucket {} lost all of its duplicate so can not find out a master for it quick build failed", idx);
                    //for session tair: if we lost all duplicate, we just go on. d_lost_flag == ALLOW_DATA_LOST_FALG
                    if (d_lost_flag != DataLostToleranceFlag.ALLOW_DATA_LOST_FALG) {
                        return false;
                    }
                }
            }
        }
        for (int line_number = 1; line_number < copy_count; ++line_number) {
            Vector<String> line_server = hash_table_dest.get(line_number);
            for (int idx = 0; idx < bucket_count; idx++) {
                if (is_node_availble(line_server.elementAt(idx)) == false) {
                    line_server.setElementAt(ConsoleConstants.INVALID_FLAG, idx);
                }
            }
        }

        return true;
    }

    /**
     * 
     * @param newDtable
     * @param oldMtable
     * @return new mtable
     */
    public Map<Integer, Vector<String>> buildQuickTable(Map<Integer, Vector<String>> newDtable, Map<Integer, Vector<String>> oldMtable) {
        if(null == newDtable || null == oldMtable) {
            return null;
        }
        Map<Integer, Vector<String>> newMtable = this.load_hash_table(newDtable);
        int copyCnt = newMtable.size();
        int bucketCnt = newMtable.get(0).size();
        for (int i = 0; i < bucketCnt; i++) {
            Set<String> servers = new HashSet<String>();
            for(int j=0; j<copyCnt; j++) {
                servers.add(oldMtable.get(j).get(i));
            }
            
            for(int j=0; j<copyCnt; j++) {
                if(!servers.contains(newMtable.get(j).get(i))) {
                    newMtable.get(j).set(i, ConsoleConstants.INVALID_FLAG);
                }
            }
        }
        return newMtable;
    }

    /**
     * 
     * @param currMtable
     * @return null if failed
     */
    public Map<Integer, Vector<String>> build_quick_table_alone(final Map<Integer, Vector<String>> currMtable) {
        /// DPJ
        init_token_count(tokens_count_in_node);
        init_token_count(tokens_count_in_node_now);
        init_token_count(mtokens_count_in_node);
        
        Map<Integer, Vector<String>> hash_table_tmp = this.load_hash_table(currMtable);

        // compute node count: tokens_count_in_node, mtokens_count_in_node
        Iterator<Entry<Integer, Vector<String>>> it = hash_table_tmp.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Vector<String>> entry = it.next();
            final Vector<String> line = entry.getValue();
            for (int i = 0; i < bucket_count; i++) {
                update_node_count(line.elementAt(i), tokens_count_in_node);
                if (entry.getKey() == 0) {
                    if (update_node_count(line.elementAt(i), mtokens_count_in_node) == false) {
                        // almost every time this will happen
                        // need_build_quick_table = true;
                    }
                }
            }
        }

        //init count_server, mcount_server
        build_index(tokens_count_in_node, count_server);
        build_index(mtokens_count_in_node, mcount_server);

        caculate_capable();

        //init mcandidate_node, scandidate_node
        init_candidate(mcandidate_node, master_server_capable, mcount_server);
        init_candidate(scandidate_node, server_capable, count_server);
        ///
        
        boolean result = build_quick_table(hash_table_tmp);
        if(result) {
            return hash_table_tmp;
        } else {
            return null;
        }
    }

    /**
     * Reference: http://www.tuicool.com/articles/qUfeI3 
     * @param hash_table_source
     * @param hash_table_result
     * @param no_quick_table
     * @return 0 - build error; 1 - ok; 2 - quick build ok
     */
    public int rebuild_table(Map<Integer, Vector<String>> hash_table_source, Map<Integer, Vector<String>> hash_table_result, boolean no_quick_table) {
        init_token_count(tokens_count_in_node);
        init_token_count(tokens_count_in_node_now);
        init_token_count(mtokens_count_in_node);

        max_count_now = 0;

        boolean need_build_quick_table = false;

        // compute node count: tokens_count_in_node, mtokens_count_in_node
        Iterator<Entry<Integer, Vector<String>>> it = hash_table_source.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Vector<String>> entry = it.next();
            final Vector<String> line = entry.getValue();
            for (int i = 0; i < bucket_count; i++) {
                update_node_count(line.elementAt(i), tokens_count_in_node);
                if (entry.getKey() == 0) {
                    if (update_node_count(line.elementAt(i), mtokens_count_in_node) == false) {
                        // almost every time this will happen
                        need_build_quick_table = true;
                    }
                }
            }
        }

        //init count_server, mcount_server
        build_index(tokens_count_in_node, count_server);
        build_index(mtokens_count_in_node, mcount_server);

        caculate_capable();

        //init mcandidate_node, scandidate_node
        init_candidate(mcandidate_node, master_server_capable, mcount_server);
        init_candidate(scandidate_node, server_capable, count_server);

        // Init hash_table_result, hard copy
        // Trick: Soft copy: hash_table_result = hash_table_source; hash_table_result.putAll(hash_table_source);
        it = hash_table_source.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Vector<String>> entry = it.next();
            Vector<String> value = new Vector<String>();
            hash_table_result.put(entry.getKey(), value);
            Iterator<String> server_it = entry.getValue().iterator();
            while (server_it.hasNext()) {
                value.add(server_it.next());
            }
        }

        // no_quick_table is always true, will not reach this branch
        if (need_build_quick_table && !no_quick_table) {
            if (build_quick_table(hash_table_result)) {
                return ConsoleConstants.BUILD_QUICK;
            }
            log.error("build quick table fail");
            return ConsoleConstants.BUILD_ERROR;
        }
        if (available_server.size() < copy_count) {
            log.error("rebuild table fail, available size: {}, copy count: {}", available_server.size(), copy_count);
            return ConsoleConstants.BUILD_ERROR;
        }
        //
        //we will check master first, then other slaves
        /////////////////////////////////////////////////////////////////////////////////////
        //checke every node and find out the bad one
        //a good one must
        //    1 the buckets this one charge of must not large than tokenPerNode ;
        //    2 the master buckets this one charge of must not large than masterTokenPerNode;
        //    3 copys of a same bucket must seperated store in different data server
        /////////////////////////////////////////////////////////////////////////////////////
        int i = 0;
        it = hash_table_result.entrySet().iterator();
        while (it.hasNext() && i < 2) {
            i++;
            Entry<Integer, Vector<String>> entry = it.next();
            int line_num_out = entry.getKey();
            for (int node_idx = 0; node_idx != bucket_count; node_idx++) {
                for (int line_num = line_num_out; line_num < copy_count; line_num++) {
                    if (line_num_out == 0 && line_num != 0)
                        continue;
                    int change_type = 0; //not need migrate
                    String node_id = hash_table_result.get(line_num).elementAt(node_idx);
                    int consider = ConsoleConstants.CONSIDER_ALL;
                    if (line_num_out == 0)
                        consider = ConsoleConstants.CONSIDER_BASE;
                    change_type = is_this_node_OK(node_id, line_num, node_idx, hash_table_result, consider, true);
                    if (change_type == ConsoleConstants.INVALID_NODE) {
                        node_id = ConsoleConstants.INVALID_FLAG;
                    }

                    if (change_type != 0) {
                        if (TDEBUG) {
                            log.debug("---------------------will change -------------line={} idx={} \n", node_idx, line_num);
                            log.debug("debug changeType ={}\n", change_type);
                            log.debug("befor change");
                            print_tokens_in_node();
                            print_hash_table(hash_table_result);
                            print_capabale();
                        }

                        if (line_num == 0) {
                            if (change_master_node(node_idx, hash_table_result, false)) {
                                continue;
                            }
                        }
                        String old_node = hash_table_result.get(line_num).elementAt(node_idx);
                        invaliad_node(line_num, node_idx, hash_table_result);
                        String suitable_node = get_suitable_node(line_num, node_idx, hash_table_result, old_node);
                        if (suitable_node.equals(ConsoleConstants.INVALID_FLAG)) {
                            log.error("I am give up, why this happend?");
                            return ConsoleConstants.BUILD_ERROR;
                        }
                        update_node(line_num, node_idx, suitable_node, hash_table_result);
                        node_id = suitable_node;
                        if (TDEBUG) {
                            //log.debug("after change");
                            print_tokens_in_node();
                            print_hash_table(hash_table_result);
                            //log.debug("----------------------------------------");
                        }
                    }
                    int token_per_node_min = get_tokens_per_node(node_id);
                    // Trick: Original: if(++tokens_count_in_node_now[node_id] == token_per_node_min + 1) {
                    // Deal trick: need add ++ into the map
                    Integer tonkensCnt = tokens_count_in_node_now.get(node_id);
                    tokens_count_in_node_now.put(node_id, ++tonkensCnt);
                    if (tonkensCnt == token_per_node_min + 1) {
                        max_count_now++;
                    }
                    //log.debug("token_per_node_min: {}, max_count_now: {}, tokens_count_in_node_now[{}]: {}", token_per_node_min, max_count_now, node_id, tokens_count_in_node_now.get(node_id));
                }
            }
            /*
            if(it == hash_table_result.begin()) {
              log.debug("first line ok");
            }*/
        }
        return ConsoleConstants.BUILD_OK;
    }

    public void set_available_server(final Set<ServerInfoBean> ava_server) {
        available_server.clear();
        Iterator<ServerInfoBean> it = ava_server.iterator();
        while (it.hasNext()) {
            ServerInfoBean node = it.next();
            available_server.add(node.server_id);
        }

    }

    public Map<Integer, Vector<String>> load_hash_table(Map<Integer, Vector<String>> hash_table_src) {
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

    public void write_hash_table(final Map<Integer, Vector<String>> hash_table_src, Map<Integer, Vector<String>> hash_table_dest) {
        hash_table_dest = load_hash_table(hash_table_src);
    }

    /*

    void load_hash_table(Map<Integer, Vector<String>>  hash_table_data,
                                        long * p_hash_table)
    {
      hash_table_data.clear();
      for(int i = 0; i < copy_count; i++) {
        hash_table_data.get(i).reserve(bucket_count);
        for(int j = 0; j < bucket_count; j++) {
          hash_table_data.get(i).
            push_back(make_pair(*p_hash_table, (*p_hash_table)  pos_mask));
          p_hash_table++;
        }
      }
    }

    void write_hash_table(final Map<Integer, Vector<String>>  hash_table_data,
                       long * p_hash_table)
    {
      for(Map<Integer, Vector<String>>::const_iterator it = hash_table_data.begin();
          it != hash_table_data.end(); it++) {
        final Vector<String>  line = it->second;
        for(int i = 0; i < bucket_count; i++) {
          (*p_hash_table) = line[i].first;
          p_hash_table++;
        }
      }
    }*/

    public int getBucket_count() {
        return bucket_count;
    }

    public void setBucket_count(int bucket_count) {
        this.bucket_count = bucket_count;
    }

    public int getCopy_count() {
        return copy_count;
    }

    public void setCopy_count(int copy_count) {
        this.copy_count = copy_count;
    }

    public DataLostToleranceFlag getD_lost_flag() {
        return d_lost_flag;
    }

    public void setD_lost_flag(DataLostToleranceFlag d_lost_flag) {
        this.d_lost_flag = d_lost_flag;
    }

    public int getB_place_flag() {
        return b_place_flag;
    }

    public void setB_place_flag(int b_place_flag) {
        this.b_place_flag = b_place_flag;
    }

    // table_builder.cpp
    // Debug method
    public void print_hash_table(Map<Integer, Vector<String>> hash_table) {
        if (hash_table.isEmpty())
            return;
        /// System.out.println(hash_table.toString());
    }

    public void print_tokens_in_node() {
    }

    public void print_count_server() {
    }

    public void print_available_server() {
        log.info(this.available_server.toString());
    }

    public void print_capabale() {

    }

    /*
    void print_tokens_in_node()
    {
      Map<String, Integer>::iterator it;
        log.debug("max_count_now =%d", max_count_now);
        log.debug("tokens");
      for(it = tokens_count_in_node.begin(); it != tokens_count_in_node.end();
          it++)
      {
        log.debug("S(%s:%"PRI64_PREFIX"d,%d)=%d ", tbsys::CNetUtil::addrToString(it->first.first).c_str(), it->first.first, it->first.second,
                  it->second);
      }
      log.debug("tokens_now");
      for(it = tokens_count_in_node_now.begin();
          it != tokens_count_in_node_now.end(); it++) {
        log.debug("S(%s:%"PRI64_PREFIX"d,%d)=%d ", tbsys::CNetUtil::addrToString(it->first.first).c_str(), it->first.first, it->first.second,
                  it->second);
      }
      log.debug("mtokes:");

      for(it = mtokens_count_in_node.begin();
          it != mtokens_count_in_node.end(); it++) {
        log.debug("S(%s:%"PRI64_PREFIX"d,%d)=%d ", tbsys::CNetUtil::addrToString(it->first.first).c_str(), it->first.first, it->first.second,
                  it->second);
      }
    }
    void print_count_server()
    {
      Map<Integer, Set<String>>::iterator it;
      log.debug("count:");
      for(it = count_server.begin(); it != count_server.end(); it--) {
        log.debug("%d:   ", it->first);
        for(Set<String>::iterator it2 = it->second.begin();
            it2 != it->second.end(); it2++) {
          log.debug("%"PRI64_PREFIX"d,%d  ", it2->first, it2->second);
        }
      }
      log.debug("mcount:");
      for(it = mcount_server.begin(); it != mcount_server.end(); it++) {
        log.debug("%d:   ", it->first);
        for(Set<String>::iterator it2 = it->second.begin();
            it2 != it->second.end(); it2++) {
          log.debug("%"PRI64_PREFIX"d,%d  ", it2->first, it2->second);
        }
      }
    }
    void print_available_server()
    {
      log.debug("available server size: %lu", available_server.size());
      log.debug("    ");
      for(Set<String>::iterator it = available_server.begin();
          it != available_server.end(); it++) {
        log.debug("S:%s,%-3d  ",
                  tbsys::CNetUtil::addrToString(it->first).c_str(),
                  it->second);
      }
    }

    void print_hash_table(Map<Integer, Vector<String>>  hash_table)
    {
      if(hash_table.empty())
        return;
      for(int j = 0; j < bucket_count; ++j) {
        char kk[64];
        sprintf(kk, "%u-->", j);
        string ss(kk);
        for(int i = 0; i < copy_count; ++i) {
          char str[1024];
          sprintf(str, "%s(%-3d)  ",
                  tbsys::CNetUtil::addrToString(hash_table[i][j].first).
                  c_str(), hash_table[i][j].second);
          ss += str;
        }
        log.debug("%s", ss.c_str());
      }
    }
    void print_capabale()
    {
      Map<String, Integer>::iterator it;
      log.debug("server capabale:");
      for(it = server_capable.begin(); it != server_capable.end(); it++) {
        log.debug("%s:%"PRI64_PREFIX"d,%d %d   ", tbsys::CNetUtil::addrToString(it->first.first).c_str(),
            it->first.first, it->first.second, it->second);
      }
      for(it = master_server_capable.begin();
          it != master_server_capable.end(); it++) {
        log.debug("%s:%"PRI64_PREFIX"d,%d %d   ", tbsys::CNetUtil::addrToString(it->first.first).c_str(),
            it->first.first, it->first.second, it->second);
      }
    }
    */
    /////////////////////////////////////////////////////////////////////////////////////////////////////////
}
