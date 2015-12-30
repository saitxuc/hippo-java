package com.hippoconsoleweb.lisneter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hippo.common.ZkConstants;
import com.hippo.common.domain.HippoClusterConifg;
import com.hippo.common.domain.HippoClusterTableInfo;
import com.hippo.common.util.FastjsonUtil;
import com.hippo.common.util.ListUtils;
import com.hippo.common.util.ServerTableUtil;
import com.hippoconsoleweb.model.ServerInfoBean;
import com.hippoconsoleweb.service.ConsoleConstants;
import com.hippoconsoleweb.tablebuilder.LbFirstTableBuilder;
import com.hippoconsoleweb.util.ZkUtils;

public class ServerChangeLisneter implements IZkChildListener {

    private Logger LOG = LoggerFactory.getLogger(ServerChangeLisneter.class);

    private String clusterName;
    private Object g_dtableLock;

    private List<String> preServerLists = null;

    private int bucketLimit = Integer.MAX_VALUE;
    private int copyCnt = 0;
    private int hashCnt = 0;

    private String cconfigPath = null;
    private String mtablePath = null;
    private String dtablePath = null;
    private String dataServerPath = null;

    private ZkClient zkClient = null;
    
    public ServerChangeLisneter(String zkAddress, String clusterName, Object g_dtableLock) throws Exception {
        this.clusterName = clusterName;
        this.zkClient = ZkUtils.getZKClient(zkAddress);
        this.g_dtableLock = g_dtableLock;

        cconfigPath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_CONFIG;
        mtablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + ZkConstants.NODE_MTABLE;
        dtablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + ZkConstants.NODE_DTABLE;
        dataServerPath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_DATA_SERVERS;
        
        initConfig();

        // For console switched when doing handleChildChange0
        checkServerTable();
    }
    
    public void checkServerTable() throws Exception {
        LOG.info(" check server table ");
        
        List<String> dataservers = this.getDataServerList();
        if(null == dataservers) {
            dataservers = new ArrayList<String>();
        }
        String mtableInfoStr = zkClient.readData(mtablePath);
        String dtableInfoStr = zkClient.readData(dtablePath);
        
        boolean rebuild = false;
        
        if (mtableInfoStr != null) {
            HippoClusterTableInfo mtableInfo = FastjsonUtil.jsonToObj(mtableInfoStr, HippoClusterTableInfo.class);
            if(mtableInfo != null) {
                // check mtable
                Map<Integer, Vector<String>> mtable = mtableInfo.getTableMap();
                boolean mtableUpdated = false;
                for(int i=0; i<copyCnt && !mtableUpdated; i++) {
                    for(int j=0; j<hashCnt; j++) {
                        String server = mtable.get(i).get(j);
                        if(!ConsoleConstants.INVALID_FLAG.equals(server) && !dataservers.contains(server)) {
                            mtable.get(i).set(j, ConsoleConstants.INVALID_FLAG);
                            mtableUpdated = true;
                            break;
                        }
                    }
                }
                if(mtableUpdated) {
                    LOG.info(" mtable un-match, need rebuild ");
                    // build quick table here, because console doesn't know it's server down if Master/Slave switched
                    Map<Integer, Vector<String>> newQuickTable = buildQuickTable(mtableInfo.getTableMap(), dataservers);
                    if (null == newQuickTable || dataservers.size() < copyCnt) {
                        newQuickTable = ServerTableUtil.copyTable(mtable);
                        HippoClusterTableInfo newQuickTableInfo = new HippoClusterTableInfo();
                        newQuickTableInfo.setVersion(mtableInfo.getVersion());
                        newQuickTableInfo.setTableMap(newQuickTable);
                        writeCtable(newQuickTableInfo);
                        writeTable(ConsoleConstants.MTABLE, newQuickTableInfo);
                        preServerLists = dataservers;
                        LOG.warn(" unable to build normal quick table, abort ");
                        return;
                    }
                    LOG.info(" quick table: {} ", newQuickTable);
                    HippoClusterTableInfo newQuickTableInfo = new HippoClusterTableInfo();
                    newQuickTableInfo.setTableMap(newQuickTable);
                    newQuickTableInfo.setVersion(mtableInfo.getVersion());

                    writeCtable(newQuickTableInfo);
                    writeTable(ConsoleConstants.MTABLE, newQuickTableInfo);
                    rebuild = true;
                }
            }
        }
        
        if (dtableInfoStr != null) {
            HippoClusterTableInfo dtableInfo = FastjsonUtil.jsonToObj(dtableInfoStr, HippoClusterTableInfo.class);
            if(dtableInfo != null) {
                // check dtable
                Map<Integer, Vector<String>> dtable = dtableInfo.getTableMap();
                boolean serverChanged = false;
                for(int i=0; i<copyCnt; i++) {
                    for(int j=0; j<hashCnt; j++) {
                        String server = dtable.get(i).get(j);
                        if(!ConsoleConstants.INVALID_FLAG.equals(server) && !dataservers.contains(server)) {
                            serverChanged = true;
                            break;
                        }
                    }
                }
                if(serverChanged) {
                    LOG.info(" dtable un-match, rebuild ");
                    rebuild = true;
                }
            }
        }
        
        if(rebuild) {
            handleChildChange0(dataServerPath, dataservers);
        }
        LOG.info(" check server finished ");
    }

    private void initConfig() {
        String cconfigString = zkClient.readData(cconfigPath);
        HippoClusterConifg config = FastjsonUtil.jsonToObj(cconfigString, HippoClusterConifg.class);
        
        if(null == config) {
            throw new RuntimeException(" config not set on node: " + cconfigPath);
        }

        this.bucketLimit = Integer.parseInt(config.getBucketsLimit());
        this.copyCnt = config.getCopycount();
        this.hashCnt = config.getHashcount();
    }

    private synchronized void handleChildChange0(String parentPath, List<String> currentChilds) throws Exception {
        LOG.info(" server changed: {} in cluster: {} ", currentChilds, this.clusterName);
    	synchronized(g_dtableLock) {
    		String mtableInfoStr = zkClient.readData(mtablePath);
            HippoClusterTableInfo mtableInfo = FastjsonUtil.jsonToObj(mtableInfoStr, HippoClusterTableInfo.class);
            String dtableInfoStr = zkClient.readData(dtablePath);
            HippoClusterTableInfo dtableInfo = FastjsonUtil.jsonToObj(dtableInfoStr, HippoClusterTableInfo.class);

            boolean firstRun = isFirstRun(mtableInfo);

            if (null == dtableInfo) {
                dtableInfo = new HippoClusterTableInfo();
                dtableInfo.setTableMap(this.buildEmptyTable(hashCnt, copyCnt));
                dtableInfo.setVersion(0);

                writeTable(ConsoleConstants.DTABLE, dtableInfo);
            }
            if (null == mtableInfo) {
                mtableInfo = new HippoClusterTableInfo();
                mtableInfo.setTableMap(this.buildEmptyTable(hashCnt, copyCnt));
                mtableInfo.setVersion(0);

                writeTable(ConsoleConstants.MTABLE, mtableInfo);
            }
            
            if (null == currentChilds || 0 == currentChilds.size()) {
                Map<Integer, Vector<String>> emptyTable = buildEmptyTable(hashCnt, copyCnt);
                mtableInfo.setTableMap(emptyTable);
                int maxv = getMaxVersion(mtableInfo, dtableInfo);
                maxv++;
                mtableInfo.setVersion(maxv);
                
                // DPJ: 2015/6/16
                preServerLists = currentChilds;

                writeCtable(mtableInfo);
                writeTable(ConsoleConstants.MTABLE, mtableInfo);
                writeTable(ConsoleConstants.DTABLE, mtableInfo);
                g_dtableLock.notifyAll();
                return;
            }

            LOG.info(" mtable: {} version: {} ", mtableInfo.getTableMap(), mtableInfo.getVersion());
            LOG.info(" dtable: {} version: {} ", dtableInfo.getTableMap(), dtableInfo.getVersion());

            List<String> downServers = null;
            if (preServerLists == null) {
                preServerLists = currentChilds;
            } else {
                downServers = checkDown(currentChilds);
            }

            if (downServers != null && downServers.size() > 0) {
                LOG.info(" server down: {}, build quick table ", downServers);
                // use current mtable to rebuild quick table
                Map<Integer, Vector<String>> newQuickTable = buildQuickTable(mtableInfo.getTableMap(), currentChilds);
                if (null == newQuickTable || currentChilds.size() < copyCnt) {
                    newQuickTable = ServerTableUtil.copyTable(mtableInfo.getTableMap());
                    for(String downServer : downServers) {
                        for(int i=0; i<copyCnt; i++) {
                            for(int j=0; j<hashCnt; j++) {
                                if(downServer.equals(newQuickTable.get(i).get(j))) {
                                    newQuickTable.get(i).set(j, ConsoleConstants.INVALID_FLAG);
                                }
                            }
                        }
                    }
                    HippoClusterTableInfo newQuickTableInfo = new HippoClusterTableInfo();
                    newQuickTableInfo.setVersion(dtableInfo.getVersion());
                    newQuickTableInfo.setTableMap(newQuickTable);
                    writeCtable(newQuickTableInfo);
                    writeTable(ConsoleConstants.MTABLE, newQuickTableInfo);
                    //newQuickTable = buildEmptyTable(hashCnt, copyCnt);
                    //newQuickTable.put(0, dtableInfo.getTableMap().get(0));
                    mtableInfo = newQuickTableInfo;
                    preServerLists = currentChilds;
                    LOG.warn(" unable to build normal quick table, abort ");
                    // 20150619: continue build mtable in case A,B/C,D down same time on dtable: A,B,C,D;B,A,D,C 
                   // return;
                }else{
                	LOG.info(" quick table: {} ", newQuickTable);
                    HippoClusterTableInfo newQuickTableInfo = new HippoClusterTableInfo();
                    newQuickTableInfo.setTableMap(newQuickTable);
                    newQuickTableInfo.setVersion(mtableInfo.getVersion());

                    writeCtable(newQuickTableInfo);
                    writeTable(ConsoleConstants.MTABLE, newQuickTableInfo);
                    // make mtable up to date for rebuild table later
                    mtableInfo = newQuickTableInfo;
                }
             
            }

            boolean capcan = checkClusterCapacity(currentChilds.size(), bucketLimit);
            if (capcan) {
            	 // first increase version to zk to lock new data server (version change indicate one migration been started)
                dtableInfo.incVersion();
                
                writeTable(ConsoleConstants.DTABLE, dtableInfo);
                
                // then rebuild table and apply to dtable
                // 20150619: this.preServerLists = getDataServerList(); - no need, two machine up same time will trigger child listener twice
                this.preServerLists = currentChilds;
                capcan = checkClusterCapacity(this.preServerLists.size(), bucketLimit);
                if (capcan) {
                    // use current mtable to rebuild table
                    Map<Integer, Vector<String>> newDtable = rebuildTable(this.preServerLists, mtableInfo.getTableMap());
                    LOG.info(" new dtable rebuilt: {} ", newDtable);
                    dtableInfo.setTableMap(newDtable);
                    
                    if (firstRun) {
                        mtableInfo.getTableMap().put(0, dtableInfo.getTableMap().get(0));
                        writeTable(ConsoleConstants.MTABLE, mtableInfo);
                    }else{
                        // 20150619: continue calculate in case A,B/C,D down same time on dtable: A,B,C,D;B,A,D,C
                        boolean updateM = false;
                        
                        Vector<String> firstline = mtableInfo.getTableMap().get(0);
                        for(int i = 0; i< firstline.size(); i++) {
                        	String masterNode = firstline.get(i);
                        	if(masterNode.equals(ConsoleConstants.INVALID_FLAG)) {
                        		mtableInfo.getTableMap().get(0).set(i, dtableInfo.getTableMap().get(0).get(i));
                        		updateM = true;
                        	}
                        }
                        
                        if(updateM) {
                            LOG.warn(" --> noreplicate, fill mtable's first line ");
                            LOG.warn(" --> noreplicate, mtable: {} ", mtableInfo.getTableMap());
                            LOG.warn(" --> noreplicate, dtable: {} ", dtableInfo.getTableMap());
                            writeTable(ConsoleConstants.MTABLE, mtableInfo);
                            LOG.warn(" --> noreplicate, mtable after: {} ", mtableInfo.getTableMap());
                        }
                    	/***
                    	boolean needM = false;
                        
                    	for(int i=0; i<hashCnt; i++) {
                        	boolean noreplicate = true;
                    		for(int j=0; j<copyCnt; j++) {
                        		if(!mtableInfo.getTableMap().get(j).get(i).equals(ConsoleConstants.INVALID_FLAG)) {
                        			noreplicate = false;
                        			break;
                        		}
                        	}
                    		if(noreplicate) {
                    			for(int j=0; j<copyCnt; j++) {
                        			mtableInfo.getTableMap().get(j).set(i, dtableInfo.getTableMap().get(j).get(i));
                        		}
                    			needM = true;
                    		}
                        }
                    	
                    	
                    	Vector<String> firstline = mtableInfo.getTableMap().get(0);
                        for(int i = 0; i< firstline.size(); i++) {
                        	String masterNode = firstline.get(i);
                        	if(masterNode.equals(ConsoleConstants.INVALID_FLAG)) {
                        		mtableInfo.getTableMap().get(0).set(i, dtableInfo.getTableMap().get(0).get(i));
                        		needM = true;
                        	}
                        }
                        
                        for(int i=0; i<hashCnt; i++) {
                        	List<String> camp1 = new ArrayList<String>();
                            List<String> camp2 = new ArrayList<String>();
                        	for(int j=0; j<copyCnt; j++) {
                        		camp1.add(mtableInfo.getTableMap().get(j).get(i));
                        		camp2.add(dtableInfo.getTableMap().get(j).get(i));
                        	}
                        	if(camp1.containsAll(camp2) && camp1.size() == camp2.size()) {
                        		for(int j=0; j<copyCnt; j++) {
                        			mtableInfo.getTableMap().get(j).set(i, dtableInfo.getTableMap().get(j).get(i));
                        		}
                        		needM = true;
                    		}
                        }
                        
                        
                        if(needM) {
                        	writeTable(ConsoleConstants.MTABLE, mtableInfo);
                        }
                        ***/
                    }
                    writeTable(ConsoleConstants.DTABLE, dtableInfo);
                }
            }

            if (!capcan) {
                LOG.info(" not capable to rebuild table ");
                if (mtableInfo.getVersion() != dtableInfo.getVersion()) {
                    int maxv = getMaxVersion(mtableInfo, dtableInfo);
                    maxv++;
                    mtableInfo.setVersion(maxv);
                    dtableInfo.setVersion(maxv);

                    LOG.info(" increase m&dtable version to: {} ", dtableInfo.getVersion());

                    // mtable first
                    writeTable(ConsoleConstants.MTABLE, mtableInfo);
                    writeTable(ConsoleConstants.DTABLE, dtableInfo);
                }
            }
            
            g_dtableLock.notifyAll();
        }
        LOG.info(" server changed handle finished. ");
    }

    private boolean isFirstRun(HippoClusterTableInfo mtableInfo) {
        if (null == mtableInfo) {
            return true;
        }
        for (int i = 0; i < copyCnt; i++) {
            for (int j = 0; j < hashCnt; j++) {
                if (!ConsoleConstants.INVALID_FLAG.equals(mtableInfo.getTableMap().get(i).get(j))) {
                    return false;
                }
            }
        }
        return true;
    }

    private int getMaxVersion(HippoClusterTableInfo table1, HippoClusterTableInfo table2) {
        int mv = table1.getVersion();
        int dv = table2.getVersion();
        return mv > dv ? mv : dv;
    }
    
    private void writeCtable(HippoClusterTableInfo tableInfo) {
        HippoClusterTableInfo ctableInfo = new HippoClusterTableInfo();
        ctableInfo.setVersion(tableInfo.getVersion());
        ctableInfo.setTableMap(ServerTableUtil.cutForCtable(tableInfo.getTableMap()));
        
        writeTable(ConsoleConstants.CTABLE, ctableInfo);
    }

    private Map<Integer, Vector<String>> rebuildTable(final List<String> avaServers, final Map<Integer, Vector<String>> mtable) {
        Map<Integer, Vector<String>> hash_table_for_builder_tmp = ServerTableUtil.copyTable(mtable);
        if (hash_table_for_builder_tmp == null) {
            hash_table_for_builder_tmp = buildEmptyTable(hashCnt, copyCnt);
        }

        LbFirstTableBuilder p_table_builder = new LbFirstTableBuilder(hashCnt, copyCnt);
        p_table_builder.setMaxBucketCnt(bucketLimit);
        Map<Integer, Vector<String>> hash_table_result = new HashMap<Integer, Vector<String>>();
        Set<ServerInfoBean> ava_server = new HashSet<ServerInfoBean>();
        if (avaServers != null) {
            for (String ser : avaServers) {
                ServerInfoBean node = new ServerInfoBean();
                node.setServer_id(ser);
                ava_server.add(node);
            }
            p_table_builder.set_available_server(ava_server);

            int result = p_table_builder.rebuild_table(hash_table_for_builder_tmp, hash_table_result, true);
            if (ConsoleConstants.BUILD_OK == result) {
                return hash_table_result;
            }
        }

        return null;
    }

    private Map<Integer, Vector<String>> buildQuickTable(final Map<Integer, Vector<String>> currMtable, final List<String> avaServers) {
        Map<Integer, Vector<String>> hash_table_for_builder_tmp = ServerTableUtil.copyTable(currMtable);
        if (hash_table_for_builder_tmp == null) {
            //hash_table_for_builder_tmp = buildEmptyTable(bucketCount, copyCount);
            //setTableFastRows(ConsoleConstants.DTABLE, hash_table_for_builder_tmp);
            return null;

        }
        LbFirstTableBuilder p_table_builder = new LbFirstTableBuilder(hashCnt, copyCnt);
        Set<ServerInfoBean> ava_server = new HashSet<ServerInfoBean>();
        if (avaServers != null) {
            for (String ser : avaServers) {
                ServerInfoBean node = new ServerInfoBean();
                node.setServer_id(ser);
                ava_server.add(node);
            }
            p_table_builder.set_available_server(ava_server);
            
            return p_table_builder.build_quick_table_alone(hash_table_for_builder_tmp);
        }

        return null;
    }

    private Map<Integer, Vector<String>> buildEmptyTable(int bucketCount, int copyCnt) {
        Map<Integer, Vector<String>> map = new HashMap<Integer, Vector<String>>();
        for (int i = 0; i < copyCnt; i++) {
            Vector<String> nodeV = new Vector<String>();
            for (int j = 0; j < bucketCount; j++) {
                nodeV.add("0");
            }
            map.put(i, nodeV);
        }
        return map;
    }

    private List<String> getDataServerList() {
        return zkClient.getChildren(dataServerPath);
    }

    private boolean checkClusterCapacity(int serverCnt, int limit) {
        if (serverCnt < this.copyCnt) {
            return false;
        }
        int needed = this.copyCnt * this.hashCnt;
        if (needed > serverCnt * limit) {
            return false;
        } else {
            return true;
        }
    }

    private void writeTable(String tableName, HippoClusterTableInfo tableInfo) {
        String tablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + "/" + tableName;
        zkClient.writeData(tablePath, FastjsonUtil.objToJson(tableInfo));
    }

    private List<String> checkDown(List<String> currServerList) {
        return ListUtils.getDiffItemsFromSource(preServerLists, currServerList);
    }

    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) {
        try {
            handleChildChange0(parentPath, currentChilds);
        } catch (Exception e) {
            LOG.error(" error in handle server change ", e);
        }
    }
}
