package com.pinganfu.hippoconsoleweb.lisneter;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinganfu.hippo.common.ZkConstants;
import com.pinganfu.hippo.common.domain.HippoClusterMigrateInfo;
import com.pinganfu.hippo.common.domain.HippoClusterTableInfo;
import com.pinganfu.hippo.common.util.FastjsonUtil;
import com.pinganfu.hippo.common.util.ServerTableUtil;
import com.pinganfu.hippoconsoleweb.service.BackupService;
import com.pinganfu.hippoconsoleweb.service.ConsoleConstants;
import com.pinganfu.hippoconsoleweb.service.ZkControlUtil;
import com.pinganfu.hippoconsoleweb.util.ZkUtils;

/**
 * 
 * @author DPJ
 */
public class MigrateInfoChangeListener implements IZkChildListener {

    private Logger LOG = LoggerFactory.getLogger(MigrateInfoChangeListener.class);

    private String zkAddress;
    private String clusterName;
    private BackupService backupService;
    private Object g_dtableLock;
    
    public MigrateInfoChangeListener(String zkAddress, String clusterName, BackupService backupService, Object g_dtableLock) {
        this.zkAddress = zkAddress;
        this.clusterName = clusterName;
        this.backupService = backupService;
        this.g_dtableLock = g_dtableLock;
    }

    @Override
	public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
	    if(null == currentChilds || currentChilds.size() == 0) {
	        LOG.warn("handleChildChange triggered but childs is empty: " + parentPath);
	        return;
	    }	    
	    fillMtableOnOneBucketFinished(parentPath, currentChilds);
	}
    
    public synchronized void fillMtableOnOneBucketFinished(String parentPath, List<String> currentChilds) throws Exception {
        LOG.info("fillMtableOnOneBucketFinished start: " + currentChilds);
        
        ZkClient zkClient = ZkUtils.getZKClient(zkAddress);
        
        String ctablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + ZkConstants.NODE_CTABLE;
        String mtablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + ZkConstants.NODE_MTABLE;
        String dtablePath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_TABLES + ZkConstants.NODE_DTABLE;
        String migrationPath = ZkConstants.DEFAULT_PATH_ROOT + "/" + clusterName + ZkConstants.NODE_MIGRATION;
        
        synchronized(g_dtableLock) {
        	HippoClusterTableInfo mtableInfo = ZkControlUtil.getTableInfoByPath(mtablePath, zkClient);
            HippoClusterTableInfo dtableInfo = ZkControlUtil.getTableInfoByPath(dtablePath, zkClient);
            if(mtableInfo == null || dtableInfo == null) {
                g_dtableLock.notifyAll();
                return;
            }
            if(mtableInfo.getVersion() == dtableInfo.getVersion()) {
                LOG.warn("fillMtableOnOneBucketFinished but version is same between mtable and dtable: {} ", mtableInfo.getVersion());
                LOG.warn("mtable: {} ",  mtableInfo.getTableMap());
                LOG.warn("dtable: {} ",  dtableInfo.getTableMap());
                g_dtableLock.notifyAll();
                return;
            }
            Map<Integer, Vector<String>> mtable = mtableInfo.getTableMap();
            Map<Integer, Vector<String>> dtable = dtableInfo.getTableMap();
            
            LOG.info("fillMtableOnOneBucketFinished mtable: " + mtable);
            LOG.info("fillMtableOnOneBucketFinished dtable: " + dtable);
            
            Map<String, Set<Integer>> machineBuckets = ServerTableUtil.getMasterToBeMigratedBucketMap(mtable, dtable);
            Set<Integer> allMigBuckets = new HashSet<Integer>();
            Iterator<Set<Integer>> allBucketsIter = machineBuckets.values().iterator();
            while(allBucketsIter.hasNext()) {
                allMigBuckets.addAll(allBucketsIter.next());
            }
            int copyCnt = mtable.keySet().size();
            boolean dtableUpdated = false;
            Iterator<String> iter = currentChilds.iterator();
            while(iter.hasNext()) {
                String bucket = iter.next();
                String migInfoStr = ZkUtils.getData(zkAddress, migrationPath+"/"+bucket);
                HippoClusterMigrateInfo migInfo = FastjsonUtil.jsonToObj(migInfoStr, HippoClusterMigrateInfo.class);
                if(migInfo != null) {
                    allMigBuckets.remove(migInfo.getBucket());
                    if(migInfo.getFailServers().size() > 0) {
                        Iterator<String> okServersIter = migInfo.getOkServers().iterator();
                        while(okServersIter.hasNext()) {
                            String server = okServersIter.next();
                            for(int i=0; i<copyCnt; i++) {
                                if(server.equals(dtable.get(i).get(migInfo.getBucket()))) {
                                    mtable.get(i).set(migInfo.getBucket(), server);
                                }
                            }
                        }
                        
                        Iterator<String> failServersIter = migInfo.getFailServers().iterator();
                        while(failServersIter.hasNext()) {
                            String server = failServersIter.next();
                            for(int i=0; i<copyCnt; i++) {
                                if(server.equals(dtable.get(i).get(migInfo.getBucket()))) {
                                    dtable.get(i).set(migInfo.getBucket(), ConsoleConstants.INVALID_FLAG);
                                    dtableUpdated = true;
                                }
                            }
                        }
                    } else {
                    	// all ok, simple replace bucket's server
                    	/***
                    	String oldmaster = mtable.get(0).get(migInfo.getBucket());
                    	for(int i=0; i<copyCnt; i++) {
                    		if(oldmaster.equals(dtable.get(i).get(migInfo.getBucket()))) {
                    			mtable.get(i).set(migInfo.getBucket(), oldmaster);
                            }
                    	}
                    	Iterator<String> okServersIter = migInfo.getOkServers().iterator();
                        while(okServersIter.hasNext()) {
                            String server = okServersIter.next();
                            for(int i=0; i<copyCnt; i++) {
                                if(server.equals(dtable.get(i).get(migInfo.getBucket()))) {
                                    mtable.get(i).set(migInfo.getBucket(), server);
                                }
                            }
                        }
                        ***/
                    	for(int i=0; i<copyCnt; i++) {
                        	mtable.get(i).set(migInfo.getBucket(), dtable.get(i).get(migInfo.getBucket()));
                        }
                    }
                }
            }
            
            // dtable first
            if(dtableUpdated) {
                // 20150623: should not go here
                ZkUtils.setData(zkAddress, dtablePath, FastjsonUtil.objToJson(dtableInfo)); 
                LOG.warn(" --> fillMtableOnOneBucketFinished set dtable: " + FastjsonUtil.objToJson(dtableInfo));           
            }
            ZkUtils.setData(zkAddress, mtablePath, FastjsonUtil.objToJson(mtableInfo));
            LOG.info("fillMtableOnOneBucketFinished set mtable: " + FastjsonUtil.objToJson(mtableInfo));
            
            if(allMigBuckets.size() == 0) {
                LOG.info("fillMtableOnOneBucketFinished allMigBuckets size = 0");    
                
                // 20150618: re-check if dtable changed when handle migrate info change
                /***
                if(mtableInfo != null && dtableInfo != null) {
                    if(!mtableInfo.getTableMap().toString().equals(dtableInfo.getTableMap().toString())) {
                        return;
                    }
                }
                ***/
                // DPJ: 20150616: remove all childs
            	iter = currentChilds.iterator();
                while(iter.hasNext()) {
                    try {
                        ZkUtils.deleteNode(zkAddress, migrationPath+"/"+iter.next(), false);
                    } catch(Exception e) {
                    }
                }
                
                // 20150623: use dtable instead of mtable to update ctable and mtable
                Map<Integer, Vector<String>> ctable = ServerTableUtil.cutForCtable(dtable);
                HippoClusterTableInfo ctableInfo = new HippoClusterTableInfo();
                ctableInfo.setTableMap(ctable);
                ctableInfo.setVersion(dtableInfo.getVersion());
                ZkUtils.setData(zkAddress, ctablePath, FastjsonUtil.objToJson(ctableInfo));
                LOG.info("fillMtableOnOneBucketFinished set ctable: " + FastjsonUtil.objToJson(ctableInfo)); 
                
                // mtableInfo.setVersion(dtableInfo.getVersion());
                String mtableStr = FastjsonUtil.objToJson(dtableInfo);
                ZkUtils.setData(zkAddress, mtablePath, mtableStr);
                LOG.info("fillMtableOnOneBucketFinished set mtable: " + mtableStr);  

                //--start zk backup
                //ZkBackUtil.zkBackUp(zkAddress,backupService);

            }
            g_dtableLock.notifyAll();
        }
        
        LOG.info("fillMtableOnOneBucketFinished end.");   
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

}
