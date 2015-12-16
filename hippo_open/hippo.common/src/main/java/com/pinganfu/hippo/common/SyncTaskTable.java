package com.pinganfu.hippo.common;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class SyncTaskTable implements Serializable {
    /**  */
    private static final long serialVersionUID = -298276907709838641L;

    private long endSyncTime;

    private long endExpiredTime;

    private LinkedList<SyncDataTask> syncTasks = null;

    private LinkedList<SyncDataTask> verifyTasks = null;

    private Map<String, Set<String>> dbInfos = null;

    public long getEndSyncTime() {
        return endSyncTime;
    }

    public void setEndSyncTime(long endSyncTime) {
        this.endSyncTime = endSyncTime;
    }

    public long getEndExpiredTime() {
        return endExpiredTime;
    }

    public void setEndExpiredTime(long endExpiredTime) {
        this.endExpiredTime = endExpiredTime;
    }

    public LinkedList<SyncDataTask> getSyncTasks() {
        return syncTasks;
    }

    public void setSyncTasks(LinkedList<SyncDataTask> syncTasks) {
        this.syncTasks = syncTasks;
    }

    public LinkedList<SyncDataTask> getVerifyTasks() {
        return verifyTasks;
    }

    public void setVerifyTasks(LinkedList<SyncDataTask> verifyTasks) {
        this.verifyTasks = verifyTasks;
    }

    public Map<String, Set<String>> getDbInfos() {
        return dbInfos;
    }

    public void setDbInfos(Map<String, Set<String>> dbInfos) {
        this.dbInfos = dbInfos;
    }

    public boolean isFinish() {
        synchronized (this) {
            if ((syncTasks == null || syncTasks.size() == 0) && (verifyTasks == null || verifyTasks.size() == 0)) {
                return true;
            } else {
                return false;
            }
        }
    }

}
