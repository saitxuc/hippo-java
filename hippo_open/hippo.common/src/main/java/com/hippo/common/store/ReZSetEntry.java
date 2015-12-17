package com.hippo.common.store;

public class ReZSetEntry {
    private Object key;
    private Object score;

    public ReZSetEntry() {

    }

    public ReZSetEntry(Object key, Object score) {
        this.key = key;
        this.score = score;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getScore() {
        return score;
    }

    public void setScore(Object score) {
        this.score = score;
    }

    public String toString() {
        return "ReZSetEntry:[" + this.key + "," + this.score + "]";
    }

}
