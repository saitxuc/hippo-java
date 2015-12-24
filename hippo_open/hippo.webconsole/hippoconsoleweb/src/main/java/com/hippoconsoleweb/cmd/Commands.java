package com.hippoconsoleweb.cmd;

public enum Commands {
    GET("GET", 0), SET("SET", 1), UPDATE("UPDATE", 2), REMOVE("REMOVE", 3), INC("INC", 4), DECR("DECR", 5), EXIT("EXIT", 6);

    private String key;

    private int val;

    private Commands(String key, int val) {
        this.key = key;
        this.val = val;
    }

    public String getKey() {
        return key;
    }

    public int getVal() {
        return val;
    }

    public static int getValueIndex(String key) {
        for (Commands c : Commands.values()) {
            if (c.getKey().equals(key.toUpperCase())) {
                return c.getVal();
            }
        }
        return -1;
    }

    public String toString() {
        return this.key + "_" + this.val;
    }
}
