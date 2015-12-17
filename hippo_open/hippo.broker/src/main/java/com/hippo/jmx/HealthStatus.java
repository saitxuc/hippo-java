package com.hippo.jmx;

import java.io.Serializable;

/**
 * 
 * @author saitxuc
 * 2015-3-16
 */
public class HealthStatus implements Serializable {
    private final String healthId;
    private final String level;
    private final String message;
    private final String resource;

    public HealthStatus(String healthId, String level, String message, String resource) {
        this.healthId = healthId;
        this.level = level;
        this.message = message;
        this.resource = resource;
    }

    public String getHealthId() {
        return healthId;
    }

    public String getLevel() {
        return level;
    }

    public String getMessage() {
        return message;
    }

    public String getResource() {
        return resource;
    }

    public String toString(){
        return healthId + ": " + level + " " + message + " from " + resource;
    }
}
