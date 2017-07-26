package com.ganesh.test;

import java.io.Serializable;

public class GupRecord implements Serializable{

    public GupRecord() {
    }

    private String action;
    private String actionDateTime;

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getActionDateTime() {
        return actionDateTime;
    }

    public void setActionDateTime(String actionDateTime) {
        this.actionDateTime = actionDateTime;
    }

    public String getGupId() {
        return gupId;
    }

    public void setGupId(String gupId) {
        this.gupId = gupId;
    }

    private String gupId;
}
