package com.hippo.network.disruptor;

public class MutableObject {

Object o = null;
    
    public MutableObject() {
        
    }

    public MutableObject(Object o) {
        this.o = o;
    }
    
    public void setObject(Object o) {
        this.o = o;
    }
    
    public Object getObject() {
        return o;
    }
	
}
