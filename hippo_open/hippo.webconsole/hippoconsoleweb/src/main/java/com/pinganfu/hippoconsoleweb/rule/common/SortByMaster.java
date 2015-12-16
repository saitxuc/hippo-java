package com.pinganfu.hippoconsoleweb.rule.common;

import java.util.Comparator;

import com.pinganfu.hippoconsoleweb.model.TongModel;

public class SortByMaster implements Comparator<Object> {

	@Override
	public int compare(Object o1, Object o2) {
		TongModel t1 = (TongModel) o1; 
		TongModel t2 = (TongModel) o2;
				
		if(t1.getIsMaster() < t2.getIsMaster()){
			return 0;
		}else{
			return 1;
		}
	}

}
