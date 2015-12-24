package com.hippoconsoleweb.rule.common;

import java.util.Comparator;

import com.hippoconsoleweb.model.ServerModel;
import com.hippoconsoleweb.model.TongModel;

public class SortByMasterTongNumber implements Comparator<Object> {

	@Override
	public int compare(Object o1, Object o2) {
		ServerModel t1 = (ServerModel) o1; 
		ServerModel t2 = (ServerModel) o2;
		int t1Number = 0;
		int t2Number = 0;
		for(TongModel tong:t1.getTongList()){
			if(tong.getIsMaster() == 1 ){
				t1Number++;
			}
		}
		
		for(TongModel tong:t2.getTongList()){
			if(tong.getIsMaster() == 1 ){
				t2Number++;
			}
		}
				
		if(t1Number < t2Number){
			return 0;
		}else{
			return 1;
		}
	}

}
