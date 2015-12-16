package com.pinganfu.hippoconsoleweb.rule.common;

import java.util.Comparator;

import com.pinganfu.hippoconsoleweb.model.ServerModel;

public class SortByTongNumberDesc implements Comparator<Object> {

	@Override
	public int compare(Object o1, Object o2) {
		ServerModel t1 = (ServerModel) o1; 
		ServerModel t2 = (ServerModel) o2;
		int t1Size = 0;
		int t2Size = 0;
		if(t1!=null && t1.getTongList() !=null){
			t1Size = t1.getTongList().size();
		}
		if(t2!=null && t2.getTongList()!=null){
			t2Size = t2.getTongList().size();
		}
		if(t1Size > t2Size){
			return 0;
		}else{
			return 1;
		}
	}

}
