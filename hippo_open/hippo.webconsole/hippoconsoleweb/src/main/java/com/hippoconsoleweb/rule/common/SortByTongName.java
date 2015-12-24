package com.hippoconsoleweb.rule.common;

import java.util.Comparator;

import com.hippoconsoleweb.model.TongModel;

public class SortByTongName implements Comparator<Object> {

	@Override
	public int compare(Object o1, Object o2) {
		TongModel t1 = (TongModel) o1; 
		TongModel t2 = (TongModel) o2;
				
		if(t1.getTongMark().compareTo(t2.getTongMark()) < 0 ){
			return 0;
		}else{
			return 1;
		}
	}

}
