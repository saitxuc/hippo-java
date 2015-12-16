package com.pinganfu.hippo.jmx;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author saitxuc
 * 2015-3-16
 */
public class HealthView implements HealthViewMBean {
	
	String currentState = "Good";
	
	@Override
	public List<HealthStatus> healthList() throws Exception {
		List<HealthStatus> answer = new ArrayList<HealthStatus>();
		
		if (answer != null && !answer.isEmpty()) {
			this.currentState = "Feeling Ill {";
			for (HealthStatus hs : answer) {
				currentState += hs + " , ";
			}
			currentState += " }";
		} else {
			this.currentState = "Good";
		}
		return answer;
	}

	@Override
	public String getCurrentStatus() {
		// TODO Auto-generated method stub
		return currentState;
	}

}
