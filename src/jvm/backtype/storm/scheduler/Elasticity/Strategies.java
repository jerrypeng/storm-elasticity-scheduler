package backtype.storm.scheduler.Elasticity;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.scheduler.Elasticity.GetTopologyInfo.Component;

public class Strategies {
	public static String centralityStrategy(HashMap<String, Component> map) {
		String bestComp = "";
		int mostDegree=0;
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			if((entry.getValue().children.size()+entry.getValue().parents.size()) > mostDegree){
				mostDegree = entry.getValue().children.size()+entry.getValue().parents.size();
				bestComp = entry.getKey();
			}
		}
		return bestComp;
	}
}
