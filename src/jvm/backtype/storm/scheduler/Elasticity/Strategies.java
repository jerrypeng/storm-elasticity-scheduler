package backtype.storm.scheduler.Elasticity;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import backtype.storm.scheduler.Elasticity.GetTopologyInfo.Component;

public class Strategies {
	public static TreeMap<Integer, Component> centralityStrategy(HashMap<String, Component> map) {
		String bestComp = "";
		int mostDegree=0;
		TreeMap<Integer, Component> ret = new TreeMap(Collections.reverseOrder());
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			ret.put(entry.getValue().children.size()+entry.getValue().parents.size(), entry.getValue());
		}
		return ret;
	}
}
