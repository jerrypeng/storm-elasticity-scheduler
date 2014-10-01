package backtype.storm.scheduler.Elasticity;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;


public class Strategies {
	
	public static TreeMap<Component, Integer> centralityStrategy(HashMap<String, Component> map) {
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), entry.getValue().children.size()+entry.getValue().parents.size());
		}
		retMap.putAll(rankMap);
		return retMap;
	}
}

class ComponentComparator implements Comparator<Component> {

	HashMap<Component, Integer> base;
    public ComponentComparator(HashMap<Component, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(Component a, Component b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
