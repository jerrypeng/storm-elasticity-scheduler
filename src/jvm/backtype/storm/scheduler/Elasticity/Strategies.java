package backtype.storm.scheduler.Elasticity;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Strategies {
	private static final Logger LOG = LoggerFactory
			.getLogger(Strategies.class);
	
	public static TreeMap<Component, Integer> centralityStrategy(Map<String, Component> map) {
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), entry.getValue().children.size()+entry.getValue().parents.size());
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		return retMap;
	}
	public static TreeMap<Component, Integer> numDescendantStrategy(Map<String, Component> map) {
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), numDescendants(entry.getValue(), map));
		}
		retMap.putAll(rankMap);
		return retMap;
	}
	
	private static Integer numDescendants(Component com, Map<String, Component> map) {
		Integer count=1;
		for (String child : com.children) {
			count+=numDescendants(map.get(child), map);
		}
		return count;
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
