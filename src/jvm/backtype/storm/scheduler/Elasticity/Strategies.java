package backtype.storm.scheduler.Elasticity;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.ExecutorDetails;

 
public class Strategies {
	private static final Logger LOG = LoggerFactory
			.getLogger(Strategies.class);
	/**
	 * rank central nodes (D)
	 * @param map
	 * @return
	 */ 
	public static TreeMap<ExecutorDetails, Integer> CentralityStrategy(Map<String, Component> map) {
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), entry.getValue().children.size()+entry.getValue().parents.size());
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);

		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	/**
	 * rank nodes with most descendant (C)
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> numDescendantStrategy(Map<String, Component> map) {
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), numDescendants(entry.getValue(), map));
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	
	/**
	 * rank nodes closest to spout (A)
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> SourceClosenessStrategy(Map<String, Component> map){
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			Integer reverse=0-distToSpout(entry.getValue(),map);
			rankMap.put(entry.getValue(), reverse);
			LOG.info("{}",reverse);
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	
	/**
	 * rank nodes closest to bolt (B)
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> SinkClosenessStrategy(Map<String, Component> map){
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			Integer reverse=0-distToBolt(entry.getValue(),map);
			rankMap.put(entry.getValue(), reverse);
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	
	/**
	 * hybrid A+C
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> SourceDescendantStrategy(Map<String, Component> map){
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), numDescendants(entry.getValue(), map)-distToSpout(entry.getValue(),map));
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	
	/**
	 * hybrid B+C
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> SinkDescendantStrategy(Map<String, Component> map){
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), numDescendants(entry.getValue(), map)-distToBolt(entry.getValue(),map));
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	/**
	 * hybrid A+D
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> SourceCentralityStrategy(Map<String, Component> map){
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), entry.getValue().children.size()+entry.getValue().parents.size()-distToSpout(entry.getValue(),map));
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	/**
	 * hybrid B+D
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> SinkCentralityStrategy(Map<String, Component> map){
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), entry.getValue().children.size()+entry.getValue().parents.size()-distToBolt(entry.getValue(),map));
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	/**
	 * hybrid A+C+D
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> SourceDescendantCentralityStrategy(Map<String, Component> map){
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), numDescendants(entry.getValue(), map)-distToSpout(entry.getValue(),map)+entry.getValue().children.size()+entry.getValue().parents.size());
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	
	/**
	 * hybrid B+C+D
	 * @param map
	 * @return
	 */
	public static TreeMap<ExecutorDetails, Integer> SinkDescendantCentralityStrategy(Map<String, Component> map){
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), numDescendants(entry.getValue(), map)-distToBolt(entry.getValue(),map)+entry.getValue().children.size()+entry.getValue().parents.size());
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		HashMap<ExecutorDetails, Integer> taskRank = new HashMap<ExecutorDetails, Integer>();
		TaskComparator tvc = new TaskComparator(taskRank);
		TreeMap<ExecutorDetails, Integer> sortedTaskRank = new TreeMap<ExecutorDetails, Integer>(tvc);
		for(Map.Entry<Component, Integer> entry : retMap.entrySet()) {
			for(ExecutorDetails exec : entry.getKey().execs) {
				taskRank.put(exec, entry.getValue());
			}
		}
		sortedTaskRank.putAll(taskRank);
		return sortedTaskRank;
	}
	
	public static TreeMap<ExecutorDetails, Integer> EdgeAware(Map<String, Component> map, Map<String, Integer>edges) {
		return null;
	}
	
	
	/****helper****/
	
	private static Integer distToBolt(Component com, Map<String, Component> map) {
		Integer max=0;
		for (String child : com.children) {
			max=Math.max(distToBolt(map.get(child), map)+1, max);
		}
		LOG.info("{}",max);
		return max;
	}
	private static Integer distToSpout(Component com, Map<String, Component> map) {
		Integer max=0;
		for (String parent : com.parents) {
			max=Math.max(distToSpout(map.get(parent), map)+1, max);
		}
		LOG.info("{}",max);
		return max;
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

class TaskComparator implements Comparator<ExecutorDetails> {

	HashMap<ExecutorDetails, Integer> base;
    public TaskComparator(HashMap<ExecutorDetails, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(ExecutorDetails a, ExecutorDetails b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}