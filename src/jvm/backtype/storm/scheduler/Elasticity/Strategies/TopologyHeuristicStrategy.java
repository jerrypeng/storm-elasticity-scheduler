package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.Component;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.Node;

public abstract class TopologyHeuristicStrategy implements IStrategy{
	protected Logger LOG = null;
	protected GlobalState _globalState;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;

	public TopologyHeuristicStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		this.LOG = LoggerFactory
				.getLogger(this.getClass());
		
	}
	
	public abstract TreeMap<Component, Integer> Strategy(Map<String, Component> map);
	
	public Integer thresholdFunction() {
		return (this._topo.getExecutors().size())/this._cluster.getSupervisors().size();
	}

	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling() {
		Map<String, Component> components = this._globalState.components.get(this._topo.getId());
		TreeMap<Component, Integer> priorityQueue = this.Strategy(components);
		
		LOG.info("priorityQueue: {}", priorityQueue);
		
		List<Node> newNodes = this._globalState.getNewNode();
		
		if(newNodes.size()<=0) {
			LOG.error("No new Nodes!");
			return null;
		}
		
		Node targetNode = newNodes.get(0);
		WorkerSlot target_ws = targetNode.slots.get(0);
		LOG.info("target location: {}:{}", targetNode.hostname, target_ws.getPort());
		
		int THRESHOLD = this.thresholdFunction();
		LOG.info("Threshold: {}", THRESHOLD);
		List<ExecutorDetails> migratedTasks = new ArrayList<ExecutorDetails>();
		for (Component comp : priorityQueue.keySet()) {
			if(migratedTasks.size()>=THRESHOLD) {
				break;
			}
			for(ExecutorDetails exec : comp.execs) {
				if(migratedTasks.size()>=THRESHOLD) {
					break;
				}
				this._globalState.migrateTask(exec, target_ws, this._topo);
				migratedTasks.add(exec);
			}
		}
		LOG.info("Tasks migrated: {}", migratedTasks);
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this._globalState.schedState.get(this._topo.getId());
		return schedMap;
	}
	
/****helper****/
	
	protected  Integer distToBolt(Component com, Map<String, Component> map) {
		Integer max=0;
		for (String child : com.children) {
			max=Math.max(distToBolt(map.get(child), map)+1, max);
		}
		LOG.info("{}",max);
		return max;
	}
	protected  Integer distToSpout(Component com, Map<String, Component> map) {
		Integer max=0;
		for (String parent : com.parents) {
			max=Math.max(distToSpout(map.get(parent), map)+1, max);
		}
		LOG.info("{}",max);
		return max;
	}
	protected  Integer numDescendants(Component com, Map<String, Component> map) {
		Integer count=1;
		for (String child : com.children) {
			count+=numDescendants(map.get(child), map);
		}
		return count;
	}
	
	public class ComponentComparator implements Comparator<Component> {

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
	
	public class NodeComparator implements Comparator<Node> {

		HashMap<Node, Integer> base;
	    public NodeComparator(HashMap<Node, Integer> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(Node o1, Node o2) {
			if (base.get(o1) >= base.get(o2)) {
	            return 1;
	        } else {
	            return -1;//pick the lowest ETP
	        } // returning 0 would merge keys
		}
	}
	
}
