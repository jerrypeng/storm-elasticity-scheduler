package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.Component;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.Node;
import backtype.storm.scheduler.Elasticity.GetStats.ComponentStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.HelperFuncs;

public class LinkLoadBasedStrategy implements IStrategy{
	protected Logger LOG = null;
	protected GlobalState _globalState;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;
	
	protected TreeMap<Component, Double> ComponentThroughputRank;
	protected HashMap<ExecutorDetails, Integer> taskThoughput;

	public LinkLoadBasedStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		
		this.ComponentThroughputRank = new TreeMap<Component, Double>();
		this.taskThoughput = new HashMap<ExecutorDetails, Integer>();
		
		this.LOG = LoggerFactory
				.getLogger(this.getClass());
		
		this.prepare();
	}
	private void prepare() {
		HashMap<String, Integer> transferTable=this._getStats.transferStatsTable;
		HashMap<String,GetStats.ComponentStats> components = this._getStats.componentStats;
		
		HashMap<Component, Double> compThroughput = new HashMap<Component, Double>();
		ComponentComparator bvc = new ComponentComparator(compThroughput);
		this.ComponentThroughputRank = new TreeMap<Component, Double>(bvc);
		for(Map.Entry<String, ComponentStats> entry : components.entrySet()) {
			Double movAvg = HelperFuncs.computeMovAvg(entry.getValue().transferThroughputHistory);
			Component comp = this._globalState.components.get(this._topo.getId()).get(entry.getKey());
			compThroughput.put(comp, movAvg);
		}
		this.ComponentThroughputRank.putAll(compThroughput);
		for(Map.Entry<String, Integer> entry : transferTable.entrySet()) {
			String[] parts = entry.getKey().split(":");
			String taskId = parts[parts.length-1];
			this.taskThoughput.put(HelperFuncs.idToExecutor(Integer.valueOf(taskId), this._topo), entry.getValue());
		}
	}
	TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		return null;
	}
	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling() {
		Map<String, Component> components = this._globalState.components.get(this._topo.getId());
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
		
		for (Component comp : this.ComponentThroughputRank.keySet()) {
			List<ExecutorDetails> compTasks = new ArrayList<ExecutorDetails>();
			compTasks.addAll(comp.execs);
			List<ExecutorDetails> childrenTasks = this.getChildrenTasks(comp);
			
			LOG.info("comTasks: {}", compTasks);
			LOG.info("childrenTasks: {}", childrenTasks);
			
			Iterator<ExecutorDetails> compTasksItr = compTasks.iterator();
			Iterator<ExecutorDetails> childrenTasksItr = childrenTasks.iterator();
			while(migratedTasks.size()<THRESHOLD){
				if(compTasksItr.hasNext()){
					ExecutorDetails exec = compTasksItr.next();
					this._globalState.migrateTask(exec, target_ws, this._topo);
					migratedTasks.add(exec);
				}
				
				if(childrenTasksItr.hasNext()) {
					ExecutorDetails exec = childrenTasksItr.next();
					this._globalState.migrateTask(exec, target_ws, this._topo);
					migratedTasks.add(exec);
				}
			}
			
		}
		LOG.info("Tasks migrated: {}", migratedTasks);
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this._globalState.schedState.get(this._topo.getId());
		return schedMap;
	}
	
	public Integer thresholdFunction() {
		return (this._topo.getExecutors().size())/this._cluster.getSupervisors().size();
	}
	
	/**Helper funcs**/
	public List<ExecutorDetails>getChildrenTasks(Component comp) {
		List<ExecutorDetails> retVal = new ArrayList<ExecutorDetails>();
		for(String child : comp.children) {
			Component c = this._globalState.components.get(this._topo.getId()).get(child);
			retVal.addAll(c.execs);
		}
		return retVal;
	}
	public class ComponentComparator implements Comparator<Component> {

		HashMap<Component, Double> base;
	    public ComponentComparator(HashMap<Component, Double> base) {
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
	public class TaskComparator implements Comparator<ExecutorDetails> {

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
	
}

