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

public abstract class LinkLoadBasedStrategy implements IStrategy{
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
		HashMap<String,GetStats.ComponentStats> components = this._getStats.componentStats.get(this._topo.getId());
		 HashMap<String, List<Integer>> compTransferHistory = this._getStats.transferThroughputHistory.get(this._topo.getId());
		
		HashMap<Component, Double> compThroughput = new HashMap<Component, Double>();
		ComponentComparator bvc = new ComponentComparator(compThroughput);
		this.ComponentThroughputRank = new TreeMap<Component, Double>(bvc);
		for(Map.Entry<String, ComponentStats> entry : components.entrySet()) {
			Double movAvg = HelperFuncs.computeMovAvg(compTransferHistory.get(entry.getKey()));
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
	
	abstract public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling();
	
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
	
	public List<ExecutorDetails>getParentTasks(Component comp) {
		List<ExecutorDetails> retVal = new ArrayList<ExecutorDetails>();
		for(String parent : comp.parents) {
			Component c = this._globalState.components.get(this._topo.getId()).get(parent);
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

