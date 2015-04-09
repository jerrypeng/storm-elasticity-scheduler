package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.Node;

public class ScaleInProximityBased {

	protected Logger LOG = null;
	protected GlobalState _globalState;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;

	public ScaleInProximityBased(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		this.LOG = LoggerFactory
				.getLogger(this.getClass());
		
	}
	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling()
	{
		return this._globalState.schedState.get(this._topo.getId());
	}
	public void removeNodesByHostname(ArrayList<String> hostname) {
		ArrayList<String> sups = new ArrayList<String>();
		for(String host : hostname) {
			for(Node n : this._globalState.nodes.values()) {
				if(n.hostname.equals(host)==true) {
					LOG.info("Found Hostname: {} with sup id: {}", hostname, n.supervisor_id);
					//this.removeNodeBySupervisorId(n.supervisor_id);
					sups.add(n.supervisor_id);
				}
			}
		}
		this.removeNodesBySupervisorId(sups);
	}
	
	public void removeNodesBySupervisorId(ArrayList<String> supervisorIds) {
		ArrayList<Node> removeNodes = new ArrayList<Node>();
		ArrayList<ExecutorDetails> moveExecutors = new ArrayList<ExecutorDetails>();
		for(String sup : supervisorIds) {
			removeNodes.add(this._globalState.nodes.get(sup));
			moveExecutors.addAll(this._globalState.nodes.get(sup).execs);
		}
		
		ArrayList<Node> elgibleNodes = new ArrayList<Node>();
		LOG.info("nodes elgible:");
		for (Node n: this._globalState.nodes.values()) {
			if(removeNodes.contains(n)==false) {
				LOG.info("-->{}", n.hostname);
				elgibleNodes.add(n);
			}
		}
		HashMap<String, ArrayList<ExecutorDetails>>compToExecs = this.getCompToExecs(moveExecutors);

		for(Entry<String, ArrayList<ExecutorDetails>> entry : compToExecs.entrySet()) {
			Component comp = this.getComponent(entry.getKey());
			for(ExecutorDetails exec : entry.getValue()) {
				Node n = this.getBestNode(comp, exec, elgibleNodes);
				WorkerSlot target = this.getBestSlot(n);
				this._globalState.migrateTask(exec, target, this._topo);
				n.execs.add(exec);
				n.slot_to_exec.get(target).add(exec);
				LOG.info("migrating {} to ws {} on node {}", new Object[]{exec, target, n.hostname});


			}
		}
		
		
	}
	
	public void removeNodeByHostname(String hostname) {
		for(Node n : this._globalState.nodes.values()) {
			if(n.hostname.equals(hostname)==true) {
				LOG.info("Found Hostname: {} with sup id: {}", hostname, n.supervisor_id);
				this.removeNodeBySupervisorId(n.supervisor_id);
			}
		}
	}	
	public void removeNodeBySupervisorId(String supervisorId) {
		ArrayList<Node> elgibleNodes = this.getElgibleNodes(supervisorId);
		LOG.info("ElgibleNodes: {}", elgibleNodes);
		HashMap<String, ArrayList<ExecutorDetails>>compToExecs = this.getCompToExecs(this._globalState.nodes.get(supervisorId).execs);
		
		for(Entry<String, ArrayList<ExecutorDetails>> entry : compToExecs.entrySet()) {
			Component comp = this.getComponent(entry.getKey());
			for(ExecutorDetails exec : entry.getValue()) {
				Node n = this.getBestNode(comp, exec, elgibleNodes);
				WorkerSlot target = this.getBestSlot(n);
				this._globalState.migrateTask(exec, target, this._topo);
				n.execs.add(exec);
				n.slot_to_exec.get(target).add(exec);
				LOG.info("migrating {} to ws {} on node {}", new Object[]{exec, target, n.hostname});


			}
		}
		
		
		
		
		
		
	}
	WorkerSlot getBestSlot(Node n) {
		
		Integer leastUsed = Integer.MAX_VALUE;
		WorkerSlot target = null;
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : n.slot_to_exec.entrySet()) {
			if(entry.getValue().size()>0) {
				if(target == null) {
					target = entry.getKey();
					leastUsed = entry.getValue().size();
				}
				if(entry.getValue().size()<leastUsed) {
					target = entry.getKey();
					leastUsed = entry.getValue().size();
				}
			}
		}
		return target;
		
	}
	
	Node getBestNode(Component src, ExecutorDetails exec, ArrayList<Node> elgibleNodes) {
		ArrayList<String> neighbors = new ArrayList<String>();
		//HashMap<Component, HashMap<Node, Integer>> rankMap = new HashMap<Component, HashMap<Node, Integer>>();
		neighbors.addAll(src.children);
		neighbors.addAll(src.parents);
		HashMap<String, Double> CompNodeRankMap = new HashMap<String, Double>();
		for(String comp : neighbors) {
			Component component = this.getComponent(comp);
			TreeMap<Node, Double>rankMap = this.getRank(src, component, elgibleNodes);
			LOG.info("Comp: {} rankMap: {}", component.id, rankMap);
			for(Entry<Node, Double> entry: rankMap.entrySet()) {
				Node n = entry.getKey();
				Double val = entry.getValue();
				String key = n.supervisor_id+":"+comp;
				CompNodeRankMap.put(key, val);
			}
		}
		ValueComparator comparator = new ValueComparator(CompNodeRankMap);
		TreeMap <String, Double> sortedCompNodeRankMap = new TreeMap<String, Double>(comparator);
		sortedCompNodeRankMap.putAll(CompNodeRankMap);
		String supId = sortedCompNodeRankMap.firstKey().split(":")[0];
		LOG.info("sortedCompNodeRankMap: {}",sortedCompNodeRankMap );
		return this._globalState.nodes.get(supId);
			
		
	}
	
	TreeMap<Node, Double> getRank(Component src, Component dest, ArrayList<Node> elgibleNodes) {
		HashMap<Node, Double> results = new HashMap<Node, Double>();
		Double totalNumOfExecs = (double) this._topo.getExecutors().size();
		for(Node n : elgibleNodes) {
			Double srcInstances = this.numOfInstances(src, n).doubleValue();
			Double destInstances = this.numOfInstances(dest, n).doubleValue();
			
			Double ratio = srcInstances/(destInstances+1) *n.execs.size()/(totalNumOfExecs/elgibleNodes.size());
			results.put(n, ratio);
			
		}
		RankValueComparator comparator = new RankValueComparator(results);
		TreeMap<Node, Double> ret = new TreeMap<Node, Double>(comparator);
		ret.putAll(results);
		return ret;
		
	}
	
	Integer numOfInstances(Component comp, Node node) {
		ArrayList<ExecutorDetails>tmp = new ArrayList<ExecutorDetails>(comp.execs);
		tmp.retainAll(node.execs);
		return tmp.size();
	}
	
	Component getComponent(String name) {
		return this._globalState.components.get(this._topo.getId()).get(name);
	}
	
	HashMap<String, ArrayList<ExecutorDetails>> getCompToExecs(List<ExecutorDetails> execs) {
		HashMap<String, ArrayList<ExecutorDetails>> compToExecs = new HashMap<String, ArrayList<ExecutorDetails>>();
		for (ExecutorDetails exec : execs) {
			String comp = this._topo.getExecutorToComponent().get(exec);
			if(compToExecs.containsKey(comp) == false) {
				compToExecs.put(comp, new ArrayList<ExecutorDetails>());
			}
			compToExecs.get(comp).add(exec);
		}
		return compToExecs;
	}
	
	
	ArrayList<Node> getElgibleNodes(String supervisorId) {
		ArrayList<Node> elgibleNodes = new ArrayList<Node>();
		LOG.info("nodes elgible:");
		for (Node n: this._globalState.nodes.values()) {
			if(n.supervisor_id.equals(supervisorId)==false) {
				LOG.info("-->{}", n.hostname);
				elgibleNodes.add(n);
			}
		}
		return elgibleNodes;
	}
	class RankValueComparator implements Comparator<Node> {

	    Map<Node, Double> base;
	    public RankValueComparator(Map<Node, Double> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(Node a, Node b) {
	        if (base.get(a) <= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}
	class ValueComparator implements Comparator<String> {

	    Map<String, Double> base;
	    public ValueComparator(Map<String, Double> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(String a, String b) {
	        if (base.get(a) <= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}
}
