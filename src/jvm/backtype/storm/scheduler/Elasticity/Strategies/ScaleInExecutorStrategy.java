package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

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
import backtype.storm.scheduler.Elasticity.HelperFuncs;
import backtype.storm.scheduler.Elasticity.Node;

public class ScaleInExecutorStrategy {
	protected static Logger LOG = null;
	protected GlobalState _globalState;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;
	protected TreeMap<Node, Integer> _rankedMap;
	protected ArrayList<Node> _rankedList = new ArrayList<Node>();

	public ScaleInExecutorStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies, TreeMap<Node, Integer> rankedMap) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		this._rankedMap = rankedMap;
		for(Entry<Node, Integer> entry : rankedMap.entrySet()) {
			this._rankedList.add(entry.getKey());
		}
		this.LOG = LoggerFactory
				.getLogger(this.getClass());
		
	}
	
	public Map<String, Map<String, Integer>> getComponentToNodeScheduling(Map<WorkerSlot, List<ExecutorDetails>> schedMap) {
		//supId->{compId->numOfInstances}
		Map<String, Map<String, Integer>> nodeCompMap = new HashMap<String, Map<String, Integer>>();
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : schedMap.entrySet()) {
			if(nodeCompMap.containsKey(entry.getKey().getNodeId())==false) {
				nodeCompMap.put(entry.getKey().getNodeId(), new HashMap<String, Integer>());
			}
			for(ExecutorDetails exec : entry.getValue()) {
				String comp = this._topo.getExecutorToComponent().get(exec);
				if(nodeCompMap.get(entry.getKey().getNodeId()).containsKey(comp) == false) {
					nodeCompMap.get(entry.getKey().getNodeId()).put(comp, 0);
				}
				int curr = nodeCompMap.get(entry.getKey().getNodeId()).get(comp);
				nodeCompMap.get(entry.getKey().getNodeId()).put(comp, curr+1);
			}
		}
		
		for(Entry<String, Map<String, Integer>> entry : nodeCompMap.entrySet()) {
			String hostname = this._globalState.nodes.get(entry.getKey()).hostname;
			LOG.info("node: {} Components: {}", hostname, entry.getValue());
		}
		return nodeCompMap;
	}
	
	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling() {
		LOG.info("!-------Entering getNewScheduling----------! ");
		Collection<ExecutorDetails> unassigned = this._cluster.getUnassignedExecutors(this._topo);
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this._globalState.schedState.get(this._topo.getId());
		Map<ExecutorDetails, WorkerSlot> ExecutorToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
		Collection<ExecutorDetails> topoExecutors = new ArrayList<ExecutorDetails>();
		
		Map<String, Map<String, Integer>> nodeCompMap = this.getComponentToNodeScheduling(schedMap);
		
	
		
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : schedMap.entrySet()) {
			for(ExecutorDetails exec : entry.getValue()) {
				topoExecutors.add(exec);
				ExecutorToSlot.put(exec, entry.getKey());
			}
		}
		
		
		List<ExecutorDetails> execs1 = this.diff(unassigned, topoExecutors);
		List<ExecutorDetails> execs2 = this.diff(topoExecutors, unassigned);
		LOG.info("execs1: {}", execs1);
		LOG.info("execs2: {}", execs2);
		//execs1: [[7, 7], [10, 10], [8, 8], [9, 9]]
		//execs2: [[7, 8], [9, 10]]

		
		for (Iterator<ExecutorDetails> iterator = execs2.iterator(); iterator.hasNext();) {
			ExecutorDetails exec = iterator.next();
			for (Iterator<ExecutorDetails> iterator2 = execs1.iterator(); iterator2.hasNext();) {
				ExecutorDetails TopoExec = iterator2.next();
				if((TopoExec.getEndTask()==exec.getEndTask() && TopoExec.getStartTask() != exec.getStartTask())
						||
						(TopoExec.getEndTask()!=exec.getEndTask() && TopoExec.getStartTask() == exec.getStartTask())) {
					WorkerSlot ws = ExecutorToSlot.get(exec);
					schedMap.get(ws).remove(exec);
					schedMap.get(ws).add(TopoExec);
					iterator.remove();
					iterator2.remove();
					break;
				}
			}
		}
		
		LOG.info("After execs1: {}", execs1);
		LOG.info("After execs2: {}", execs2);

		
		
		LOG.info("!-------Exit getNewScheduling----------! ");
		return schedMap;
	}
	
	List<ExecutorDetails> diff(Collection<ExecutorDetails> execs1, Collection<ExecutorDetails> execs2) {
		List<ExecutorDetails> retList = new ArrayList<ExecutorDetails>();
		for (ExecutorDetails exec : execs1) {
			if(execs2.contains(exec)==false) {
				retList.add(exec);
			}
		}
		return retList;
	}
	
	public void removeNodesBySupervisorId(ArrayList<String> supervisorIds) {
		
		
			Map<Component, Integer> compExecRm = this.findComponentsOnNode(supervisorIds);
			LOG.info("Nodes: {} has Components: {}", supervisorIds, compExecRm);
			
			HelperFuncs.decreaseParallelism(compExecRm, this._topo);
			
	}
	
	public static void decreaseParallelism(Map<Component, Integer> compMap, TopologyDetails topo) {
		String cmd = "/var/storm/storm_0/bin/storm rebalance -w 0 "+topo.getName();
		for(Entry<Component, Integer> entry : compMap.entrySet()) {
			Integer parallelism_hint = entry.getKey().execs.size() - entry.getValue();
			String component_id = entry.getKey().id;
			LOG.info("decreasing parallelism to {} of component {} in topo {}", new Object[]{parallelism_hint, component_id, topo.getName()});
			cmd+=" -e "+component_id+"="+parallelism_hint;
		}

		Process p;
		try {
			LOG.info("cmd: {}", cmd);
			p = Runtime.getRuntime().exec(cmd);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public Map<Component, Integer> findComponentsOnNode(ArrayList<String>supervisorIds) {
		HashMap<Component, Integer> comps = new HashMap<Component, Integer>();
		for(String supervisorId : supervisorIds) {
			Node n = this._globalState.nodes.get(supervisorId);
			for(ExecutorDetails exec : n.execs) {
				String comp = this._topo.getExecutorToComponent().get(exec);
				Component component = this._globalState.components.get(this._topo.getId()).get(comp);
				if(comps.containsKey(component) == false) {
					comps.put(component, 0);
				}
				comps.put(component, comps.get(component)+1);
			}
		}
		return comps;
		
	} 
	
	public void removeNodesByHostname(ArrayList<String> hostname) {
		ArrayList<String> sups = new ArrayList<String>();
		for(String host : hostname) {
			for(Node n : this._globalState.nodes.values()) {
				if(n.hostname.equals(host)==true) {
					LOG.info("Found Hostname: {} with sup id: {}", host, n.supervisor_id);
					//this.removeNodeBySupervisorId(n.supervisor_id);
					sups.add(n.supervisor_id);
				}
			}
		}
		this.removeNodesBySupervisorId(sups);
	}
	
	public WorkerSlot findBestSlot3(Node node) {

		LOG.info("Node: " + node.hostname);
		WorkerSlot target = null;
		int least = Integer.MAX_VALUE;
		for (Entry<WorkerSlot, List<ExecutorDetails>> entry : node.slot_to_exec
				.entrySet()) {
			List<ExecutorDetails> execs = this._globalState.schedState.get(
					this._topo.getId()).get(entry.getKey());
			if (execs != null && execs.size() > 0) {
				LOG.info("-->slots: {} execs {}", entry.getKey().getPort(),
						execs.size());
				if (execs.size() < least) {

					target = entry.getKey();
					least = execs.size();
				}
			}
		}
		return target;
	}
}
