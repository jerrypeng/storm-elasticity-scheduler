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
//		for(Entry<Node, Integer> entry : rankedMap.entrySet()) {
//			this._rankedList.add(entry.getKey());
//		}
		this.LOG = LoggerFactory
				.getLogger(this.getClass());
		
	}
	
	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling(ArrayList<String> hostname) {
		LOG.info("!-------Entering getNewScheduling----------! ");
		Collection<ExecutorDetails> unassigned = this._cluster.getUnassignedExecutors(this._topo);
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this._globalState.schedState.get(this._topo.getId());
		Map<WorkerSlot, List<ExecutorDetails>> newSchedMap = this._globalState.schedState.get(this._topo.getId());
		Map<ExecutorDetails, WorkerSlot> ExecutorToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
		Collection<ExecutorDetails> topoExecutors = new ArrayList<ExecutorDetails>();
		
		Map<String, Map<String, Integer>> nodeCompMap = this.getComponentToNodeScheduling(schedMap);
		Map<WorkerSlot, Map<String, Integer>> workerCompMap = this.getComponentWorkerScheduler(schedMap);
		
		ArrayList<String> supsRm = new ArrayList<String>();
		for(String host : hostname) {
			for(Node n : this._globalState.nodes.values()) {
				if(n.hostname.equals(host)==true) {
					LOG.info("Found Hostname: {} with sup id: {}", host, n.supervisor_id);
					//this.removeNodeBySupervisorId(n.supervisor_id);
					supsRm.add(n.supervisor_id);
				}
			}
		}
		
	
		
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
		
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : schedMap.entrySet()) {
			
			newSchedMap.put(entry.getKey(), new ArrayList<ExecutorDetails> ());
			
			if(supsRm.contains(entry.getKey().getNodeId()) == false){
				for(ExecutorDetails exec : entry.getValue()) {
					if(unassigned.contains(exec) == true) {
						newSchedMap.get(entry.getKey()).add(exec);
					}
				}
			}
		}
		
		LOG.info("initial: ");

		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : newSchedMap.entrySet()) {
			String hname = this._globalState.nodes.get(entry.getKey().getNodeId()).hostname + ":" + entry.getKey().getPort();
			LOG.info("Slot: {} execs: {}", hname, entry.getValue());
		}
		
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : newSchedMap.entrySet()) {
			Map<String, Integer> compNum = workerCompMap.get(entry.getKey());
			for(Entry<String, Integer> e : compNum.entrySet()) {
				Integer count = this.findCompInstancs(e.getKey(), entry.getValue());
				int diff = e.getValue()-count;
				for(int i=0; i<diff; i++) {
					
					ExecutorDetails ed = this.getAndRmExecsOfComp(e.getKey(), execs1);
					if(ed == null) {
						LOG.info("ERROR: Cannot find another instance of {}", e.getKey());
						return null;
					}
					newSchedMap.get(entry.getKey()).add(ed);
				}
			}

		}

		LOG.info("Final: ");
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : newSchedMap.entrySet()) {
			String hname = this._globalState.nodes.get(entry.getKey().getNodeId()).hostname + ":" + entry.getKey().getPort();
			LOG.info("Slot: {} execs: {}", hname, entry.getValue());
		}
		
		LOG.info("!-------Exit getNewScheduling----------! ");
		return newSchedMap;
	}
	
	public ExecutorDetails getAndRmExecsOfComp(String comp, List<ExecutorDetails> execs) {
		for (Iterator<ExecutorDetails> iterator = execs.iterator(); iterator.hasNext();) {
			ExecutorDetails exec = iterator.next();
			if(this._topo.getExecutorToComponent().get(exec).equals(comp) == true) {
				iterator.remove();
				return exec;
			}
		}
		return null;
	}
	
	public Integer findCompInstancs(String comp, List<ExecutorDetails> execs) {
		int count=0;
		for(ExecutorDetails exec : execs) {
			if(this._topo.getExecutorToComponent().get(exec).equals(comp) ==true) {
				count++;
			}
		}
		return count;
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
	
	public Map<WorkerSlot, Map<String, Integer>> getComponentWorkerScheduler(Map<WorkerSlot, List<ExecutorDetails>> schedMap) {
		
		Map<WorkerSlot, Map<String, Integer>> workerCompMap = new HashMap<WorkerSlot, Map<String, Integer>>();

		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : schedMap.entrySet()) {
			if(workerCompMap.containsKey(entry.getKey())==false) {
				workerCompMap.put(entry.getKey(), new HashMap<String, Integer>());
			}
			for(ExecutorDetails exec : entry.getValue()) {
				Map<ExecutorDetails, String> compToExecutor = this._globalState.storedCompToExecMapping.get(this._topo.getId());
				String comp=compToExecutor.get(exec);
				if (workerCompMap.get(entry.getKey()).containsKey(comp) == false) {
					workerCompMap.get(entry.getKey()).put(comp, 0);
				}
				int curr = workerCompMap.get(entry.getKey()).get(comp);
				workerCompMap.get(entry.getKey()).put(comp, curr+1);
			}
		}
		for(Entry<WorkerSlot, Map<String, Integer>> entry : workerCompMap.entrySet()) {
			String hostname = this._globalState.nodes.get(entry.getKey().getNodeId()).hostname + ":" + entry.getKey().getPort();
			LOG.info("Slot: {} Components: {}", hostname, entry.getValue());
		}
		return workerCompMap;
	}
	
	public Map<String, Map<String, Integer>> getComponentToNodeScheduling(Map<WorkerSlot, List<ExecutorDetails>> schedMap) {
		//supId->{compId->numOfInstances}
		Map<String, Map<String, Integer>> nodeCompMap = new HashMap<String, Map<String, Integer>>();
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : schedMap.entrySet()) {
			if(nodeCompMap.containsKey(entry.getKey().getNodeId())==false) {
				nodeCompMap.put(entry.getKey().getNodeId(), new HashMap<String, Integer>());
			}
			
			for(ExecutorDetails exec : entry.getValue()) {
				Map<ExecutorDetails, String> compToExecutor = this._globalState.storedCompToExecMapping.get(this._topo.getId());
				String comp=compToExecutor.get(exec);
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
