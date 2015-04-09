package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.Component;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.Node;

public class UnevenScheduler {
	protected Logger LOG = null;
	protected GlobalState _globalState;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	

	public UnevenScheduler(GlobalState globalState, GetStats getStats,
			Cluster cluster, Topologies topologies) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this.LOG = LoggerFactory.getLogger(this.getClass());

	}

	public void schedule() {

		for (TopologyDetails topo : this._topologies.getTopologies()) {

			Map<String, Component> comps = this._globalState.components
					.get(topo.getId());
			
			ArrayList<ExecutorDetails> unassigned = new ArrayList<ExecutorDetails>();
			ArrayList<ExecutorDetails> unassignedSysExecs = new ArrayList<ExecutorDetails>();
			for(ExecutorDetails exec : this._cluster.getUnassignedExecutors(topo)){
				if(topo.getExecutorToComponent().get(exec).matches("(__).*") == false) {
					unassigned.add(exec);
				} else {
					unassignedSysExecs.add(exec);
				}
			}
			
			if (unassigned.size() == 0) {
				continue;
			}
			
			Number numWorkers = (Number)topo.getConf().get(Config.TOPOLOGY_WORKERS);
			LOG.info("Number of workers: {}", numWorkers);
			
			Integer distribution = (int)Math.ceil((double) unassigned.size()
					/ (double) numWorkers.intValue());
			LOG.info("distribution: {}", distribution);
			Map<WorkerSlot, Integer>workerCountMap = new HashMap<WorkerSlot, Integer> ();
			Map<String, Integer>nodeCountMap = new HashMap<String, Integer> ();
			Map<String, Integer>nodeWorkerCount = new HashMap<String, Integer>();
			
			//distribution = Math.ceil(distribution);
			ArrayList<Node> nodes = new ArrayList<Node>(
					this._globalState.nodes.values());
			int x = 0;
			for(int i=0; i< unassigned.size(); i++) {
				if(x>=nodes.size()) {
					x=0;
				}
				if(nodeCountMap.containsKey(nodes.get(x).supervisor_id)==false) {
					nodeCountMap.put(nodes.get(x).supervisor_id, 0);
				}
				nodeCountMap.put(nodes.get(x).supervisor_id, nodeCountMap.get(nodes.get(x).supervisor_id) + 1);
				x++;
			}
			
			LOG.info("nodeCountMap: {}", nodeCountMap);
			
			x = 0;
			for(int i=0; i< numWorkers.intValue(); i++) {
				if(x>=nodes.size()) {
					x=0;
				}
				if(nodeWorkerCount.containsKey(nodes.get(x).supervisor_id)==false) {
					nodeWorkerCount.put(nodes.get(x).supervisor_id, 0);
				}
				nodeWorkerCount.put(nodes.get(x).supervisor_id, nodeWorkerCount.get(nodes.get(x).supervisor_id) + 1);
				x++;
			}
			
			ArrayList<WorkerSlot> slots = new ArrayList<WorkerSlot>();
			LOG.info("nodeWorkerCount: {}", nodeWorkerCount);
			for(Entry<String, Integer> entry : nodeWorkerCount.entrySet()) {
				ArrayList<WorkerSlot> ws = this.findEmptySlots(this._globalState.nodes.get(entry.getKey()), entry.getValue());
				if (ws != null) {
					slots.addAll(ws);
				}
			}
			LOG.info("slots: {}",slots);
//			for(Node n : nodes) {
//				for (int i = 0; i < numWorkers.intValue(); i++) {
//					WorkerSlot ws = this.findEmptySlot(n, );
//					if (ws != null) {
//						slots.add(ws);
//					}
//				}
//			}
			x = 0;
			for(int i=0; i< unassigned.size(); i++) {
				if(x>=slots.size()) {
					x=0;
				}
				LOG.info("x: {} -- {}", x, slots.get(x));
				if(workerCountMap.containsKey(slots.get(x))==false) {
					workerCountMap.put(slots.get(x), 0);
				}
				workerCountMap.put(slots.get(x), workerCountMap.get(slots.get(x)) + 1);
				x++;
			}
			
			LOG.info("workerCountMap: {}", workerCountMap);
			
			Map<String, ArrayList<ExecutorDetails>> compToExec = new HashMap<String, ArrayList<ExecutorDetails>>();
			for(ExecutorDetails exec: unassigned) {
				String comp = topo.getExecutorToComponent().get(exec);
				if(compToExec.containsKey(comp) == false) {
					compToExec.put(comp, new ArrayList<ExecutorDetails>());
				}
				compToExec.get(comp).add(exec);
			}
			
			Map<WorkerSlot, ArrayList<ExecutorDetails>> schedMap = new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>();
			int i = 0;
			for(Entry<String, ArrayList<ExecutorDetails>> entry : compToExec.entrySet()) {
				for(ExecutorDetails exec : entry.getValue()) {
					if (i >= slots.size()) {
						i = 0;
					}

					//WorkerSlot ws = this.findBestSlot2(nodes.get(i));
					WorkerSlot ws = slots.get(i);
					if (schedMap.containsKey(ws) == false) {
						schedMap.put(ws, new ArrayList<ExecutorDetails>());
					}
					schedMap.get(ws).add(exec);
					if (schedMap.get(ws).size() >= workerCountMap.get(ws)) {
						i++;
					}
				}
			}
			
			//round robin sys tasks
			Iterator it = schedMap.entrySet().iterator();
			for(ExecutorDetails exec : unassignedSysExecs) {
				if(it.hasNext()==false) {
					it = schedMap.entrySet().iterator();
				} 
				Entry<WorkerSlot, ArrayList<ExecutorDetails>>  pair = (Entry<WorkerSlot, ArrayList<ExecutorDetails>>) it.next();
				pair.getValue().add(exec);
				
			}
		
			LOG.info("SchedMap: {}", schedMap);
			if (schedMap != null) {
				//this._cluster.freeSlots(schedMap.keySet());
				for (Entry<WorkerSlot, ArrayList<ExecutorDetails>> sched : schedMap
						.entrySet()) {
					
					if(this._cluster.isSlotOccupied(sched.getKey())==true){
						
					}
				
					this._cluster.assign(sched.getKey(),
							topo.getId(), sched.getValue());
					LOG.info("Assigning {}=>{}",
							sched.getKey(), sched.getValue());
				}
			}

		}
	}
	
	public WorkerSlot findEmptySlot(Node node) {
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : node.slot_to_exec.entrySet()) {
			if(entry.getValue().size() == 0) {
				return entry.getKey();
			}
		}
		return null;
	}
	
	public ArrayList<WorkerSlot> findEmptySlots(Node node, int num) {
		ArrayList<WorkerSlot> slots = new ArrayList<WorkerSlot>();
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : node.slot_to_exec.entrySet()) {
			if(entry.getValue().size() == 0) {
				slots.add(entry.getKey());
			}
			if(slots.size()>=num) {
				return slots;
			}
		}
		return null;
	}

	public WorkerSlot findBestSlot2(Node node) {
		WorkerSlot target = null;
		for (Entry<WorkerSlot, List<ExecutorDetails>> entry : node.slot_to_exec
				.entrySet()) {
			if (target == null) {
				target = entry.getKey();
			}
			if (entry.getValue().size() > 0) {
				target = entry.getKey();
				break;
			}
		}

		return target;
	}

}
