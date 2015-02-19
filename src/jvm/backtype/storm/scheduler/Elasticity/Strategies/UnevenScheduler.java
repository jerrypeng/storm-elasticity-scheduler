package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
			ArrayList<ExecutorDetails> unassigned = new ArrayList<ExecutorDetails>(
					this._cluster.getUnassignedExecutors(topo));
			
			if (unassigned.size() == 0) {
				continue;
			}
			
			Integer distribution = (int)Math.ceil((double) unassigned.size()
					/ (double) this._globalState.nodes.size());
			LOG.info("distribution: {}", distribution);
			//distribution = Math.ceil(distribution);
			ArrayList<Node> nodes = new ArrayList<Node>(
					this._globalState.nodes.values());
			ArrayList<WorkerSlot> slots = new ArrayList<WorkerSlot>();
			for(Node n : nodes) {
				WorkerSlot ws = this.findEmptySlot(n);
				if(ws != null){
					slots.add(ws);
				}
			}
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
					if (schedMap.get(ws).size() >= distribution) {
						i++;
					}
				}
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
