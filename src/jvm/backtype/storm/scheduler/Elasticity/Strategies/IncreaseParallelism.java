package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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

public class IncreaseParallelism extends TopologyHeuristicStrategy{

	public IncreaseParallelism(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		super(globalState, getStats, topo, cluster, topologies);
	}

	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling() {
		LOG.info("!-------Entering IncreaseParallelism----------! ");
		Collection<ExecutorDetails> unassigned = this._cluster.getUnassignedExecutors(this._topo);
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this._globalState.schedState.get(this._topo.getId());
		Map<ExecutorDetails, WorkerSlot> ExecutorToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
		Collection<ExecutorDetails> topoExecutors = new ArrayList<ExecutorDetails>();
		List<Node> newNodes = this._globalState.getNewNode();
		
		if(newNodes.size()<=0) {
			LOG.error("No new Nodes!");
			return null;
		}
		
		Node targetNode = newNodes.get(0);
		
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

		Integer i=0;
		for(ExecutorDetails exec : execs1) {
			if(i>=targetNode.slots.size()) {
				i=0;
			}
			WorkerSlot target_ws = targetNode.slots.get(i);
			LOG.info("target location: {}:{}", targetNode.hostname, target_ws.getPort());
			if(schedMap.containsKey(target_ws)==false) {
				schedMap.put(target_ws, new ArrayList<ExecutorDetails>());
			}
			schedMap.get(target_ws).add(exec);
			i++;
		}
		
		LOG.info("!-------Exit IncreaseParallelism----------! ");
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

	@Override
	public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		// TODO Auto-generated method stub
		return null;
	}

}
