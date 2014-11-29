package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.HashMap;
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

public class IncreaseParallelismTest extends TopologyHeuristicStrategy{

	public IncreaseParallelismTest(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		super(globalState, getStats, topo, cluster, topologies);
	}

	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling() {
		HelperFuncs.changeParallelism(this._topo.getId(), "exclaim2", 4);
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = new HashMap<WorkerSlot, List<ExecutorDetails>>();
		for(Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : this._globalState.schedState.get(this._topo.getId()).entrySet()) {
			schedMap.put(entry.getKey(), new ArrayList<ExecutorDetails>());
			for(ExecutorDetails exec : entry.getValue()) {
				if(exec.getStartTask() != exec.getEndTask()) {
					LOG.info("Exec: {} split into:");
					for(int i=exec.getStartTask(); i<exec.getEndTask()+1; i++) {
						ExecutorDetails newExec = new ExecutorDetails(i, i);
						LOG.info("{} -> {}", i, newExec);
						schedMap.get(entry.getKey()).add(newExec);
					}
				} else {
					schedMap.get(entry.getKey()).add(exec);
				}
				
			}
		}
		LOG.info("schedMap: {}", schedMap);
		return schedMap;
	}

	@Override
	public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		// TODO Auto-generated method stub
		return null;
	}

}
