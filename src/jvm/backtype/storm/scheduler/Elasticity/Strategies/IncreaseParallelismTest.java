package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.List;
import java.util.Map;
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
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this._globalState.schedState.get(this._topo.getId());
		return schedMap;
	}

	@Override
	public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		// TODO Auto-generated method stub
		return null;
	}

}
