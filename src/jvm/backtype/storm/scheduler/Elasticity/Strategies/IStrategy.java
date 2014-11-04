package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.List;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;

public abstract class IStrategy {
	private GlobalState _globalState;
	private GetStats _getStats;
	private Cluster _cluster;
	private Topologies _topologies;
	
	public IStrategy(GlobalState globalState, GetStats getStats, Cluster cluster, Topologies topologies) {
		this._globalState=globalState;
	}
	public Map <WorkerSlot, List<ExecutorDetails>> getNewScheduling(GlobalState globalState, GetStats getStats, Cluster cluster, Topologies topologies);
}
