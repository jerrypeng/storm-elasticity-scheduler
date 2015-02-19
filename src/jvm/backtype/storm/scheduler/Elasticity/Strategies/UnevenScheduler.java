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
			Integer distribution = unassigned.size()
					/ this._globalState.nodes.size();
			ArrayList<Node> nodes = new ArrayList<Node>(
					this._globalState.nodes.values());
			Map<WorkerSlot, ArrayList<ExecutorDetails>> schedMap = new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>();
			int i = 0;
			int j = 0;
			while (true) {
				if (j >= unassigned.size()) {
					break;
				}
				if (i >= nodes.size()) {
					i = 0;
				}

				WorkerSlot ws = this.findBestSlot2(nodes.get(i));
				if (schedMap.containsKey(ws) == false) {
					schedMap.put(ws, new ArrayList<ExecutorDetails>());
				}
				schedMap.get(ws).add(unassigned.get(j));
				j++;
				if (j % distribution == 0) {
					j++;
				}
			}
			LOG.info("SchedMap: {}", schedMap);
			if (schedMap != null) {
				for (Entry<WorkerSlot, ArrayList<ExecutorDetails>> sched : schedMap
						.entrySet()) {
					this._cluster.assign(sched.getKey(),
							topo.getId(), sched.getValue());
					LOG.info("Assigning {}=>{}",
							sched.getKey(), sched.getValue());
				}
			}

		}
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
