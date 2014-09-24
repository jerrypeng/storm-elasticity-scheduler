package backtype.storm.scheduler.Elasticity;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class StoreState {
	private static StoreState instance = null;
	private static final Logger LOG = LoggerFactory.getLogger(StoreState.class);
	
	public Map<ExecutorDetails, WorkerSlot> execToWorkers;
	Map<String, Map<WorkerSlot, Collection<ExecutorDetails>>> schedMap;
	
	protected StoreState(Cluster cluster, Topologies topologies) {
		//cluster.getAssignmentById(topologyId)
	}
	
	public static StoreState getInstance(Cluster cluster, Topologies topologies) {
		if(instance==null) {
			instance = new StoreState(cluster, topologies);
		}
		return instance;
	}
	
	public void storeState(Cluster cluster, Topologies topologies) {
		LOG.info("Storing State...");
		for(TopologyDetails topo : topologies.getTopologies()) {
			execToWorkers = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
			LOG.info("execToWorker: {}", execToWorkers);
		}
		
	}
}
