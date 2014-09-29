package backtype.storm.scheduler.Elasticity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class StoreState {
	private static StoreState instance = null;
	public boolean balanced = false;
	public Map<String, Node> nodes;
	
	public Map<ExecutorDetails, WorkerSlot> execToWorkers;
	
	private static final Logger LOG = LoggerFactory.getLogger(StoreState.class);
	
	
	protected StoreState(Cluster cluster, Topologies topologies) {
		nodes = new HashMap<String, Node>();
	}
	
	public static StoreState getInstance(Cluster cluster, Topologies topologies) {
		if(instance==null) {
			instance = new StoreState(cluster, topologies);
		}
		return instance;
	}
	
	public void storeState(Cluster cluster, Topologies topologies) {
		LOG.info("Storing State...");
		this.updateNodes(cluster, topologies);
		for(TopologyDetails topo : topologies.getTopologies()) {
			if(cluster.getAssignmentById(topo.getId())!=null) {
				execToWorkers = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
				LOG.info("execToWorker: {}", execToWorkers);
			}
			
		}
		
	}
	
	public void updateNodes(Cluster cluster, Topologies topologies) {
		for(Map.Entry<String, SupervisorDetails> sup : cluster.getSupervisors().entrySet()) {
			
				Node newNode = new Node(sup.getKey(), cluster);
				
				for (Map.Entry<String, SchedulerAssignment> entry : cluster.getAssignments().entrySet()) {
					for(Map.Entry<ExecutorDetails,WorkerSlot> exec : entry.getValue().getExecutorToSlot().entrySet()){
						newNode.slot_to_exec.get(exec.getValue()).add(exec.getKey());
						newNode.execs.add(exec.getKey());
					}
				}
				nodes.put(sup.getKey(), newNode);
			
		}
		
		for(TopologyDetails topo : topologies.getTopologies()) {
			
		}
	}
	
	public List<Node> getEmptyNode() {
		List<Node> retVal = new ArrayList<Node>();
		for (Map.Entry<String, Node> n : this.nodes.entrySet()) {
			if(n.getValue().execs.size()==0) {
				retVal.add(n.getValue());
			}
		}
		return retVal;
	}
}
