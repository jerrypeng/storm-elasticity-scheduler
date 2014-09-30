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
		this.nodes = new HashMap<String, Node>();
		for(Map.Entry<String, SupervisorDetails> sup : cluster.getSupervisors().entrySet()) {
			
				Node newNode = new Node(sup.getKey(), cluster);
				nodes.put(sup.getKey(), newNode);
		}
		
		for (Map.Entry<String, SchedulerAssignment> entry : cluster.getAssignments().entrySet()) {
			for(Map.Entry<ExecutorDetails,WorkerSlot> exec : entry.getValue().getExecutorToSlot().entrySet()){
				if(nodes.containsKey(exec.getValue().getNodeId()) == true) {
					if(nodes.get(exec.getValue().getNodeId()).slot_to_exec.containsKey(exec.getValue()) == true) {
						nodes.get(exec.getValue().getNodeId()).slot_to_exec.get(exec.getValue()).add(exec.getKey());
						nodes.get(exec.getValue().getNodeId()).execs.add(exec.getKey());
					} else {
						LOG.info("ERROR: should have node {} should have worker: {}", exec.getValue().getNodeId(), exec.getValue());
						return;
					}
				} else {
					LOG.info("ERROR: should have node {}", exec.getValue().getNodeId());
					return;
				}
				/*
				if(newNode.slot_to_exec.containsKey(exec.getValue()) == false) {
					newNode.slot_to_exec.put(exec.getValue(), new ArrayList<ExecutorDetails>());
				}
				newNode.slot_to_exec.get(exec.getValue()).add(exec.getKey());
				newNode.execs.add(exec.getKey());
				*/
			}
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
