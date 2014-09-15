package backtype.storm.scheduler.Elasticity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;

public class HelperFuncs {
	static HashMap<String, ArrayList<ExecutorDetails>> nodeToTask(Cluster cluster, String topoId) {
		HashMap<String, ArrayList<ExecutorDetails>> retMap = new HashMap<String, ArrayList<ExecutorDetails>>();
		
		for(Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
			String nodeId = entry.getValue().getNodeId();
			if(retMap.containsKey(nodeId)==false) {
				retMap.put(nodeId, new ArrayList<ExecutorDetails>());
			}
			retMap.get(nodeId).add(entry.getKey());
		}
		
	}

}
