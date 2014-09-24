package backtype.storm.scheduler.Elasticity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;

public class HelperFuncs {
	
	static HashMap<String, ArrayList<ExecutorDetails>> nodeToTask(Cluster cluster, String topoId) {
		HashMap<String, ArrayList<ExecutorDetails>> retMap = new HashMap<String, ArrayList<ExecutorDetails>>();
		if(cluster.getAssignmentById(topoId)!=null && cluster.getAssignmentById(topoId).getExecutorToSlot()!=null) {
			for(Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
				String nodeId = cluster.getHost(entry.getValue().getNodeId());
				if(retMap.containsKey(nodeId)==false) {
					
					retMap.put(nodeId, new ArrayList<ExecutorDetails>());
				}
				retMap.get(nodeId).add(entry.getKey());
			}
		}
		
		return retMap;
		
	}
	static String getStatus(String topo_id) {
		TSocket tsocket = new TSocket("localhost", 6627);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);
		try {
			tTransport.open();

			ClusterSummary clusterSummary = client.getClusterInfo();
			List<TopologySummary> topologies = clusterSummary.get_topologies();

			for (TopologySummary topo : topologies) {
				if(topo.get_id().equals(topo_id)==true) {
					return topo.get_status();
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}
}
