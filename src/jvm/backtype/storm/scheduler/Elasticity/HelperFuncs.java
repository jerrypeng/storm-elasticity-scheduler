package backtype.storm.scheduler.Elasticity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class HelperFuncs {
	private static final Logger LOG = LoggerFactory
			.getLogger(HelperFuncs.class);

	static void assignTasks(WorkerSlot slot, String topologyId,
			Collection<ExecutorDetails> executors, Cluster cluster,
			Topologies topologies) {
		LOG.info("Assigning using HelperFuncs Assign...");
		WorkerSlot curr_slot = null;
		Collection<ExecutorDetails> curr_executors = new ArrayList<ExecutorDetails>();

		/*
		 * for (Map.Entry<ExecutorDetails, WorkerSlot> ws :
		 * cluster.getAssignmentById(topologyId).getExecutorToSlot().entrySet())
		 * { if(ws.getValue().getNodeId().equals(slot.getNodeId())==true &&
		 * ws.getValue().getPort() == slot.getPort()) { curr_slot =
		 * ws.getValue(); } }
		 */
		for (WorkerSlot ws : cluster.getAssignableSlots()) {

			if (ws.getNodeId().equals(slot.getNodeId()) == true
					&& ws.getPort() == slot.getPort()) {
				curr_slot = ws;
			}

		}

		if (curr_slot == null) {
			LOG.error("Error: worker: {} does not exist!", slot);
			return;
		}

		for (ExecutorDetails old_exec : executors) {
			for (ExecutorDetails exec : topologies.getById(topologyId)
					.getExecutors()) {
				if (old_exec.getEndTask() == exec.getEndTask()
						&& old_exec.getStartTask() == exec.getStartTask()) {
					curr_executors.add(exec);
				}
			}
		}

		if (executors.size() != curr_executors.size()) {
			LOG.error(
					"Error: executors size: {} curr_executors: {} not the same!",
					executors.size(), curr_executors.size());
		}

		cluster.assign(curr_slot, topologyId, curr_executors);

	}

	static void unassignTask(ExecutorDetails exec,
			Map<ExecutorDetails, WorkerSlot> execToSlot) {
		if (execToSlot.containsKey(exec) == true) {
			execToSlot.remove(exec);
		}

	}

	static void unassignTasks(List<ExecutorDetails> execs,
			Map<ExecutorDetails, WorkerSlot> execToSlot) {
		for (ExecutorDetails exec : execs) {
			unassignTask(exec, execToSlot);
		}
	}

	static List<ExecutorDetails> compToExecs(TopologyDetails topo, String comp) {
		List<ExecutorDetails> execs = new ArrayList<ExecutorDetails>();
		for (Map.Entry<ExecutorDetails, String> entry : topo
				.getExecutorToComponent().entrySet()) {
			if (entry.getValue().equals(comp) == true) {
				execs.add(entry.getKey());
			}
		}
		return execs;
	}

	static HashMap<String, ArrayList<ExecutorDetails>> nodeToTask(
			Cluster cluster, String topoId) {
		HashMap<String, ArrayList<ExecutorDetails>> retMap = new HashMap<String, ArrayList<ExecutorDetails>>();
		if (cluster.getAssignmentById(topoId) != null
				&& cluster.getAssignmentById(topoId).getExecutorToSlot() != null) {
			for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster
					.getAssignmentById(topoId).getExecutorToSlot().entrySet()) {
				String nodeId = cluster.getHost(entry.getValue().getNodeId());
				if (retMap.containsKey(nodeId) == false) {

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
				if (topo.get_id().equals(topo_id) == true) {
					return topo.get_status();
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		return null;
	}

	static void migrate(TreeMap<ExecutorDetails, Integer> priorityQueue,
			TopologyDetails topo, Integer THRESHOLD, GlobalState globalState,
			WorkerSlot target_ws, Cluster cluster, Topologies topologies) {
		
		
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = globalState.schedState
				.get(topo.getId());
		List<ExecutorDetails> migratedTasks = new ArrayList<ExecutorDetails>();
		for (ExecutorDetails exec : priorityQueue.keySet()) {
			if (migratedTasks.size() >= THRESHOLD) {
				break;
			}

			globalState.migrateTask(exec, target_ws, topo);
			migratedTasks.add(exec);

		}

		LOG.info("Tasks migrated: {}", migratedTasks);
		for (Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap
				.entrySet()) {
			// cluster.assign(sched.getKey(), topo.getId(), sched.getValue());
			HelperFuncs.assignTasks(sched.getKey(), topo.getId(),
					sched.getValue(), cluster, topologies);
			LOG.info("Assigning {}=>{}", sched.getKey(), sched.getValue());
		}
	}
	
	public static String printPriorityQueue(TreeMap<ExecutorDetails, Integer> priorityQueue, TopologyDetails topo) {
		String retVal= "";
		for (ExecutorDetails exec : priorityQueue.keySet()) {
			retVal+=exec.toString()+"-->"+topo.getExecutorToComponent().get(exec)+"\n";
		}
		return retVal;
	}
	public static Double weightedMovingAverageAlg1(List<Integer> values) {
		return null;
	}
	
	public static Double computeMovAvg(List<Integer> values) {
		Double sum = 0.0;
		for (Integer val : values) {
			sum+=val;
		}
		return sum/(values.size()*1.0);
	}
	
	public static ExecutorDetails idToExecutor(Integer taskId, TopologyDetails topo) {
		for (ExecutorDetails exec : topo.getExecutors()) {
			if(exec.getStartTask() == taskId.intValue()) {
				return exec;
			}
		}
		return null;
	}
	
}
