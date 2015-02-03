package backtype.storm.scheduler.Elasticity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.Utils;

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

	public static void unassignTask(ExecutorDetails exec,
			Map<ExecutorDetails, WorkerSlot> execToSlot) {
		if (execToSlot.containsKey(exec) == true) {
			execToSlot.remove(exec);
		}

	}

	public static void unassignTasks(List<ExecutorDetails> execs,
			Map<ExecutorDetails, WorkerSlot> execToSlot) {
		for (ExecutorDetails exec : execs) {
			unassignTask(exec, execToSlot);
		}
	}

	public static List<ExecutorDetails> compToExecs(TopologyDetails topo, String comp) {
		List<ExecutorDetails> execs = new ArrayList<ExecutorDetails>();
		for (Map.Entry<ExecutorDetails, String> entry : topo
				.getExecutorToComponent().entrySet()) {
			if (entry.getValue().equals(comp) == true) {
				execs.add(entry.getKey());
			}
		}
		return execs;
	}

	public static HashMap<String, ArrayList<ExecutorDetails>> nodeToTask(
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

	public static String getStatus(String topo_id) {
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

	public static void migrate(TreeMap<ExecutorDetails, Integer> priorityQueue,
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
	
	public static String printRank(TreeMap<Component, Double> rankMap) {
		String retVal="";
		int rank=1;
		for(Map.Entry<Component, Double> entry : rankMap.entrySet()) {
			retVal+=rank+". Component: "+entry.getKey().id+ " value: " + entry.getValue();
			rank++;
		}
		return retVal;
	}
	
	public static void writeToFile(File file, String data) {
		try {
			FileWriter fileWritter = new FileWriter(file, true);
			BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
			bufferWritter.append(data);
			bufferWritter.close();
			fileWritter.close();
		} catch (IOException ex) {
			LOG.info("error! writin to file {}", ex);
		}
	}
	
	public static void decreaseParallelism(Map<Component, Integer> compMap, TopologyDetails topo) {
		String cmd = "/var/storm/storm_0/bin/storm rebalance -w 0 "+topo.getName();
		for(Entry<Component, Integer> entry : compMap.entrySet()) {
			Integer parallelism_hint = entry.getKey().execs.size() - entry.getValue();
			String component_id = entry.getKey().id;
			LOG.info("Increasing parallelism to {} of component {} in topo {}", new Object[]{parallelism_hint, component_id, topo.getName()});
			cmd+=" -e "+component_id+"="+parallelism_hint;
		}

		Process p;
		try {
			LOG.info("cmd: {}", cmd);
			p = Runtime.getRuntime().exec(cmd);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public static void changeParallelism2(Map<Component, Integer> compMap, TopologyDetails topo) {
		String cmd = "/var/storm/storm_0/bin/storm rebalance -w 0 "+topo.getName();
		for(Entry<Component, Integer> entry : compMap.entrySet()) {
			Integer parallelism_hint = entry.getKey().execs.size() + entry.getValue();
			String component_id = entry.getKey().id;
			LOG.info("Increasing parallelism to {} of component {} in topo {}", new Object[]{parallelism_hint, component_id, topo.getName()});
			cmd+=" -e "+component_id+"="+parallelism_hint;
		}

		//StringBuffer output = new StringBuffer();

		Process p;
		try {
			LOG.info("cmd: {}", cmd);
			p = Runtime.getRuntime().exec(cmd);
			//p.waitFor();
			//BufferedReader reader = new BufferedReader(new InputStreamReader(
			//		p.getInputStream()));

			//String line = "";
//			while ((line = reader.readLine()) != null) {
//				output.append(line + "\n");
//			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		//LOG.info(output.toString());
	}
	public static void changeParallelism(TopologyDetails topo, String component_id, Integer parallelism_hint) {
		LOG.info("Increasing parallelism to {} of component {} in topo {}", new Object[]{parallelism_hint, component_id, topo.getName()});
		TSocket tsocket = new TSocket("localhost", 6627);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);
		try {
			tTransport.open();
			LOG.info("Activating: {}", topo.getId());
			//client.activate(topo.getId());
			//client.activate(topo.getName());
		    ///Utils.sleep(30000);

			//client.getTopology(topo_id).get_bolts().get(component_id).get_common().set_parallelism_hint(parallelism_hint);
			
			LOG.info("Parallelsim_hint: {}", client.getTopology(topo.getId()).get_bolts().get(component_id).get_common().get_parallelism_hint());
			
			//client.getTopologyInfo(topo_id).set_status("ACTIVE");
			
		
			//client.getTopologyInfo(topo_id).set_executors(executors);
			RebalanceOptions options = new RebalanceOptions();
			Map<String, Integer> num_executors = new HashMap<String, Integer>();
			//num_executors.put(component_id, parallelism_hint);
			//num_executors.put("word", 10);
			//num_executors.put("exclaim", 3);
			//options.set_num_executors(num_executors);
			options.put_to_num_executors(component_id, parallelism_hint);
			options.set_wait_secs(0);
			client.rebalance(topo.getName(), options);
			LOG.info("rebalance done!");
			//client.
			
			//LOG.info("Parallelsim_hint: {}", client.getTopology(topo_id).get_bolts().get(component_id).get_common().get_parallelism_hint());
			
		} catch (TException e) {
			e.printStackTrace();
			LOG.info(e.toString());
		} catch (NotAliveException e) {
			e.printStackTrace();
			LOG.info(e.toString());
			LOG.info(e.get_msg());
			LOG.info(e.getMessage());
			LOG.info(e.getStackTrace().toString());
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
			LOG.info(e.toString());
		}
		
		try {
			LOG.info("Parallelsim_hint: {}", client.getTopology(topo.getId()).get_bolts().get(component_id).get_common().get_parallelism_hint());
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static Map<String, Integer> taskRange(List<ExecutorDetails> execs) {
		Map<String, Integer> retMap = new HashMap<String, Integer>();
		retMap.put("start", null);
		retMap.put("end", null);
		for (ExecutorDetails exec : execs) {
			if(retMap.get("start") == null) {
				retMap.put("start", exec.getStartTask());
			}
			if(retMap.get("end") == null) {
				retMap.put("end", exec.getEndTask());
			}
			
			if(retMap.get("start") > exec.getStartTask()) {
				retMap.put("start", exec.getStartTask());
			}
			if(retMap.get("end") < exec.getEndTask()) {
				retMap.put("end", exec.getEndTask());
			}
		}
		return retMap;
	}
}
