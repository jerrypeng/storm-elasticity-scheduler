package backtype.storm.scheduler.Elasticity;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class GlobalState {

	private static final Logger LOG = LoggerFactory
			.getLogger(GlobalState.class);
	
	private static GlobalState instance = null;
	
	/**
	 * supervisor id -> Node
	 */
	public Map<String, Node> nodes;
	
	/**
	 * topology id -> <component name - > Component>
	 */
	public Map<String, Map<String, Component>>components;
	
	/**
	 * Topology id -> <worker slot -> collection<executors>>
	 */
	public Map <String, Map<WorkerSlot, List<ExecutorDetails>>> schedState;
	
	/**
	 * Topology id -> num of workers
	 */
	public Map<String, Integer> topoWorkers = new HashMap<String, Integer>();
	
	//edge and throughput
	public TreeMap<List<Component>, Integer> edgeThroughput;
	
	public boolean isBalanced = false;
	
	private GlobalState() {
		this.schedState = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
		
	}

	public static GlobalState getInstance() {
		if(instance==null) {
			instance = new GlobalState();
		}
		return instance;
	}
	
	public void storeState(Cluster cluster, Topologies topologies) {
		this.storeSchedState(cluster, topologies);
	}
	
	public boolean stateEmpty() {
		return this.schedState.isEmpty();
	}
	
	public void storeSchedState(Cluster cluster, Topologies topologies) {
		this.schedState = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
		
		for(TopologyDetails topo : topologies.getTopologies()) {
			if(cluster.getAssignmentById(topo.getId())!=null) {
				
				Map<WorkerSlot, List<ExecutorDetails>> topoSched = new HashMap<WorkerSlot, List<ExecutorDetails>>();
				for(Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignmentById(topo.getId()).getExecutorToSlot().entrySet()) {
					if(topoSched.containsKey(entry.getValue()) == false) {
						topoSched.put(entry.getValue(), new ArrayList<ExecutorDetails>());
					}
					topoSched.get(entry.getValue()).add(entry.getKey());
				}
				
				this.schedState.put(topo.getId(), topoSched);
				
			}
			
		}
	}
	
	public void clearStoreState() {
		this.schedState = new HashMap<String, Map<WorkerSlot, List<ExecutorDetails>>>();
	}
	
	public void updateInfo(Cluster cluster, Topologies topologies) {
		this.nodes = this.getNodes(cluster);
		this.components = this.getComponents(topologies);
	}

	private  Map<String, Map<String, Component>> getComponents(Topologies topologies) {
		Map<String, Map<String, Component>> retVal = new HashMap<String, Map<String, Component>>();
		this.topoWorkers = new HashMap<String, Integer>();
		GetTopologyInfo gt = new GetTopologyInfo();
		
		for(TopologyDetails topo : topologies.getTopologies()) {
			gt.getTopologyInfo(topo.getId());
			for(Component comp : gt.all_comp.values()) {
				comp.execs = HelperFuncs.compToExecs(topo, comp.id);
			}
			retVal.put(topo.getId(), gt.all_comp);
			
			this.topoWorkers.put(topo.getId(), gt.numWorkers);
		}
		return retVal;
	}
	
	private Map<String, Node> getNodes(Cluster cluster) {
		Map<String, Node> retVal = new HashMap<String, Node>();
		for (Map.Entry<String, SupervisorDetails> sup : cluster
				.getSupervisors().entrySet()) {

			Node newNode = new Node(sup.getKey(), cluster);
			retVal.put(sup.getKey(), newNode);
		}

		for (Map.Entry<String, SchedulerAssignment> entry : cluster
				.getAssignments().entrySet()) {
			for (Map.Entry<ExecutorDetails, WorkerSlot> exec : entry.getValue()
					.getExecutorToSlot().entrySet()) {
				if (retVal.containsKey(exec.getValue().getNodeId()) == true) {
					if (retVal.get(exec.getValue().getNodeId()).slot_to_exec
							.containsKey(exec.getValue()) == true) {
						retVal.get(exec.getValue().getNodeId()).slot_to_exec
								.get(exec.getValue()).add(exec.getKey());
						retVal.get(exec.getValue().getNodeId()).execs.add(exec
								.getKey());
					} else {
						LOG.info(
								"ERROR: should have node {} should have worker: {}",
								exec.getValue().getNodeId(), exec.getValue());
						return null;
					}
				} else {
					LOG.info("ERROR: should have node {}", exec.getValue()
							.getNodeId());
					return null;
				}
			}
		}
		return retVal;
	}
	
	/**
	 * migrate exec to ws
	 * @param exec
	 * @param ws
	 */
	public void migrateTask(ExecutorDetails exec, WorkerSlot ws, TopologyDetails topo) {
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this.schedState.get(topo.getId());
		
		if(this.execExist(exec, topo) == false) {
			LOG.error("Error: exec {} does not exist!", exec);
			return;
		}
		
		if(schedMap.containsKey(ws)==false) {
			schedMap.put(ws, new ArrayList<ExecutorDetails>());
		}
		
		for(Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap.entrySet()) {
			if(sched.getValue().contains(exec) == true) {
				sched.getValue().remove(exec);
			}
		}
		
		schedMap.get(ws).add(exec);
	}
	
	public boolean execExist(ExecutorDetails exec, TopologyDetails topo) {
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this.schedState.get(topo.getId());
		for(Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap.entrySet()) {
			if(sched.getValue().contains(exec) == true) {
				return true;
			}
		}
		return false;
	}
	
	public List<Node> getNewNode () {
		
		List<Node> retVal = new ArrayList<Node>();
		retVal.addAll(this.nodes.values());
		
		for(Map.Entry<String, Map<WorkerSlot, List<ExecutorDetails>>> i : this.schedState.entrySet()) {
			for(Map.Entry<WorkerSlot, List<ExecutorDetails>> k : i.getValue().entrySet()) {
				if(k.getValue().size() > 0 ){
					for(Node n : this.nodes.values()) {
						if(n.slots.contains(k.getKey())){
							retVal.remove(n);
						}
					}
				}
			}
		}
		/*
		for (Map.Entry<String, Node> n : this.nodes.entrySet()) {
			if(n.getValue().execs.size()==0) {
				retVal.add(n.getValue());
			}
		}
		*/
		return retVal;
	}
	public String ComponentsToString() {
		String str = "";
		str+="\n!--Components--!\n";
		for(Map.Entry<String, Map<String, Component>> entry : this.components.entrySet()) {
			str+="->Topology: "+entry.getKey()+"\n";
			for (Map.Entry<String, Component> comp : entry.getValue().entrySet()) {
				str+="-->Component: "+comp.getValue().id+"=="+entry.getKey()+"\n";
				str+="--->Parents: "+comp.getValue().parents+"\n";
				str+="--->Children: "+comp.getValue().children+"\n";
				str+="--->execs: " + comp.getValue().execs+"\n";
			}
		}
		return str;
	}
	
	public String NodesToString() {
		String str = "";
		str+="\n!--Nodes--! \n";
		for (Map.Entry<String, Node> n : this.nodes.entrySet()) {
			str+="->hostname: "+n.getValue().hostname+" Supervisor Id: "+n.getValue().supervisor_id+"\n";
			str+="->Execs: "+n.getValue().execs+"\n";
			str+="->WorkerToExec: \n";
			for(Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : n.getValue().slot_to_exec.entrySet()) {
				str+="-->"+entry.getKey().getPort()+" => "+entry.getValue()+"\n";
			}	
		}
		return str;
	}
	
	public String StoredStateToString() {
		String str="";
		str+="\n!--Stored Scheduling State--!\n";
		for(Map.Entry<String, Map<WorkerSlot, List<ExecutorDetails>>> entry : this.schedState.entrySet()) {
			str+="->Topology: "+entry.getKey()+"\n";
			for(Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : entry.getValue().entrySet()) {
				str+="-->WorkerSlot: "+sched.getKey().getNodeId()+":"+sched.getKey().getNodeId()+"\n";
				str+=sched.getValue()+"\n";
			}
		}
		return str;
	}
	
	
	@Override 
	public String toString(){
		String str="";
		str+=this.NodesToString();
		str+=this.ComponentsToString();
		str+=this.StoredStateToString();
		str+="\n topWorkers: "+ this.topoWorkers+"\n";
		
		return str;
	}
	
	private boolean log_pre = false;
	private boolean log_after = false;
	private boolean log_scheduling_info = false;
	
	public void logBeforeSchedulingInfo(String filename, TopologyDetails topo){
		String LOG_PATH = "/tmp/";
		File file= new File(LOG_PATH + filename + "_SchedulingInfo");
		if(HelperFuncs.getStatus(topo.getId()).equals("ACTIVE") && log_pre==false) {
			String data = "<!---Before Rebalancing---!>\n\n";
			data+=this.NodesToString();
			
			HelperFuncs.writeToFile(file, data);
			this.log_pre=true;
		}
	}
	
	public void logAfterSchedulingInfo(String filename, TopologyDetails topo){
		String LOG_PATH = "/tmp/";
		File file= new File(LOG_PATH + filename + "_SchedulingInfo");
		if(HelperFuncs.getStatus(topo.getId()).equals("REBALANCING") && log_after==false) {
			String data = "<!--After Re-balancing--!>\n\n";
			data+=this.NodesToString();
			
			HelperFuncs.writeToFile(file, data);
			this.log_after = true;
		}
	}
	
	public void logTopologyInfo(String filename, TopologyDetails topo){
		String LOG_PATH = "/tmp/";
		File file= new File(LOG_PATH + filename + "_SchedulingInfo");
		if(log_scheduling_info==false) {
			String data = "<!---Topology Info---!>\n\n";
			data+=this.ComponentsToString();
			
			HelperFuncs.writeToFile(file, data);
		}
	}
}
