package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
import backtype.storm.scheduler.Elasticity.Strategies.ScaleInProximityBased.ValueComparator;

public class ScaleInETPStrategy {
	protected Logger LOG = null;
	protected GlobalState _globalState;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;
	protected TreeMap<Node, Integer> _rankedMap;
	protected ArrayList<Node> _rankedList = new ArrayList<Node>();

	public ScaleInETPStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies, TreeMap<Node, Integer> rankedMap) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		this._rankedMap = rankedMap;
		for(Entry<Node, Integer> entry : rankedMap.entrySet()) {
			this._rankedList.add(entry.getKey());
		}
		this.LOG = LoggerFactory
				.getLogger(this.getClass());
		
	}
	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling()
	{
		return this._globalState.schedState.get(this._topo.getId());
	}
	
	public void removeNodesBySupervisorId(int top) {
		ArrayList<String> supervisorIds = new ArrayList<String>();
		for(int i=0; i<this._rankedList.size(); i++) {
			if(i<=top-1) {
				supervisorIds.add(this._rankedList.get(i).supervisor_id);
			}
		}
		ArrayList<Node> removeNodes = new ArrayList<Node>();
		ArrayList<ExecutorDetails> moveExecutors = new ArrayList<ExecutorDetails>();
		ArrayList<ExecutorDetails> moveSysExecutors = new ArrayList<ExecutorDetails>();
		for(String sup : supervisorIds) {
			removeNodes.add(this._globalState.nodes.get(sup));
			for(ExecutorDetails exec : this._globalState.nodes.get(sup).execs) {
				if(this._topo.getExecutorToComponent().get(exec).matches("(__).*")==false) {
					moveExecutors.add(exec);
				} else {
					moveSysExecutors.add(exec);
				}
			}
		}
		
		ArrayList<Node> elgibleNodes = new ArrayList<Node>();
		LOG.info("nodes elgible:");
		for (Node n: this._rankedList) {
			if(removeNodes.contains(n)==false) {
				LOG.info("-->{}", n.hostname);
				elgibleNodes.add(n);
			}
		}
		
		
		//ArrayList<WorkerSlot> slots = this.findBestSlots(elgibleNodes);
		int i = 0;
		int j = 0;
		while(true) {
			if(i>=moveExecutors.size()){
				break;
			} else if(j>=elgibleNodes.size()) {
				j=0;
			}
			ExecutorDetails exec = moveExecutors.get(i);
			
			//WorkerSlot target = this.findBestSlot2(elgibleNodes.get(j));
			//WorkerSlot target = slots.get(j);
			Node targetNode = elgibleNodes.get(j);
			WorkerSlot targetSlot = this.findBestSlot(targetNode);
			
			
			
			this._globalState.migrateTask(exec, targetSlot, this._topo);
			
			LOG.info("migrating {}:{} to ws {} on node {} .... i: {} j: {}", new Object[]{this._topo.getExecutorToComponent().get(exec), exec, targetSlot.getPort(), targetNode.hostname, i ,j});
			
			i++;
			j++;
		}
		
		this.scheduleSysTasks(moveSysExecutors, elgibleNodes);
		
	}
	public void scheduleSysTasks(ArrayList<ExecutorDetails> moveSysExecutors, ArrayList<Node> elgibleNodes) {
		//roundrobin acks tasks
		int i = 0;
		int j = 0;
				while(true) {
					if(i>=moveSysExecutors.size()){
						break;
					} else if(j>=elgibleNodes.size()) {
						j=0;
					}
					ExecutorDetails exec = moveSysExecutors.get(i);
					
					//WorkerSlot target = this.findBestSlot2(elgibleNodes.get(j));
					//WorkerSlot target = slots.get(j);
					Node targetNode = elgibleNodes.get(j);
					WorkerSlot targetSlot = this.findBestSlot3(targetNode);
					
					
					
					this._globalState.migrateTask(exec, targetSlot, this._topo);
					
					LOG.info("migrating {}:{} to ws {} on node {} .... i: {} j: {}", new Object[]{this._topo.getExecutorToComponent().get(exec), exec, targetSlot.getPort(), targetNode.hostname, i ,j});
					
					i++;
					j++;
				}
	}
	
	public WorkerSlot findBestSlot3(Node node) {
		
		LOG.info("Node: "+node.hostname);
		WorkerSlot target =null;
		int least = Integer.MAX_VALUE;
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : node.slot_to_exec.entrySet()) {
			List<ExecutorDetails> execs = this._globalState.schedState.get(this._topo.getId()).get(entry.getKey());
			if(execs!=null && execs.size()>0) {
				LOG.info("-->slots: {} execs {}",entry.getKey().getPort(), execs.size());
				if(execs.size()<least) {
					
					target = entry.getKey();
					least = execs.size();
				}
			}
		}
		return target;
	}
	
	public WorkerSlot findBestSlot(Node node) {
		WorkerSlot target =null;
		int least = Integer.MAX_VALUE;
		for(Entry<WorkerSlot, List<ExecutorDetails>> entry : node.slot_to_exec.entrySet()) {
			if(entry.getValue().size()<least) {
				target = entry.getKey();
				least = entry.getValue().size();
			}
		}
		return target;
	}
	
	public ArrayList<WorkerSlot> findBestSlots(ArrayList<Node> nodes) {
		ArrayList<WorkerSlot> slots = new ArrayList<WorkerSlot>();
		for(Node n : nodes) {
			for(Entry<WorkerSlot, List<ExecutorDetails>> entry : n.slot_to_exec.entrySet()) {
				if(entry.getValue().size()>0) {
					slots.add(entry.getKey());
				}
			}
		}
		return slots;
	}
	
}
