package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.Component;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.Node;

public class MostLinkLoad extends LinkLoadBasedStrategy{

	public MostLinkLoad(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		super(globalState, getStats, topo, cluster, topologies);
		// TODO Auto-generated constructor stub
	}
	
	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling() {
		Map<String, Component> components = this._globalState.components.get(this._topo.getId());
		List<Node> newNodes = this._globalState.getNewNode();
		
		if(newNodes.size()<=0) {
			LOG.error("No new Nodes!");
			return null;
		}
		
		Node targetNode = newNodes.get(0);
		WorkerSlot target_ws = targetNode.slots.get(0);
		LOG.info("target location: {}:{}", targetNode.hostname, target_ws.getPort());
		
		int THRESHOLD = this.thresholdFunction();
		LOG.info("Threshold: {}", THRESHOLD);
		List<ExecutorDetails> migratedTasks = new ArrayList<ExecutorDetails>();
		
		for (Component comp : this.ComponentThroughputRank.keySet()) {
			if(migratedTasks.size() >= THRESHOLD) {
				break;
			}
			List<ExecutorDetails> compTasks = new ArrayList<ExecutorDetails>();
			compTasks.addAll(comp.execs);
			List<ExecutorDetails> childrenTasks = this.getChildrenTasks(comp);
			List<ExecutorDetails> parentTasks = this.getParentTasks(comp);
			
			LOG.info("comp: {}", comp.id);
			LOG.info("comTasks: {}", compTasks);
			LOG.info("childrenTasks: {}", childrenTasks);
			
			Iterator<ExecutorDetails> compTasksItr = compTasks.iterator();
			Iterator<ExecutorDetails> childrenTasksItr = childrenTasks.iterator();
			Iterator<ExecutorDetails> parentTaskItr = parentTasks.iterator();
			while(migratedTasks.size()<THRESHOLD && (compTasksItr.hasNext() || childrenTasksItr.hasNext() || parentTaskItr.hasNext())){
				if(compTasksItr.hasNext()){
					ExecutorDetails exec = compTasksItr.next();
					if(migratedTasks.contains(exec) == false) {
						this._globalState.migrateTask(exec, target_ws, this._topo);
						migratedTasks.add(exec);
					}
				}
				
				if(migratedTasks.size()>= THRESHOLD) {
					break;
				}
				
				if(childrenTasksItr.hasNext()) {
					ExecutorDetails exec = childrenTasksItr.next();
					if(migratedTasks.contains(exec) == false) {
						this._globalState.migrateTask(exec, target_ws, this._topo);
						migratedTasks.add(exec);
					}
				}
				
				if(migratedTasks.size()>= THRESHOLD) {
					break;
				}
				
				if (parentTaskItr.hasNext()) {
					ExecutorDetails exec = parentTaskItr.next();
					if(migratedTasks.contains(exec) == false) {
						this._globalState.migrateTask(exec, target_ws, this._topo);
						migratedTasks.add(exec);
					}
				}
			}
			
		}
		LOG.info("Tasks migrated: {}", migratedTasks);
		Map<WorkerSlot, List<ExecutorDetails>> schedMap = this._globalState.schedState.get(this._topo.getId());
		return schedMap;
	}

}
