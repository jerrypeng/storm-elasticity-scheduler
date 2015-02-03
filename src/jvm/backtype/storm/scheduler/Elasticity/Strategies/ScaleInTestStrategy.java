package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.List;
import java.util.Map;
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
import backtype.storm.scheduler.Elasticity.HelperFuncs;
import backtype.storm.scheduler.Elasticity.Node;

public class ScaleInTestStrategy {
	protected Logger LOG = null;
	protected GlobalState _globalState;
	protected GetStats _getStats;
	protected Cluster _cluster;
	protected Topologies _topologies;
	protected TopologyDetails _topo;

	public ScaleInTestStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		this._globalState = globalState;
		this._getStats = getStats;
		this._cluster = cluster;
		this._topologies = topologies;
		this._topo = topo;
		this.LOG = LoggerFactory
				.getLogger(this.getClass());
		
	}
	
	public void getNewScheduling()
	{
		
	}
	
	public void removeNode(String supervisorId) {
		Node node = this._globalState.nodes.get(supervisorId);
		
		
		for(ExecutorDetails exec : node.execs) {
			String c = this._topo.getExecutorToComponent().get(exec);
			Component comp = this._globalState.components.get(this._topo.getId()).get(c);
			if(comp.execs.size()==1) {
				//migrate to another node
				WorkerSlot target =this.findTarget(supervisorId);
			
				this._globalState.migrateTask(exec, target, this._topo);
				
			} else {
				
				comp.execs.remove(exec);
				//decrease parallelism
			}
			
			
		}
	}
	
	public WorkerSlot findTarget(String supervisorId) {
		WorkerSlot target =null;
		for (Node entry : this._globalState.nodes.values()) {
			if(entry.supervisor_id!=supervisorId) {
				Integer least = Integer.MAX_VALUE;
				for (Entry<WorkerSlot, List<ExecutorDetails>> w : entry.slot_to_exec.entrySet()) {
					if(least > w.getValue().size()) {
						least = w.getValue().size();
						target = w.getKey();
					}
				}
			}
		}
		return target;
	}
}
