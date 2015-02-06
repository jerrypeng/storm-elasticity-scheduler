package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
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
	
	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling()
	{
		return this._globalState.schedState.get(this._topo.getId());
	}
	
	public void removeNodeByHostname(String hostname) {
		for(Node n : this._globalState.nodes.values()) {
			if(n.hostname.equals(hostname)==true) {
				LOG.info("Found Hostname: {} with sup id: {}", hostname, n.supervisor_id);
				this.removeNodeBySupervisorId(n.supervisor_id);
			}
		}
	}
	
	public void removeNodeBySupervisorId(String supervisorId) {
		Node node = this._globalState.nodes.get(supervisorId);
		
		ArrayList<Node> elgibleNodes = new ArrayList<Node>();
		LOG.info("nodes elgible:");
		for (Node n: this._globalState.nodes.values()) {
			if(n.supervisor_id.equals(supervisorId)==false) {
				LOG.info("-->{}", n.hostname);
				elgibleNodes.add(n);
			}
		}
		int i = 0;
		int j = 0;
		while(true) {
			if(i>=node.execs.size()){
				break;
			} else if(j>=elgibleNodes.size()) {
				j=0;
			}
			ExecutorDetails exec = node.execs.get(i);
			
			WorkerSlot target = this.findBestSlot(elgibleNodes.get(j));
			
			this._globalState.migrateTask(exec, target, this._topo);
			
			LOG.info("migrating {} to ws {} on node {} .... i: {} j: {}", new Object[]{exec, elgibleNodes.get(j).hostname, target.getPort(), i ,j});
			
			i++;
			j++;
		}
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
}
