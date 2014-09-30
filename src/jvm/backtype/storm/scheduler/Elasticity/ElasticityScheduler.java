package backtype.storm.scheduler.Elasticity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.GetTopologyInfo.Component;

public class ElasticityScheduler implements IScheduler {
	private static final Logger LOG = LoggerFactory
			.getLogger(ElasticityScheduler.class);
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		LOG.info("\n\n\nRerunning ElasticityScheduler...");
		
		/**
		 * Get stats
		 */
		GetStats gs = GetStats.getInstance("ElasticityScheduler");
		gs.getStatistics();
		
		/**
		 * Get topology info
		 */
		GetTopologyInfo gt = new GetTopologyInfo();
		gt.getTopologyInfo();
		
		LOG.info("Topology layout: {}", gt.all_comp);
		TreeMap<Component, Integer> comp = Strategies
				.centralityStrategy(gt.all_comp);
		
		LOG.info("priority queue: {}", comp);
		
		/**
		 * Start hardware monitoring server
		 */
		Master server = Master.getInstance();

		/**
		 * Store state
		 */
		StoreState ss = StoreState.getInstance(cluster, topologies);
		LOG.info("Nodes: {}", ss.nodes);
			
		for (TopologyDetails topo : topologies.getTopologies()) {
			String status = HelperFuncs.getStatus(topo.getId());
			LOG.info("status: {}", status);
			if (status.equals("REBALANCING")) {
				if (ss.balanced == false) {
					LOG.info("Do nothing....");
					LOG.info("ID: {} NAME: {}", topo.getId(), topo.getName());
					LOG.info("Unassigned Executors for {}: ", topo.getName());
					for (Map.Entry<ExecutorDetails, String> k : cluster
							.getNeedsSchedulingExecutorToComponents(topo)
							.entrySet()) {
						LOG.info("{} -> {}", k.getKey(), k.getValue());
					}
					/*
					Node n = getNewNode();
					n.getWorkers;
					
					cluster.getAvailableSlots(supervisor)
					*/
					
					List<Node> newNodes = ss.getEmptyNode();
					if(newNodes.size()>0) {
						Node newNode = newNodes.get(0);
						WorkerSlot ws = newNode.slots.get(0);
						
						TreeMap<Component, Integer> priority_queue = Strategies
								.centralityStrategy(gt.all_comp);
						
						List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>();
						
						LOG.info("priority queue: {}", priority_queue);
						
						int threshold = 3;
						for (Map.Entry<Component,Integer> entry : priority_queue.entrySet()) {
							 for (ExecutorDetails exec : HelperFuncs.compToExecs(topo, entry.getKey().id)) {
								 if(threshold <= executors.size()) {
										break;
									}
								 executors.add(exec);
							 }
							 if(threshold <= executors.size()) {
									break;
							}
						}
						
						HelperFuncs.unassignTasks(executors, cluster.getAssignmentById(topo.getId()).getExecutorToSlot());
						
						cluster.assign(ws, topo.getId(), executors);
						
					}
					
					
					ss.balanced = true;
				}
					
				
			} else {
				LOG.info("ID: {} NAME: {}", topo.getId(), topo.getName());
				LOG.info("Unassigned Executors for {}: ", topo.getName());

				for (Map.Entry<ExecutorDetails, String> k : cluster
						.getNeedsSchedulingExecutorToComponents(topo)
						.entrySet()) {
					LOG.info("{} -> {}", k.getKey(), k.getValue());
				}

				LOG.info("running EvenScheduler now...");
				new backtype.storm.scheduler.EvenScheduler().schedule(
						topologies, cluster);

				
				ss.storeState(cluster, topologies);
				ss.balanced = false;
			}

			LOG.info("Current Assignment: {}",
					HelperFuncs.nodeToTask(cluster, topo.getId()));
		}

	}
}
