package backtype.storm.scheduler.Elasticity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.MsgServer.MsgServer;
import backtype.storm.scheduler.Elasticity.Strategies.ScaleInProximityBased;
import backtype.storm.scheduler.Elasticity.Strategies.ScaleInTestStrategy;
import backtype.storm.scheduler.Elasticity.Strategies.StellaInComplexStrategy;
import backtype.storm.scheduler.Elasticity.Strategies.StellaInComplexStrategy.Plan;
import backtype.storm.scheduler.Elasticity.Strategies.StellaInStrategy;
import backtype.storm.scheduler.Elasticity.Strategies.UnevenScheduler;

public class ScaleInScheduler implements IScheduler{
	private static final Logger LOG = LoggerFactory
			.getLogger(ScaleInScheduler.class);
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		
		LOG.info("\n\n\nRerunning TestScheduler...");

		/**
		 * Starting msg server
		 */
		MsgServer msgServer = MsgServer.start(5001);

		/**
		 * Get Global info
		 */
		GlobalState globalState = GlobalState
				.getInstance("ElasticityScheduler");
		globalState.updateInfo(cluster, topologies);

		LOG.info("Global State:\n{}", globalState);

		/**
		 * Get stats
		 */
		GetStats stats = GetStats.getInstance("ElasticityScheduler");
		stats.getStatistics();
		//LOG.info(stats.printTransferThroughputHistory());
		//LOG.info(stats.printEmitThroughputHistory());
		//LOG.info(stats.printExecuteThroughputHistory());
		/**
		 * Start hardware monitoring server
		 */
		Master server = Master.getInstance();
		
		/**
		 * Start Scheduling
		 */
		for (TopologyDetails topo : topologies.getTopologies()) {
			globalState.logTopologyInfo(topo);
			String status = HelperFuncs.getStatus(topo.getId());
			LOG.info("status: {}", status);
			MsgServer.Signal signal = msgServer.getMessage();
			if (signal == MsgServer.Signal.ScaleIn) {
				LOG.info("/*** Scaling In ***/");
				StellaInComplexStrategy si = new StellaInComplexStrategy(globalState, stats, topo, cluster, topologies);
				//Node n = si.StrategyScaleIn();
				Plan p=si.StrategyScaleIn();
				//ScaleInProximityBased strategy = new ScaleInProximityBased(globalState, stats, topo, cluster, topologies);
				ScaleInTestStrategy strategy = new ScaleInTestStrategy(globalState, stats, topo, cluster, topologies);
				//strategy.removeNodeByHostname("pc494.emulab.net");
				//remove
				strategy.removeNodeBySupervisorId(p.target.supervisor_id);
				Map<WorkerSlot, List<ExecutorDetails>> schedMap = strategy
						.getNewScheduling();
				LOG.info("SchedMap: {}", schedMap);
				if (schedMap != null) {
					cluster.freeSlots(schedMap.keySet());
					for (Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap
							.entrySet()) {
						cluster.assign(sched.getKey(),
								topo.getId(), sched.getValue());
						LOG.info("Assigning {}=>{}",
								sched.getKey(), sched.getValue());
					}
				}
				
				globalState.rebalancingState = MsgServer.Signal.ScaleIn;
			} else {
				LOG.info("ID: {} NAME: {}", topo.getId(), topo.getName());
				LOG.info("Unassigned Executors for {}: ", topo.getName());

				for (Map.Entry<ExecutorDetails, String> k : cluster
						.getNeedsSchedulingExecutorToComponents(topo)
						.entrySet()) {
					LOG.info("{} -> {}", k.getKey(), k.getValue());
				}

				LOG.info("running UnEvenScheduler now...");
				//new backtype.storm.scheduler.EvenScheduler().schedule(
				//		topologies, cluster);
				UnevenScheduler ns = new UnevenScheduler(globalState, stats, cluster, topologies);
				ns.schedule();

				globalState.storeState(cluster, topologies);
				globalState.isBalanced = false;
			}

			LOG.info("Current Assignment: {}",
					HelperFuncs.nodeToTask(cluster, topo.getId()));
		}
		if (topologies.getTopologies().size() == 0) {
			globalState.clearStoreState();
		}
	
	}
}
