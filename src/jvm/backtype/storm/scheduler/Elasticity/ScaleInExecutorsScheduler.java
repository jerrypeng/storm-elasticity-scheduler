package backtype.storm.scheduler.Elasticity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.Elasticity.MsgServer.MsgServer;
import backtype.storm.scheduler.Elasticity.Strategies.ScaleInETPStrategy;
import backtype.storm.scheduler.Elasticity.Strategies.ScaleInExecutorStrategy;
import backtype.storm.scheduler.Elasticity.Strategies.ScaleInProximityBased;
import backtype.storm.scheduler.Elasticity.Strategies.ScaleInTestStrategy;
import backtype.storm.scheduler.Elasticity.Strategies.StellaInStrategy;
import backtype.storm.scheduler.Elasticity.Strategies.UnevenScheduler;
import backtype.storm.scheduler.Elasticity.Strategies.UnevenScheduler2;

public class ScaleInExecutorsScheduler implements IScheduler{
	private static final Logger LOG = LoggerFactory
			.getLogger(ScaleInExecutorsScheduler.class);
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		
		LOG.info("\n\n\nRerunning ScaleInExecutorsScheduler...");

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
			if (signal == MsgServer.Signal.ScaleIn || (globalState.rebalancingState == MsgServer.Signal.ScaleIn && status.equals("REBALANCING"))) {
				
				ScaleInExecutorStrategy strategy = new ScaleInExecutorStrategy(globalState, stats, topo, cluster, topologies, null);
				ArrayList<String> hosts = new ArrayList<String>();
				hosts.add("pc402.emulab.net");
                hosts.add("pc408.emulab.net");
               
				if(signal == MsgServer.Signal.ScaleIn ) {
					LOG.info("/*** Scaling In ***/");
				
					StellaInStrategy si = new StellaInStrategy(globalState, stats, topo, cluster, topologies);
					TreeMap<Node, Integer> rankMap = si.StrategyScaleInAll();
	
					
					
					strategy.removeNodesByHostname(hosts);
					globalState.rebalancingState = MsgServer.Signal.ScaleIn;
				} else if((globalState.rebalancingState == MsgServer.Signal.ScaleIn && status.equals("REBALANCING"))) {
					LOG.info("After Rebalancing...");
					LOG.info("Unassigned Executors for {}: ", topo.getName());
					
					strategy.getNewScheduling();
					
					
					
					globalState.rebalancingState =null;
				}
				
			} else {
				LOG.info("ID: {} NAME: {}", topo.getId(), topo.getName());
				LOG.info("Unassigned Executors for {}: ", topo.getName());

				for (Map.Entry<ExecutorDetails, String> k : cluster
						.getNeedsSchedulingExecutorToComponents(topo)
						.entrySet()) {
					LOG.info("{} -> {}", k.getKey(), k.getValue());
				}

				
				//new backtype.storm.scheduler.EvenScheduler().schedule(
				//		topologies, cluster);
				if(cluster.getUnassignedExecutors(topo).size()<topo.getExecutors().size()) {
					LOG.info("running EvenScheduler now...");
					new backtype.storm.scheduler.EvenScheduler().schedule(
							topologies, cluster);
				} else {
					LOG.info("running UnEvenScheduler now...");
					UnevenScheduler2 ns = new UnevenScheduler2(globalState, stats, cluster, topologies);
					ns.schedule();
				}

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