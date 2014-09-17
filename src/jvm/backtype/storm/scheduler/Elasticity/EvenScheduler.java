package backtype.storm.scheduler.Elasticity;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

public class EvenScheduler {
	private static final Logger LOG = LoggerFactory
			.getLogger(EvenScheduler.class);
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		LOG.info("\n\n\nRerunning scheduling...");
		for (TopologyDetails topo : topologies.getTopologies()) {
			LOG.info("ID: {} NAME: {}", topo.getId(), topo.getName());
			LOG.info("Unassigned Executors for {}: ", topo.getName());
			LOG.info("Current Assignment: {}", HelperFuncs.nodeToTask(cluster, topo.getId()));
		}
		GetStats gs = GetStats.getInstance();
		GetTopologyInfo gt = new GetTopologyInfo();
		gs.getStatistics("EvenScheduler");
		gt.getTopologyInfo();
		LOG.info("Topology layout: {}", gt.all_comp);
		
		String comp = Strategies.centralityStrategy(gt.all_comp);
		LOG.info("Best Comp: {}", comp);

		LOG.info("running EvenScheduler now...");
		new EvenScheduler().schedule(topologies, cluster);
	}
}
