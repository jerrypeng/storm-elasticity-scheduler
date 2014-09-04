package backtype.storm.scheduler.Elasticity;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
//import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.TopologyDetails;


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
		LOG.info("\n\n\nRerunning scheduling...");

		GetStats gs = new GetStats();
		gs.getStatistics();

		for (TopologyDetails topo : topologies.getTopologies()) {
			LOG.info("Unassigned Executors for {}: ", topo.getName());
			for (Map.Entry<ExecutorDetails, String> k : cluster.getNeedsSchedulingExecutorToComponents(topo).entrySet()) {
				LOG.info("{} -> {}", k.getKey(), k.getValue());
			}
		}
		LOG.info("Current Assignment: ");
		for (Map.Entry<String, SchedulerAssignment> k : cluster.getAssignments().entrySet()) {
			LOG.info("{} -> {}", k.getKey(), k.getValue().getExecutors());
		}

		//new EvenScheduler().schedule(topologies, cluster);
	}
}
