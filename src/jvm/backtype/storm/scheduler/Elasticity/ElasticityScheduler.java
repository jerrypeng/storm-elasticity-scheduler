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
		 * Get Global info
		 */
		GlobalState globalState = GlobalState.getInstance();
		globalState.updateInfo(cluster, topologies);

		LOG.info("Global State:\n{}", globalState);

		/**
		 * Get stats
		 */
		 GetStats stats = GetStats.getInstance("ElasticityScheduler");
		 stats.getStatistics();

		/**
		 * Start hardware monitoring server
		 */
		Master server = Master.getInstance();

		/**
		 * Start Scheduling
		 */
		for (TopologyDetails topo : topologies.getTopologies()) {
			String status = HelperFuncs.getStatus(topo.getId());
			LOG.info("status: {}", status);
			if (status.equals("REBALANCING")) {
				if (globalState.isBalanced == false) {
					LOG.info("Rebalancing...{}=={}", cluster.getUnassignedExecutors(topo).size(), topo
							.getExecutors().size());
					if (cluster.getUnassignedExecutors(topo).size() == topo
							.getExecutors().size()) {
						LOG.info("Making migration assignments...");
						
						TreeMap<Component, Integer> priorityQueue = Strategies.numDescendantStrategy(globalState.components.get(topo.getId()));
						Strategies.centralityStrategy(globalState.components.get(topo.getId()));
						
						LOG.info("priorityQueue: {}", priorityQueue);
						
						List<Node> newNodes = globalState.getNewNode();
						
						if(newNodes.size()<=0) {
							LOG.error("No new Nodes!");
							break;
						}
						
						Map<WorkerSlot, List<ExecutorDetails>> schedMap = globalState.schedState.get(topo.getId());
						
						Node targetNode = newNodes.get(0);
						WorkerSlot target_ws = targetNode.slots.get(0);
						LOG.info("target location: {}:{}", targetNode.hostname, target_ws.getPort());
						
						int THRESHOLD = (topo.getExecutors().size())/cluster.getSupervisors().size();
						LOG.info("Threshold: {}", THRESHOLD);
						List<ExecutorDetails> migratedTasks = new ArrayList<ExecutorDetails>();
						for (Component comp : priorityQueue.keySet()) {
							if(migratedTasks.size()>=THRESHOLD) {
								break;
							}
							for(ExecutorDetails exec : comp.execs) {
								if(migratedTasks.size()>=THRESHOLD) {
									break;
								}
								globalState.migrateTask(exec, target_ws, topo);
								migratedTasks.add(exec);
							}
						}
						
						LOG.info("Tasks migrated: {}", migratedTasks);
						for(Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap.entrySet()) {
							//cluster.assign(sched.getKey(), topo.getId(), sched.getValue());
							HelperFuncs.assignTasks(sched.getKey(), topo.getId(), sched.getValue(), cluster, topologies);
							LOG.info("Assigning {}=>{}",sched.getKey(), sched.getValue());
						}

						globalState.isBalanced = true;
					}
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

				globalState.storeState(cluster, topologies);
				globalState.isBalanced = false;
			}

			LOG.info("Current Assignment: {}",
					HelperFuncs.nodeToTask(cluster, topo.getId()));
		}
		if(topologies.getTopologies().size()==0){
			globalState.clearStoreState();
		}
		

	}
}
