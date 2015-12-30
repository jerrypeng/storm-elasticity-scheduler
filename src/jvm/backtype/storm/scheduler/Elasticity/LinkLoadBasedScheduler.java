package backtype.storm.scheduler.Elasticity;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Elasticity.MsgServer.MsgServer;
import backtype.storm.scheduler.Elasticity.Strategies.LeastLinkLoad;
import backtype.storm.scheduler.Elasticity.Strategies.StellaOutStrategy;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LinkLoadBasedScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory
            .getLogger(LinkLoadBasedScheduler.class);
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
            if (signal == MsgServer.Signal.ScaleOut) {
                if (globalState.stateEmpty() == false) {
                    List<Node> newNodes = globalState.getNewNode();

                    if (newNodes.size() > 0) {

                        LOG.info("link load based scheduling...");
                        LeastLinkLoad leastLinkLoad = new LeastLinkLoad(globalState, stats,
                                topo, cluster, topologies);
                        Map<WorkerSlot, List<ExecutorDetails>> schedMap = leastLinkLoad.getNewScheduling2();
                        if (schedMap != null) {
                            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> sched : schedMap
                                    .entrySet()) {
                                cluster.assign(sched.getKey(),
                                        topo.getId(), sched.getValue());
                                LOG.info("Assigning {}=>{}",
                                        sched.getKey(), sched.getValue());
                            }
                        }
                    }
                }
            }
        }
    }

}