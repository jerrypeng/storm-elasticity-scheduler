package backtype.storm.scheduler.Elasticity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

public class GetStats {
	private static GetStats instance = null;
	private static final Logger LOG = LoggerFactory.getLogger(GetStats.class);
	public HashMap<String, Integer> statsTable;
	public HashMap<String, Long> startTimes;
	public HashMap<String, Integer> node_stats;

	protected GetStats() {
		statsTable = new HashMap<String, Integer>();
		startTimes = new HashMap<String, Long>();
		node_stats = new HashMap<String, Integer>();

	}

	public static GetStats getInstance() {
		if (instance == null) {
			instance = new GetStats();
		}
		return instance;
	}

	public void getStatistics(String filename) {
		LOG.info("Getting stats...");

		TSocket tsocket = new TSocket("localhost", 6627);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);

		try {
			tTransport.open();
			ClusterSummary clusterSummary = client.getClusterInfo();
			List<TopologySummary> topologies = clusterSummary.get_topologies();
			for (TopologySummary topo : topologies) {
				if (this.startTimes.containsKey(topo.get_id()) == false) {
					this.startTimes.put(topo.get_id(),
							(System.currentTimeMillis() / 1000));
				}
				TopologyInfo topologyInfo = null;
				try {
					topologyInfo = client.getTopologyInfo(topo.get_id());
				} catch (Exception e) {
					LOG.info(e.toString());
					continue;
				}
				List<ExecutorSummary> executorSummaries = topologyInfo
						.get_executors();

				for (ExecutorSummary executorSummary : executorSummaries) {

					ExecutorStats executorStats = executorSummary.get_stats();
					if (executorStats == null) {
						continue;
					}
					String host = executorSummary.get_host();
					int port = executorSummary.get_port();
					String componentId = executorSummary.get_component_id();

					String taskId = Integer.toString(executorSummary
							.get_executor_info().get_task_start());

					Map<String, Map<String, Long>> transfer = executorStats
							.get_transferred();

					if (transfer.get(":all-time").get("default") != null) {
						String hash_id = host + ':' + port + ':' + componentId
								+ ":" + topo.get_id() + ":" + taskId;
						Integer totalOutput = transfer.get(":all-time")
								.get("default").intValue();

						if (this.statsTable.containsKey(hash_id) == false) {
							this.statsTable.put(hash_id, totalOutput);
						}

						Integer throughput = totalOutput
								- this.statsTable.get(hash_id);
						LOG.info((host + ':' + port + ':' + componentId + ":"
								+ topo.get_id() + ":" + taskId + ","
								+ transfer.get(":all-time").get("default")
								+ "," + this.statsTable.get(hash_id) + "," + throughput));

						this.statsTable.put(hash_id, totalOutput);

						// get node stats
						if (this.node_stats.containsKey(host) == false) {
							this.node_stats.put(host, 0);
						}
						this.node_stats.put(host, this.node_stats.get(host)
								+ throughput);

						long unixTime = (System.currentTimeMillis() / 1000)
								- this.startTimes.get(topo.get_id());
						String data = String.valueOf(unixTime) + ':' + host
								+ ':' + port + ':' + componentId + ":"
								+ topo.get_id() + ":" + taskId + ","
								+ throughput + "\n";

						String filePath = "/tmp/" + filename;
						try {
							// LOG.info("writting to file...");
							File file = new File(filePath);
							FileWriter fileWritter = new FileWriter(file, true);
							BufferedWriter bufferWritter = new BufferedWriter(
									fileWritter);
							bufferWritter.append(data);
							bufferWritter.close();
							fileWritter.close();
						} catch (IOException ex) {
							LOG.info("error! writin to file {}", ex);
						}
					}
				}
				LOG.info("OVERALL THROUGHPUT: {}", this.node_stats);
			}
		} catch (TException e) {
			e.printStackTrace();
		}
	}

}
