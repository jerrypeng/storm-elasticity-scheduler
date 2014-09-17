package backtype.storm.scheduler.Elasticity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.fest.util.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;

public class GetStats {
	private static GetStats instance = null;
	private static final Logger LOG = LoggerFactory.getLogger(GetStats.class);
	public HashMap<String, Integer> statsTable;
	public HashMap<String, Long> startTimes;
	public HashMap<String, Integer> node_stats;
	public HashMap<String, HashMap<String, ArrayList<ExecutorSummary>>> location_stats;
	public HashMap<String, Integer> indv_component_stats;
	public HashMap<String, HashMap<String, Integer>> node_component_stats;
	public HashMap<String, Integer> parallelism_hint;
	private File complete_log;
	private File avg_log;
	
	private static String LOG_PATH="/tmp/";

	protected GetStats(String filename) {
		statsTable = new HashMap<String, Integer>();
		startTimes = new HashMap<String, Long>();
		node_stats = new HashMap<String, Integer>();
		location_stats = new HashMap<String, HashMap<String, ArrayList<ExecutorSummary>>>();
		indv_component_stats = new HashMap<String, Integer>();
		node_component_stats = new HashMap<String, HashMap<String, Integer>>();
		parallelism_hint = new HashMap<String, Integer> ();
		
		//delete old files
		try {
			complete_log = new File(LOG_PATH+filename+"_complete");
			avg_log = new File(LOG_PATH+filename+"_complete");
			
			complete_log.delete();
			avg_log.delete();
		}catch(Exception e){
			 
    		e.printStackTrace();
 
    	}

	}

	public static GetStats getInstance(String filename) {
		if (instance == null) {
			instance = new GetStats(filename);
		}
		return instance;
	}

	public void getStatistics() {
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
				StormTopology stormTopo = null;
				try {
					topologyInfo = client.getTopologyInfo(topo.get_id());
					stormTopo = client.getTopology(topo.get_id());
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
					if(this.location_stats.containsKey(host) == false) {
						this.location_stats.put(host, new HashMap<String, ArrayList<ExecutorSummary>>());
						this.location_stats.get(host).put("bolts", new ArrayList<ExecutorSummary>());
						this.location_stats.get(host).put("spouts", new ArrayList<ExecutorSummary>());
						this.node_component_stats.put(host, new HashMap<String, Integer>());
						this.node_component_stats.get(host).put("bolts", 0);
						this.node_component_stats.get(host).put("spouts", 0);
					}

					if (stormTopo.get_bolts().containsKey(componentId) == true){
						this.location_stats.get(host).get("bolts").add(executorSummary);
						this.parallelism_hint.put(componentId, stormTopo.get_bolts().get(componentId).get_common().get_parallelism_hint());
					} else if (stormTopo.get_spouts().containsKey(componentId) == true) {
						this.location_stats.get(host).get("spouts").add(executorSummary);
						this.parallelism_hint.put(componentId, stormTopo.get_spouts().get(componentId).get_common().get_parallelism_hint());
					} else {
						LOG.info("ERROR: type of component not determined!");
					}
					
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
						LOG.info("-->transfered: {}\n -->emmitted: {}", executorStats.get_transferred(), executorStats.get_emitted());

						
						this.statsTable.put(hash_id, totalOutput);

						// get node stats
						if (this.node_stats.containsKey(host) == false) {
							this.node_stats.put(host, 0);
						}
						this.node_stats.put(host, this.node_stats.get(host)
								+ throughput);

						//get node component stats
						if (stormTopo.get_bolts().containsKey(componentId) == true){
							this.node_component_stats.get(host).put("bolts", this.node_component_stats.get(host).get("bolts")+throughput);
						} else if (stormTopo.get_spouts().containsKey(componentId) == true) {
							this.node_component_stats.get(host).put("spouts", this.node_component_stats.get(host).get("spouts")+throughput);
						}
						
						//get individual component stats
						if(this.indv_component_stats.containsKey(componentId)==false) {
							this.indv_component_stats.put(componentId, 0);
						}
						this.indv_component_stats.put(componentId, this.indv_component_stats.get(componentId) + throughput);
						
						
						
						//write to file
						long unixTime = (System.currentTimeMillis() / 1000)
								- this.startTimes.get(topo.get_id());
						String data = String.valueOf(unixTime) + ':' + host
								+ ':' + port + ':' + componentId + ":"
								+ topo.get_id() + ":" + taskId + ","
								+ throughput + "\n";

						try {
							// LOG.info("writting to file...");
							
							FileWriter fileWritter = new FileWriter(this.complete_log, true);
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
				LOG.info("!!!- GENERAL STATISTICS -!!!");
				LOG.info("OVERALL THROUGHPUT: {}", this.node_stats);
				this.node_stats.clear();
				LOG.info("NODE STATS:");
				for(Map.Entry<String, HashMap<String, ArrayList<ExecutorSummary>>> entry : this.location_stats.entrySet()) {
					LOG.info("{}:", entry.getKey());
					LOG.info("# of Spouts: {}    # of Bolts: {}", entry.getValue().get("spouts").size(), entry.getValue().get("bolts").size());
					LOG.info("total Spout throughput: {}    total Bolt throughput: {}", this.node_component_stats.get(entry.getKey()).get("spouts"), this.node_component_stats.get(entry.getKey()).get("bolts"));
					LOG.info("Spouts: {}\nBolts: {}",  entry.getValue().get("spouts"), entry.getValue().get("bolts"));
				}
				LOG.info("COMPONENT STATS:");
				for(Map.Entry<String, Integer> entry: this.indv_component_stats.entrySet()) {
					LOG.info("Component: {} avg throughput: {}", entry.getKey(), entry.getValue() / this.parallelism_hint.get(entry.getKey()));
					
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		}
	}

}
