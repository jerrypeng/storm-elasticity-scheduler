package backtype.storm.scheduler.Elasticity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.generated.TopologySummary;

public class GetTopologyInfo {
	
	private static GetTopologyInfo instance = null;
	private HashMap<String, Component> bolts = null;
	private HashMap<String, Component> spouts = null;
	private HashMap<String, Component> all_comp = null;
	private static final Logger LOG = LoggerFactory.getLogger(GetTopologyInfo.class);
	
	public class Component {
		public List<Component> parents = null;
		public List<Component> children = null;
		public Component() {
			this.parents = new ArrayList<Component>();
			this.children = new ArrayList<Component>();
		}
	}
	
	protected GetTopologyInfo () {
		
	}
	
	public static GetTopologyInfo getInstance() {
		if(instance == null) {
			instance = new GetTopologyInfo();
		}
		return instance;
	}
		
	public void getTopologyInfo() {
		LOG.info("Getting Topology info...");
		
		TSocket tsocket = new TSocket("localhost", 6627);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		Nimbus.Client client = new Nimbus.Client(tBinaryProtocol);
		
		try {
			tTransport.open();
			
			ClusterSummary clusterSummary = client.getClusterInfo();
			List<TopologySummary> topologies = clusterSummary.get_topologies();
			LOG.info("number of topologies: {}", topologies.size());
			for (TopologySummary topo : topologies) {
				StormTopology storm_topo = client.getTopology(topo.get_id());
				//spouts
				LOG.info("!!-SPOUTs-!!");
				for (Map.Entry<String, SpoutSpec> s : storm_topo.get_spouts().entrySet()) {
					LOG.info("Id: {}", s.getKey());
					LOG.info("INPUTS");
					for(Map.Entry<GlobalStreamId,Grouping> entry : s.getValue().get_common().get_inputs().entrySet()) {
						/*
						LOG.info("component id: {} stream id: {}, fields, {}", new Object[] {entry.getKey().get_componentId()
						,entry.getKey().get_streamId() ,entry.getValue().get_fields()});
						*/
						LOG.info("component id: {} stream id: {}", new Object[] {entry.getKey().get_componentId()
								,entry.getKey().get_streamId()});
						LOG.info("grouping: {}", entry.getValue());
					}
					LOG.info("OUTPUTS: ");
					for (Map.Entry<String, StreamInfo> entry : s.getValue().get_common().get_streams().entrySet()) {
						LOG.info("{}--{}", entry.getKey(), entry.getValue().get_output_fields());
					}
				}
				//bolt
				LOG.info("!!-BOLTs-!!");
				for (Map.Entry<String, Bolt> s : storm_topo.get_bolts().entrySet()) {
					LOG.info("Id: {}", s.getKey());
					LOG.info("INPUTS");
					for(Map.Entry<GlobalStreamId,Grouping> entry : s.getValue().get_common().get_inputs().entrySet()) {
						/*
						LOG.info("component id: {} stream id: {}, fields, {}", new Object[] {entry.getKey().get_componentId()
						,entry.getKey().get_streamId() ,entry.getValue().get_fields()});
						*/
						LOG.info("component id: {} stream id: {}", new Object[] {entry.getKey().get_componentId()
								,entry.getKey().get_streamId()});
						LOG.info("grouping: {}", entry.getValue());
					}
					LOG.info("OUTPUTS: ");
					for (Map.Entry<String, StreamInfo> entry : s.getValue().get_common().get_streams().entrySet()) {
						LOG.info("{}--{}", entry.getKey(), entry.getValue().get_output_fields());
					}
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
