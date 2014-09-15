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

	//private static GetTopologyInfo instance = null;
	private HashMap<String, Component> bolts = null;
	private HashMap<String, Component> spouts = null;
	public HashMap<String, Component> all_comp = null;
	private static final Logger LOG = LoggerFactory
			.getLogger(GetTopologyInfo.class);

	public class Component {
		public List<String> parents = null;
		public List<String> children = null;

		public Component() {
			this.parents = new ArrayList<String>();
			this.children = new ArrayList<String>();
		}
		@Override public String toString() {
			String retVal = "Parents: "+this.parents.toString()+" Children: "+this.children.toString();
			return retVal;
		}
	}

	public GetTopologyInfo() {
		this.bolts = new HashMap<String, Component>();
		this.spouts = new HashMap<String, Component>();
		this.all_comp = new HashMap<String, Component>();
	}

	/*
	public static GetTopologyInfo getInstance() {
		if (instance == null) {
			instance = new GetTopologyInfo();
		}
		return instance;
	}
*/
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
				// spouts
				LOG.info("!!-SPOUTs-!!");
				for (Map.Entry<String, SpoutSpec> s : storm_topo.get_spouts()
						.entrySet()) {
					if (s.getKey().matches("(__).*") == false) {
						Component newComp = null;
						if (this.all_comp.containsKey(s.getKey())) {
							newComp = this.all_comp.get(s.getKey());
						} else {
							newComp = new Component();
							this.all_comp.put(s.getKey(), newComp);
						}
						
						LOG.info("Id: {}", s.getKey());
						LOG.info("INPUTS");
						for (Map.Entry<GlobalStreamId, Grouping> entry : s
								.getValue().get_common().get_inputs()
								.entrySet()) {

							if (entry.getKey().get_componentId()
									.matches("(__).*") == false) {
								LOG.info("component id: {} stream id: {}",
										new Object[] {
												entry.getKey()
														.get_componentId(),
												entry.getKey().get_streamId() });
								LOG.info("grouping: {}", entry.getValue());

								newComp.parents.add(entry.getKey()
										.get_componentId());
								if (this.all_comp.containsKey(entry.getKey()
										.get_componentId()) == false) {
									this.all_comp
											.put(entry.getKey()
													.get_componentId(),
													new Component());
								}
								this.all_comp.get(entry.getKey()
										.get_componentId()).children.add(s
										.getKey());
							}
						}
						/*
						LOG.info("OUTPUTS: ");
						for (Map.Entry<String, StreamInfo> entry : s.getValue()
								.get_common().get_streams().entrySet()) {
							LOG.info("{}--{}", entry.getKey(), entry.getValue()
									.get_output_fields());
							LOG.info("JSON: {}", s.getValue().get_common()
									.get_json_conf().toString());
							LOG.info("--{}", entry.getValue());
						}
						*/
					}
				}
				// bolt
				LOG.info("!!-BOLTs-!!");
				for (Map.Entry<String, Bolt> s : storm_topo.get_bolts()
						.entrySet()) {
					if (s.getKey().matches("(__).*") == false) {
						Component newComp = null;
						if (this.all_comp.containsKey(s.getKey())) {
							newComp = this.all_comp.get(s.getKey());
						} else {
							newComp = new Component();
							this.all_comp.put(s.getKey(), newComp);
						}
						LOG.info("Id: {}", s.getKey());
						LOG.info("INPUTS");
						for (Map.Entry<GlobalStreamId, Grouping> entry : s
								.getValue().get_common().get_inputs()
								.entrySet()) {
							if (entry.getKey().get_componentId()
									.matches("(__).*") == false) {
								LOG.info("component id: {} stream id: {}",
										new Object[] {
												entry.getKey()
														.get_componentId(),
												entry.getKey().get_streamId() });
								LOG.info("grouping: {}", entry.getValue());
								newComp.parents.add(entry.getKey()
										.get_componentId());
								if (this.all_comp.containsKey(entry.getKey()
										.get_componentId()) == false) {
									this.all_comp
											.put(entry.getKey()
													.get_componentId(),
													new Component());
								}
								this.all_comp.get(entry.getKey()
										.get_componentId()).children.add(s
										.getKey());
							}
						}
						/*
						LOG.info("OUTPUTS: ");
						for (Map.Entry<String, StreamInfo> entry : s.getValue()
								.get_common().get_streams().entrySet()) {
							LOG.info("{}--{}", entry.getKey(), entry.getValue()
									.get_output_fields());
							LOG.info("JSON: {}", s.getValue().get_common()
									.get_json_conf().toString());
							LOG.info("--{}", entry.getValue());
						}
						*/
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
