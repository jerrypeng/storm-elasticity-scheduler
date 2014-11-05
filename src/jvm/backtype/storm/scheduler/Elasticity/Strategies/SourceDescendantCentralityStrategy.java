package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.Component;

/***
 * hybrid A+C+D
 * @author jerry
 */
public class SourceDescendantCentralityStrategy extends TopologyHeuristicStrategy {

	public SourceDescendantCentralityStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		super(globalState, getStats, topo, cluster, topologies);
	}

	@Override
	public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			rankMap.put(entry.getValue(), numDescendants(entry.getValue(), map)-distToSpout(entry.getValue(),map)+entry.getValue().children.size()+entry.getValue().parents.size());
			LOG.info("{}--{}", entry.getKey(), rankMap.get(entry.getKey()));
		}
		retMap.putAll(rankMap);
		return retMap;
	}

}
