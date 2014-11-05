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
 * rank nodes closest to bolt (B)
 * @author jerry
 */
public class SinkClosenessStrategy extends TopologyHeuristicStrategy {

	public SinkClosenessStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		super(globalState, getStats, topo, cluster, topologies);
	}

	@Override
	public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		HashMap<Component, Integer> rankMap = new HashMap<Component, Integer>();
		ComponentComparator bvc =  new ComponentComparator(rankMap);
		TreeMap<Component, Integer>retMap = new TreeMap<Component, Integer>(bvc);
		for(Map.Entry<String, Component> entry : map.entrySet()) {
			Integer reverse=0-distToBolt(entry.getValue(),map);
			rankMap.put(entry.getValue(), reverse);
		}
		retMap.putAll(rankMap);
		return retMap;
	}
}
