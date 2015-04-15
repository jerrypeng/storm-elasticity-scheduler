package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.Elasticity.Component;
import backtype.storm.scheduler.Elasticity.GetStats;
import backtype.storm.scheduler.Elasticity.GlobalState;
import backtype.storm.scheduler.Elasticity.HelperFuncs;
import backtype.storm.scheduler.Elasticity.Node;
import backtype.storm.scheduler.Elasticity.Strategies.StellaInStrategy.ComponentComparatorDouble;
import backtype.storm.scheduler.Elasticity.Strategies.TopologyHeuristicStrategy.NodeComparator;

/***
 * rank percentage of effect of each node
 * @author Le
 */

public class StellaInComplexStrategy extends TopologyHeuristicStrategy {
	public class Plan{
		public Node target;
		public HashMap<ExecutorDetails,Node> PlanDetail;	
	}
	
	HashMap<Double, HashMap<ExecutorDetails,Node>> PlanCollection=new HashMap<Double, HashMap<ExecutorDetails,Node>>();
	HashMap<String, Double> ExpectedEmitRateMap = new HashMap<String, Double>();
	HashMap<String, Double> ExpectedExecuteRateMap = new HashMap<String, Double>();
	HashMap<String, Double> EmitRateMap = new HashMap<String, Double>();
	HashMap<String, Double> ExecuteRateMap = new HashMap<String, Double>();
	HashMap<String, Integer> ParallelismMap = new HashMap<String, Integer>();
	HashMap<Node, List<ExecutorDetails>> NodeExecutorMap = new HashMap<Node, List<ExecutorDetails>>();
	//HashMap<String, Double> ETPMap=new HashMap<String,Double>();
	HashMap<String, Double> SinkMap = new HashMap<String, Double>();
	HashMap<String, Double> IOMap = new HashMap<String, Double>();
	ArrayList<String> CongestedComp = new ArrayList<String>();//store a list of congested component
	ArrayList<String> sourceList=new ArrayList<String>();
	int sourceCount;
	
	int count;
	
	public StellaInComplexStrategy(GlobalState globalState, GetStats getStats,
			TopologyDetails topo, Cluster cluster, Topologies topologies) {
		super(globalState, getStats, topo, cluster, topologies);
		//count=topo.getExecutors().size()/this._cluster.getSupervisors().size();
		//LOG.info("NUMBER OF EXECUTORS WE'RE GOING TO ADD: {}", count);
		//topo.getExecutorToComponent()
	}

	
	public Plan StrategyScaleIn() {
		
		init();
		HashMap<ExecutorDetails, Node> ret=new HashMap<ExecutorDetails, Node>();

		Plan p=new Plan();
		p=StellaAlg();

		LOG.info("!--------!");
		LOG.info("FINAL MAP: {}", p.PlanDetail);
		LOG.info("!--------!");
		return p;
	}

	
	
	private void init() {
		// TODO Auto-generated method stub
		this.EmitRateMap = new HashMap<String, Double>();
		
		//initialize the final plan
		
		//emit rate map
		for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.emitThroughputHistory.entrySet()) {
			LOG.info("Topology: {}", i.getKey());
			for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				//this.EmitRateMap.put(k.getKey(), HelperFuncs.computeMovAvg(k.getValue()));
				this.EmitRateMap.put(k.getKey(), (double)this._getStats.componentStats.get(this._topo.getId()).get(k.getKey()).total_emit_throughput);
			}
		}
		LOG.info("Emit Rate: {}", EmitRateMap);
		this.ExpectedEmitRateMap.putAll(EmitRateMap);
		
		//construct a executors map for all supervisors
		this.NodeExecutorMap = new HashMap<Node, List<ExecutorDetails>>();
		
		for(Node n:this._globalState.nodes.values()){
			this.NodeExecutorMap.put(n, n.execs);
			LOG.info("adding node {} with execs {}",n,n.execs); 
		}
		
		
		//construct a map for execute for each component
		this.ExecuteRateMap = new HashMap<String, Double>();
		for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.executeThroughputHistory.entrySet()) {
			LOG.info("Topology: {}", i.getKey());
			for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				Component self=this._globalState.components.get(this._topo.getId()).get(k.getKey());
				if(self.parents.size()==0){
					this.ExecuteRateMap.put(k.getKey(), this.EmitRateMap.get(k.getKey()));
				}
				else{
					//this.ExecuteRateMap.put(k.getKey(), HelperFuncs.computeMovAvg(k.getValue()));
					this.ExecuteRateMap.put(k.getKey(), (double)this._getStats.componentStats.get(this._topo.getId()).get(k.getKey()).total_execute_throughput);
				}
				
			}
		}
		LOG.info("Execute Rate: {}", ExecuteRateMap);
		//this.ExpectedExecuteRateMap.putAll(ExecuteRateMap);
		
		//parallelism map
		this.ParallelismMap = new HashMap<String, Integer>();
		for( Map.Entry<String, HashMap<String, List<Integer>>> i : this._getStats.emitThroughputHistory.entrySet()) {
			LOG.info("Topology: {}", i.getKey());
			for(Map.Entry<String, List<Integer>> k : i.getValue().entrySet()) {
				Component self=this._globalState.components.get(this._topo.getId()).get(k.getKey());
				LOG.info("Component: {}", self.id);
				LOG.info("parallelism level: {}", self.execs.size());
				this.ParallelismMap.put(self.id, self.execs.size());
			}
		}
		
		//source list, in case we need to speed up the entire thing
		this.sourceList=new ArrayList<String>();
		for( Map.Entry<String, Double> i : EmitRateMap.entrySet()) {
			Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
			if(self.parents.size()==0){
				this.sourceList.add(self.id);
			}
		}
		this.sourceCount=0;
		
	}

	

	public class ComponentComparatorDouble implements Comparator<String> {

		HashMap<String, Double> base;
	    public ComponentComparatorDouble(HashMap<String, Double> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(String a, String b) {
	        if (base.get(a) >= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}

	
	public Plan StellaAlg() {
		// TODO Auto-generated method stub
		//construct a map for emit throughput for each component
		
		//HashMap<Component, Integer> ret=new HashMap<Component, Integer>();
		
		
		//pick the node for kill off
		//construct a map for in-out throughput for congested component
		LOG.info("construct a map for in-out throughput for congested component");
		ComponentComparatorDouble bvc1 = new ComponentComparatorDouble(this.IOMap);
		TreeMap<String, Double> IORankMap = new TreeMap<String, Double>(bvc1);
		for( Map.Entry<String, Double> i : this.EmitRateMap.entrySet()) {
			Double out=i.getValue();
			Double in=0.0;
			Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
			LOG.info("for component {}", self.id);
			if(self.parents.size()!=0){
				for(String parent: self.parents){
					in+=ExpectedEmitRateMap.get(parent);
				}
			}
			if(in>1.2*out){
				Double io=in-out;
				this.IOMap.put(i.getKey(), io);
				//this.CongestedComp.add(i.getKey());
				LOG.info("component: {} IO overflow: {}", i.getKey(), io);
			}	
		}
		//IORankMap.putAll(this.IOMap);
		LOG.info("overload map:{}", IOMap);	
		
		for( Map.Entry<String, Double> i : ExpectedEmitRateMap.entrySet()) {
			Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
			if(self.children.size()==0){
				LOG.info("sink: {} throughput: {}", i.getKey(), (i.getValue()));
				this.SinkMap.put(i.getKey(),(i.getValue()));
			}
		}
		

		//for each node, add the expected throughput
		HashMap<Node, Integer> NodeMap = new HashMap<Node, Integer>();
		NodeComparator bvc =  new NodeComparator(NodeMap);
		TreeMap<Node, Integer> RankedNodeMap = new TreeMap<Node, Integer>(bvc);
		Node topNode=null;
		Double topThroughput=0.0;
		for(Node node:this._globalState.nodes.values()){
			LOG.info("for node : {} testing residual throughput", node.hostname);
			Double throughput_remain=ResidualThroughput(node);
			if(throughput_remain>topThroughput){
				topThroughput=throughput_remain;
				topNode=node;
			}
		}
		LOG.info("FINAL NODE REMOVED: {} throughput: {}", topNode, topThroughput);
		Plan p=new Plan();
		p.PlanDetail=this.PlanCollection.get(topThroughput);
		p.target=topNode;
		return p;
		
	}



	private Double ResidualThroughput(Node node) {
		//PlanCollection
		LOG.info("Starting for node : {} testing residual throughput", node.hostname);
		HashMap<ExecutorDetails, Node> plan= new HashMap<ExecutorDetails, Node>();
		Double throughput_remain=0.0;
		
		HashMap<String, Double> expectedETPMap=new HashMap<String,Double>();
		
		//initialize expected plan: node->executor
		HashMap<Node, List<ExecutorDetails>> ExpectedNodeExecutorMap =new HashMap<Node, List<ExecutorDetails>>();
		ExpectedNodeExecutorMap.putAll(NodeExecutorMap);
		LOG.info("node to executor map : {} ", ExpectedNodeExecutorMap);
		
		//initialize component executor speed: component->speed
		HashMap<String, Double> ExpectedExecuteRateMap =new HashMap<String, Double>();
		ExpectedExecuteRateMap.putAll(ExecuteRateMap);
		LOG.info("component->speed : {} ", ExpectedExecuteRateMap);
		
		//initialize congested component list, copy over the congested Component list
		//ArrayList<String> ExpectedCongestedComp= new ArrayList<String>(this.CongestedComp);
		HashMap<String, Double> ExpectedIOMap =new HashMap<String, Double>();
		ExpectedIOMap.putAll(this.IOMap);
		LOG.info("congested component->expected throughput : {} ", ExpectedIOMap);
		
		//initialize sinkmap
		HashMap<String, Double> ExpectedSinkMap=new HashMap<String, Double>();
		ExpectedSinkMap.putAll(this.SinkMap);
		LOG.info("sink->throughput : {} ", ExpectedSinkMap);
		
		//initialize executor executing speed: executor->speed
		LOG.info("initialize executor executing speed: executor->speed");
		LOG.info("emitRatesTable: {}", this._getStats.emitRatesTable);
		LOG.info("executeRatesTable: {}", this._getStats.executeRatesTable);
		HashMap<ExecutorDetails, Double> ExecutorExecuteRateMap=new HashMap<ExecutorDetails, Double>();
		for(ExecutorDetails e: this._topo.getExecutors()){
			Integer start_id=e.getStartTask();
			String startid_str=start_id.toString();
			for(String hash_id:this._getStats.emitRatesTable.keySet()){
				String[] hashid_arr=hash_id.split(":");
				if(hashid_arr[hashid_arr.length-1].equals(startid_str)){
					if(this._globalState.components.get(this._topo.getId()).get(getCompByExec(e)).parents.size()!=0){
						/*if(this._getStats.executeRatesTable.get(hash_id)==0){
							continue;
						}*/
						if(ExecutorExecuteRateMap.get(hash_id)!=null&&this._getStats.executeRatesTable.get(hash_id)<ExecutorExecuteRateMap.get(hash_id)){
							continue;
						}
						ExecutorExecuteRateMap.put(e,(double)this._getStats.executeRatesTable.get(hash_id));
						LOG.info("executor:{} speed: {} ", e,(double)this._getStats.executeRatesTable.get(hash_id) );
					}
					else{
						/*if(this._getStats.emitRatesTable.get(hash_id)==0){
							continue;
						}*/
						if(ExecutorExecuteRateMap.get(hash_id)!=null&&this._getStats.emitRatesTable.get(hash_id)<ExecutorExecuteRateMap.get(hash_id)){
							continue;
						}
						ExecutorExecuteRateMap.put(e,(double)this._getStats.emitRatesTable.get(hash_id));
						LOG.info("emit:{} speed: {} ", e,(double)this._getStats.emitRatesTable.get(hash_id) );
					}	
				}
			}
			//Double rate=this._getStats.executeStatsTable.
		}
		HashMap<ExecutorDetails, Double> ExpectedExecutorExecuteRateMap=new HashMap<ExecutorDetails, Double>();
		ExpectedExecutorExecuteRateMap.putAll(ExecutorExecuteRateMap);
		/****************initial reconstruction**************/
		
		//reconstruct execute speed map
		
		LOG.info("NodeExecutorMap: {}", ExpectedNodeExecutorMap);
		LOG.info("ExpectedExecuteRateMap: {}",ExpectedExecuteRateMap);
		for(ExecutorDetails e:this.NodeExecutorMap.get(node)){ 
			LOG.info("Executor: {}",e); 
			String comp=getCompByExec(e);
			LOG.info("belongs to component: {}",comp);
			Double orig=ExpectedExecuteRateMap.get(comp);
			LOG.info("original: {}",orig);
			ExpectedExecuteRateMap.put(getCompByExec(e), orig-ExecutorExecuteRateMap.get(e));
			LOG.info("comp:{} new speed: {} ", getCompByExec(e),orig-ExecutorExecuteRateMap.get(e) );
			ExpectedExecutorExecuteRateMap.put(e, 0.0);
			ExpectedNodeExecutorMap.put(node, new ArrayList<ExecutorDetails>());
		}
		
		//rewind executor expectedExecutrRateMap
		UpdateExecuteRate(ExpectedExecuteRateMap,this.sourceList);
		
		//rewind congested component
		RewindIOMap(ExpectedIOMap,ExpectedExecuteRateMap);
		
		//rewind sink map
		RewindSinkMap(ExpectedSinkMap, ExpectedExecuteRateMap);
		
		//rewind ETP value
		RewindETP(expectedETPMap, ExpectedIOMap,ExpectedSinkMap, ExpectedExecuteRateMap);
		
		
		//for each executor in the node
		for(ExecutorDetails e:this.NodeExecutorMap.get(node)){
			//assume moving this executor
			LOG.info("+++for executor {}", e.getStartTask());
			Node topNode=chkZeroETP(expectedETPMap,ExpectedNodeExecutorMap,node);
			LOG.info("+++topNode {}", topNode.hostname);
			//for each executor on that node, shrink the execution speed
			for(ExecutorDetails f:this.NodeExecutorMap.get(topNode)){
				Double cur=ExpectedExecutorExecuteRateMap.get(f);
				Double delta=cur/(ExpectedNodeExecutorMap.get(topNode).size()+1);
				Double orig=ExpectedExecuteRateMap.get(getCompByExec(f));
				ExpectedExecuteRateMap.put(getCompByExec(f), orig-delta);//update in component map
				ExpectedExecutorExecuteRateMap.put(f, cur-delta);
			}
			//increase throughput for the current executor
			Double exec_orig=ExecutorExecuteRateMap.get(e);
			Double delta=exec_orig*NodeExecutorMap.get(node).size()/(ExpectedNodeExecutorMap.get(topNode).size()+1);
			Double orig=ExpectedExecuteRateMap.get(getCompByExec(e));
			LOG.info("+++oldComp {} has speed {}, new speed {}", getCompByExec(e),orig);
			LOG.info("+++newComp {} has speed {}, new speed {}", getCompByExec(e),orig-delta);
			ExpectedExecuteRateMap.put(getCompByExec(e), orig+delta);
			ExpectedExecutorExecuteRateMap.put(e, exec_orig-delta);
			ExpectedNodeExecutorMap.get(topNode).add(e);
			
			//rewind iomap
			//reconstruct execute speed map
			/*for(ExecutorDetails ed:this.NodeExecutorMap.get(node)){
				orig=ExpectedExecuteRateMap.get(getCompByExec(ed));
				ExpectedExecuteRateMap.put(getCompByExec(ed), orig-ExecutorExecuteRateMap.get(ed));
			}*/
			
			//rewind executor expectedExecuteRateMap
			UpdateExecuteRate(ExpectedExecuteRateMap,this.sourceList);
			
			//rewind congested component
			RewindIOMap(ExpectedIOMap,ExpectedExecuteRateMap);
			
			//rewind sink map
			RewindSinkMap(ExpectedSinkMap, ExpectedExecuteRateMap);
			
			//rewind ETP value
			RewindETP(expectedETPMap, ExpectedIOMap,ExpectedSinkMap, ExpectedExecuteRateMap);
			
			//throughput_remain+=AllSinkThroughput(ExpectedSinkMap);
			
			throughput_remain=AllSinkThroughput(ExpectedSinkMap);
			LOG.info("throughput remain {}", throughput_remain);
			plan.put(e, topNode);
		}
		this.PlanCollection.put(throughput_remain, plan);
		return throughput_remain;
	}


	private Double AllSinkThroughput(HashMap<String, Double> expectedSinkMap) {
		// TODO Auto-generated method stub
		Double ret=0.0;
		for(Map.Entry<String, Double> e:expectedSinkMap.entrySet()){
			ret=ret+e.getValue();
		}
		return ret;
	}


	private void RewindSinkMap(HashMap<String, Double> expectedSinkMap,
			HashMap<String, Double> expectedExecuteRateMap) {
		// TODO Auto-generated method stub
		LOG.info("==RewindSinkMap==" );
		for(Map.Entry<String, Double> e : expectedSinkMap.entrySet()){
			expectedSinkMap.put(e.getKey(), expectedExecuteRateMap.get(e.getKey()));
		}
		LOG.info("sink map: {}", expectedSinkMap);
	}


	private void RewindIOMap(HashMap<String, Double> expectedIOMap,
			HashMap<String, Double> expectedExecuteRateMap) {
		// TODO Auto-generated method stub
		LOG.info("==RewindIOMap==" );
		expectedIOMap=new HashMap<String, Double>();
		for( Map.Entry<String, Double> i : expectedExecuteRateMap.entrySet()) {
			Double out=i.getValue();
			Double in=0.0;
			Component self=this._globalState.components.get(this._topo.getId()).get(i.getKey());
			if(self.parents.size()!=0){
				for(String parent: self.parents){
					in+=expectedExecuteRateMap.get(parent);//!!!!!!!!!!!!!!!!QUESTIONABLE!!!!!!!!!!!!
				}
			}
			if(in>1.2*out){
				Double io=in-out;
				expectedIOMap.put(i.getKey(), io);
				//this.CongestedComp.add(i.getKey());
				//LOG.info("component: {} IO overflow: {}", i.getKey(), io);
			}	
		}
		//expectedIOMap.putAll(IOMap);
		LOG.info("overload map:{}", expectedIOMap);
	}


	private void RewindETP( HashMap<String, Double> expectedETPMap,HashMap<String, Double> expectedIOMap, HashMap<String, Double> expectedSinkMap,HashMap<String, Double> expectedExecuteRateMap ) {
		//compute sink total
		LOG.info("==RewindETP==" );
		Double sink_total=0.0;
		for (Map.Entry<String, Double> entry : expectedExecuteRateMap.entrySet()) {
			if(SinkMap.get(entry.getKey())!=null){
				sink_total+=expectedExecuteRateMap.get(entry.getKey());
			}
		}
		for (Map.Entry<String, Double> entry : ExpectedEmitRateMap.entrySet()) {
			Component self=this._globalState.components.get(this._topo.getId()).get(entry.getKey());
			LOG.info("start checking component {}", self.id);
			LOG.info("sink map used: {}", expectedSinkMap);
			Double score=RecursiveFind(self,expectedIOMap,expectedSinkMap)*100;
			LOG.info("comp: {} effective throughput percentage: {}", self.id, score.intValue());
			expectedETPMap.put(self.id, score/sink_total);
		}
		LOG.info("expectedETP map: {}", expectedETPMap);
	}


	private void UpdateExecuteRate(HashMap<String, Double> expectedExecuteRateMap,List<String> sourceList) {
		// for each source in the topology
		LOG.info("==Update  expectedExecuteRateMap==" );
		for(String source_id:sourceList){
			//check whether there is an overflow
			Component source=this._globalState.components.get(this._topo.getId()).get(source_id);
			Double CurrentSelfRate=expectedExecuteRateMap.get(source.id);
			Double ParentRateSum=0.0;
			if(source.parents.size()!=0){
				for(String c:source.parents){
					ParentRateSum+=expectedExecuteRateMap.get(c);
				}
				if(ParentRateSum<CurrentSelfRate){
					expectedExecuteRateMap.put(source.id, ParentRateSum);
				}
			}
			/*for(String c:source.children){
				ChildrenRateSum+=expectedExecuteRateMap.get(c);
			}
			if(ChildrenRateSum>CurrentSelfRate){
				//proportionally shrink children's execution rate
				for(String c: source.children){
					expectedExecuteRateMap.put(c,expectedExecuteRateMap.get(c)*CurrentSelfRate/ChildrenRateSum );
				}	
			}*/
			UpdateExecuteRate(expectedExecuteRateMap, source.children);
		}
		LOG.info("Execute rate map: {}", expectedExecuteRateMap);
	}

	private Double RecursiveFind(Component self,  HashMap<String, Double> ExpectediOMap, HashMap<String, Double> ExpectedSinkMap) {
		LOG.info("checking component: {}", self);
		LOG.info("children size: {}", self.children.size());
		LOG.info("sink map: {}", ExpectedSinkMap);

		if(self.children.size()==0){
			LOG.info("final value: {}", ExpectedSinkMap.get(self.id));
			return ExpectedSinkMap.get(self.id);//this branch leads to a final value with no overflowed node between
		}
		Double sum=0.0;
		for (int i=0; i<self.children.size();i++){
			LOG.info("children list: {}", self.children);
			if(ExpectediOMap.get(self.children.get(i))!=null){//if child is also overflowed, return 0 on this branch
				continue;//ignore this branch move forward
			}
			else{
				Component child=this._globalState.components.get(this._topo.getId()).get(self.children.get(i));//lookup child's component
				sum+=RecursiveFind(child,ExpectediOMap,ExpectedSinkMap);
			}	
		}
		return sum;
	}


	private Node chkZeroETP(HashMap<String, Double> ETPMap, HashMap<Node, List<ExecutorDetails>> expectedNodeExecutorMap, Node node) {
		// TODO Auto-generated method stub
		LOG.info("==check the number of ETP zero executors==" );
		Node top=null;
		int best_count=0;
		Double least_score=0.0;

		for(Node n:expectedNodeExecutorMap.keySet()){
			if(n.hostname.equals(node.hostname)){
				continue;
			}
			LOG.info("==checking node {}==", n.hostname );
			Double node_ETP=0.0;
			int zero_count=0;
			for(ExecutorDetails e:expectedNodeExecutorMap.get(n)){
				Double exec_etp=ETPMap.get(getCompByExec(e));
				if(exec_etp==0.0)
					zero_count++;
				node_ETP+=exec_etp;
			}
			if(top==null){
				top=n;
				least_score=node_ETP;
				best_count=zero_count;
			}
			if(zero_count>best_count){
				top=n;
				least_score=node_ETP;
				best_count=zero_count;
			}
			else if(zero_count==best_count){
				if(node_ETP<least_score){
					top=n;
					least_score=node_ETP;
					best_count=zero_count;
				}
				else if(node_ETP==least_score){
					int outputcount=0;
					for(ExecutorDetails e:expectedNodeExecutorMap.get(n)){
						if(this._globalState.components.get(this._topo.getId()).get(getCompByExec(e)).children.size()==0){
							outputcount++;
						}
					}
					if(outputcount==0){
						top=n;
						least_score=node_ETP;
						best_count=zero_count;
					}
				}
			}
			LOG.info("==node {} has zero count {}==", n.hostname, zero_count );
		}
		LOG.info("==best node {} with count {}==", top.hostname, best_count );
		return top;
	}


	


	public String getCompByExec(ExecutorDetails e) {
		// TODO Auto-generated method stub
		return this._topo.getExecutorToComponent().get(e);
	}


	@Override
	public TreeMap<Component, Integer> Strategy(Map<String, Component> map) {
		// TODO Auto-generated method stub
		return null;
	}
}