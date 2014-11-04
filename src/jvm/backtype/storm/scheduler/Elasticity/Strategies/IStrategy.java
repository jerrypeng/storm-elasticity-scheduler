package backtype.storm.scheduler.Elasticity.Strategies;

import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;

public interface IStrategy {
	
	public Map<WorkerSlot, List<ExecutorDetails>> getNewScheduling();
		
	
}
