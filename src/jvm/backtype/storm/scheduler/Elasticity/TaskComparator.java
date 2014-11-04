package backtype.storm.scheduler.Elasticity;

import java.util.Comparator;
import java.util.HashMap;

import backtype.storm.scheduler.ExecutorDetails;

public class TaskComparator implements Comparator<ExecutorDetails> {

	HashMap<ExecutorDetails, Integer> base;
    public TaskComparator(HashMap<ExecutorDetails, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(ExecutorDetails a, ExecutorDetails b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}