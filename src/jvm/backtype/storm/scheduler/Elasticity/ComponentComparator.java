package backtype.storm.scheduler.Elasticity;

import java.util.Comparator;
import java.util.HashMap;

public class ComponentComparator implements Comparator<Component> {

	HashMap<Component, Integer> base;
    public ComponentComparator(HashMap<Component, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(Component a, Component b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}