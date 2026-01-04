package simulator;

import scheduler.IScheduler;
import scheduler.OmegaScheduler;
import simulator.core.CellState;
import simulator.core.Workload;

import java.util.*;

/**
 * Omega simulator that extends ClusterSimulator.
 * Manages Omega schedulers that use optimistic concurrency control.
 */
public class OmegaSimulator extends ClusterSimulator {
    
    public OmegaSimulator(CellState cellState,
                         Map<String, OmegaScheduler> schedulers,
                         Map<String, List<String>> workloadToSchedulerMap,
                         List<Workload> workloads,
                         List<Workload> prefillWorkloads,
                         boolean logging) {
        super(cellState,
              convertSchedulers(schedulers),
              workloadToSchedulerMap,
              workloads,
              prefillWorkloads,
              logging);
        
        // Set up pointer to this simulator in each scheduler
        for (OmegaScheduler scheduler : schedulers.values()) {
            scheduler.setOmegaSimulator(this);
        }
    }
    
    private static Map<String, IScheduler> convertSchedulers(
            Map<String, OmegaScheduler> omegaSchedulers) {
        Map<String, IScheduler> result = new HashMap<>();
        for (Map.Entry<String, OmegaScheduler> entry : omegaSchedulers.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}

