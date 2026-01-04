package simulator;

import scheduler.IScheduler;
import scheduler.MesosScheduler;
import scheduler.MesosAllocator;
import simulator.core.CellState;
import simulator.core.Workload;

import java.util.*;

/**
 * Mesos simulator that extends ClusterSimulator.
 * Manages Mesos schedulers and the Mesos allocator.
 */
public class MesosSimulator extends ClusterSimulator {
    private final MesosAllocator allocator;
    
    public MesosSimulator(CellState cellState,
                         Map<String, MesosScheduler> schedulers,
                         Map<String, List<String>> workloadToSchedulerMap,
                         List<Workload> workloads,
                         List<Workload> prefillWorkloads,
                         MesosAllocator allocator,
                         boolean logging) {
        super(cellState,
              convertSchedulers(schedulers),
              workloadToSchedulerMap,
              workloads,
              prefillWorkloads,
              logging);
        
        if (!cellState.getConflictMode().equals("resource-fit")) {
            throw new IllegalArgumentException(
                "Mesos requires cellstate to be set up with resource-fit conflictMode");
        }
        
        this.allocator = allocator;
        allocator.setSimulator(this);
        
        log("========================================================");
        log(String.format(
            "Mesos SIM CONSTRUCTOR - CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s).",
            cellState.getTotalOccupiedCpus(),
            cellState.getTotalOccupiedCpus() / cellState.getTotalCpus() * 100.0,
            "%",
            cellState.getTotalOccupiedMem(),
            cellState.getTotalOccupiedMem() / cellState.getTotalMem() * 100.0,
            "%"));
        
        // Set up pointer to this simulator in each scheduler
        for (MesosScheduler scheduler : schedulers.values()) {
            scheduler.setMesosSimulator(this);
        }
    }
    
    private static Map<String, IScheduler> convertSchedulers(
            Map<String, MesosScheduler> mesosSchedulers) {
        Map<String, IScheduler> result = new HashMap<>();
        for (Map.Entry<String, MesosScheduler> entry : mesosSchedulers.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
    
    public MesosAllocator getAllocator() {
        return allocator;
    }
}

