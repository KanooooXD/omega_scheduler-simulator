package simulator.core;

import scheduler.IScheduler;

/**
 * Represents a change (delta) to the cell state.
 * A ClaimDelta represents the allocation of resources on a specific machine
 * to a scheduler for a task.
 */
public class ClaimDelta {
    private final IScheduler scheduler;
    private final int machineID;
    private final int machineSeqNum;
    private final double duration;
    private final double cpus;
    private final double mem;
    
    public ClaimDelta(IScheduler scheduler, int machineID, int machineSeqNum,
                     double duration, double cpus, double mem) {
        this.scheduler = scheduler;
        this.machineID = machineID;
        this.machineSeqNum = machineSeqNum;
        this.duration = duration;
        this.cpus = cpus;
        this.mem = mem;
    }
    
    public IScheduler getScheduler() { return scheduler; }
    public int getMachineID() { return machineID; }
    public int getMachineSeqNum() { return machineSeqNum; }
    public double getDuration() { return duration; }
    public double getCpus() { return cpus; }
    public double getMem() { return mem; }
    
    /**
     * Apply this delta to the cell state, allocating the resources.
     * Increments the sequence number of the machine.
     */
    public void apply(CellState cellState, boolean locked) {
        cellState.assignResources(scheduler, machineID, cpus, mem, locked);
        // Mark that the machine has changed, used for testing for conflicts
        // when using optimistic concurrency.
        cellState.incrementMachineSeqNum(machineID);
    }
    
    /**
     * Unapply this delta, freeing the resources.
     */
    public void unApply(CellState cellState, boolean locked) {
        cellState.freeResources(scheduler, machineID, cpus, mem, locked);
    }
}

