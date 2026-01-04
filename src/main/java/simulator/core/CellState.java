package simulator.core;

import scheduler.IScheduler;
import simulator.ClusterSimulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the state of a cluster cell, tracking resource allocation
 * across all machines and schedulers.
 */
public class CellState {
    private final int numMachines;
    private final double cpusPerMachine;
    private final double memPerMachine;
    private final String conflictMode;
    private final String transactionMode;
    
    private ClusterSimulator simulator;
    
    // Per-machine resource tracking
    private final double[] allocatedCpusPerMachine;
    private final double[] allocatedMemPerMachine;
    private final int[] machineSeqNums;
    
    // Per-scheduler resource tracking
    private final Map<String, Double> occupiedCpus = new HashMap<>();
    private final Map<String, Double> occupiedMem = new HashMap<>();
    private final Map<String, Double> lockedCpus = new HashMap<>();
    private final Map<String, Double> lockedMem = new HashMap<>();
    
    // Aggregated totals
    private double totalOccupiedCpus = 0.0;
    private double totalOccupiedMem = 0.0;
    private double totalLockedCpus = 0.0;
    private double totalLockedMem = 0.0;
    
    public CellState(int numMachines, double cpusPerMachine, double memPerMachine,
                    String conflictMode, String transactionMode) {
        if (!conflictMode.equals("resource-fit") && !conflictMode.equals("sequence-numbers")) {
            throw new IllegalArgumentException(
                "conflictMode must be one of: {'resource-fit', 'sequence-numbers'}, " +
                "but it was " + conflictMode);
        }
        if (!transactionMode.equals("all-or-nothing") && !transactionMode.equals("incremental")) {
            throw new IllegalArgumentException(
                "transactionMode must be one of: {'all-or-nothing', 'incremental'}, " +
                "but it was " + transactionMode);
        }
        
        this.numMachines = numMachines;
        this.cpusPerMachine = cpusPerMachine;
        this.memPerMachine = memPerMachine;
        this.conflictMode = conflictMode;
        this.transactionMode = transactionMode;
        this.allocatedCpusPerMachine = new double[numMachines];
        this.allocatedMemPerMachine = new double[numMachines];
        this.machineSeqNums = new int[numMachines];
    }
    
    // Getters
    public int getNumMachines() { return numMachines; }
    public double getCpusPerMachine() { return cpusPerMachine; }
    public double getMemPerMachine() { return memPerMachine; }
    public String getConflictMode() { return conflictMode; }
    public String getTransactionMode() { return transactionMode; }
    public ClusterSimulator getSimulator() { return simulator; }
    public void setSimulator(ClusterSimulator simulator) { this.simulator = simulator; }
    
    public double getTotalCpus() {
        return numMachines * cpusPerMachine;
    }
    
    public double getTotalMem() {
        return numMachines * memPerMachine;
    }
    
    public double getAvailableCpus() {
        return getTotalCpus() - (totalOccupiedCpus + totalLockedCpus);
    }
    
    public double getAvailableMem() {
        return getTotalMem() - (totalOccupiedMem + totalLockedMem);
    }
    
    public double getTotalOccupiedCpus() { return totalOccupiedCpus; }
    public double getTotalOccupiedMem() { return totalOccupiedMem; }
    public double getTotalLockedCpus() { return totalLockedCpus; }
    public double getTotalLockedMem() { return totalLockedMem; }
    
    public Map<String, Double> getOccupiedCpus() { return occupiedCpus; }
    public Map<String, Double> getOccupiedMem() { return occupiedMem; }
    
    public int getMachineSeqNum(int machineID) {
        return machineSeqNums[machineID];
    }
    
    public void incrementMachineSeqNum(int machineID) {
        machineSeqNums[machineID]++;
    }
    
    /**
     * Get available CPUs on a specific machine.
     */
    public double availableCpusPerMachine(int machineID) {
        if (machineID > allocatedCpusPerMachine.length - 1) {
            throw new IllegalArgumentException("There is no machine with ID " + machineID);
        }
        return cpusPerMachine - allocatedCpusPerMachine[machineID];
    }
    
    /**
     * Get available memory on a specific machine.
     */
    public double availableMemPerMachine(int machineID) {
        if (machineID > allocatedMemPerMachine.length - 1) {
            throw new IllegalArgumentException("There is no machine with ID " + machineID);
        }
        return memPerMachine - allocatedMemPerMachine[machineID];
    }
    
    /**
     * Allocate resources on a machine to a scheduler.
     * @param locked Mark these resources as being pessimistically locked
     *               (i.e. while they are offered as part of a Mesos resource-offer).
     */
    public void assignResources(IScheduler scheduler, int machineID, double cpus, 
                               double mem, boolean locked) {
        String schedulerName = scheduler.getName();
        
        if (locked) {
            lockedCpus.put(schedulerName, 
                lockedCpus.getOrDefault(schedulerName, 0.0) + cpus);
            lockedMem.put(schedulerName, 
                lockedMem.getOrDefault(schedulerName, 0.0) + mem);
            totalLockedCpus += cpus;
            totalLockedMem += mem;
        } else {
            occupiedCpus.put(schedulerName, 
                occupiedCpus.getOrDefault(schedulerName, 0.0) + cpus);
            occupiedMem.put(schedulerName, 
                occupiedMem.getOrDefault(schedulerName, 0.0) + mem);
            totalOccupiedCpus += cpus;
            totalOccupiedMem += mem;
        }
        
        // Validate machine has enough resources
        if (availableCpusPerMachine(machineID) < cpus) {
            throw new IllegalStateException(String.format(
                "Scheduler %s tried to claim %f cpus on machine %d, " +
                "but it only has %f unallocated cpus right now.",
                schedulerName, cpus, machineID, availableCpusPerMachine(machineID)));
        }
        if (availableMemPerMachine(machineID) < mem) {
            throw new IllegalStateException(String.format(
                "Scheduler %s tried to claim %f mem on machine %d, " +
                "but it only has %f mem unallocated right now.",
                schedulerName, mem, machineID, availableMemPerMachine(machineID)));
        }
        
        allocatedCpusPerMachine[machineID] += cpus;
        allocatedMemPerMachine[machineID] += mem;
    }
    
    /**
     * Release the specified number of resources used by a scheduler.
     */
    public void freeResources(IScheduler scheduler, int machineID, double cpus,
                             double mem, boolean locked) {
        String schedulerName = scheduler.getName();
        
        if (locked) {
            if (!lockedCpus.containsKey(schedulerName)) {
                throw new IllegalStateException(
                    schedulerName + " tried to free locked resources but has none.");
            }
            double currentCpus = lockedCpus.get(schedulerName);
            double currentMem = lockedMem.get(schedulerName);
            if (currentCpus < cpus - 0.001 || currentMem < mem - 0.001) {
                throw new IllegalStateException(String.format(
                    "%s tried to free %f cpus, %f mem, but was only locking %f cpus, %f mem.",
                    schedulerName, cpus, mem, currentCpus, currentMem));
            }
            lockedCpus.put(schedulerName, currentCpus - cpus);
            lockedMem.put(schedulerName, currentMem - mem);
            totalLockedCpus -= cpus;
            totalLockedMem -= mem;
        } else {
            if (!occupiedCpus.containsKey(schedulerName)) {
                throw new IllegalStateException(
                    schedulerName + " tried to free resources but has none.");
            }
            double currentCpus = occupiedCpus.get(schedulerName);
            double currentMem = occupiedMem.get(schedulerName);
            if (currentCpus < cpus - 0.001 || currentMem < mem - 0.001) {
                throw new IllegalStateException(String.format(
                    "%s tried to free %f cpus, %f mem, but was only occupying %f cpus, %f mem.",
                    schedulerName, cpus, mem, currentCpus, currentMem));
            }
            occupiedCpus.put(schedulerName, currentCpus - cpus);
            occupiedMem.put(schedulerName, currentMem - mem);
            totalOccupiedCpus -= cpus;
            totalOccupiedMem -= mem;
        }
        
        allocatedCpusPerMachine[machineID] -= cpus;
        allocatedMemPerMachine[machineID] -= mem;
    }
    
    /**
     * Return a copy of this cell state in its current state.
     */
    public CellState copy() {
        CellState newCellState = new CellState(numMachines, cpusPerMachine, memPerMachine,
                                             conflictMode, transactionMode);
        System.arraycopy(allocatedCpusPerMachine, 0, 
                        newCellState.allocatedCpusPerMachine, 0, numMachines);
        System.arraycopy(allocatedMemPerMachine, 0, 
                        newCellState.allocatedMemPerMachine, 0, numMachines);
        System.arraycopy(machineSeqNums, 0, 
                        newCellState.machineSeqNums, 0, numMachines);
        newCellState.occupiedCpus.putAll(occupiedCpus);
        newCellState.occupiedMem.putAll(occupiedMem);
        newCellState.lockedCpus.putAll(lockedCpus);
        newCellState.lockedMem.putAll(lockedMem);
        newCellState.totalOccupiedCpus = totalOccupiedCpus;
        newCellState.totalOccupiedMem = totalOccupiedMem;
        newCellState.totalLockedCpus = totalLockedCpus;
        newCellState.totalLockedMem = totalLockedMem;
        return newCellState;
    }
    
    /**
     * Result of committing a transaction.
     */
    public static class CommitResult {
        private final List<ClaimDelta> committedDeltas;
        private final List<ClaimDelta> conflictedDeltas;
        
        public CommitResult(List<ClaimDelta> committedDeltas, List<ClaimDelta> conflictedDeltas) {
            this.committedDeltas = committedDeltas;
            this.conflictedDeltas = conflictedDeltas;
        }
        
        public List<ClaimDelta> getCommittedDeltas() { return committedDeltas; }
        public List<ClaimDelta> getConflictedDeltas() { return conflictedDeltas; }
    }
    
    /**
     * Attempt to commit a list of deltas, returning any that conflicted.
     */
    public CommitResult commit(List<ClaimDelta> deltas, boolean scheduleEndEvent) {
        boolean rollback = false;
        List<ClaimDelta> appliedDeltas = new ArrayList<>();
        List<ClaimDelta> conflictDeltas = new ArrayList<>();
        
        // Commit non-conflicting deltas
        for (ClaimDelta d : deltas) {
            if (causesConflict(d)) {
                if (simulator != null) {
                    simulator.log(String.format(
                        "delta (%s mach-%d seqNum-%d) caused a conflict.",
                        d.getScheduler().getName(), d.getMachineID(), d.getMachineSeqNum()));
                }
                conflictDeltas.add(d);
                if (transactionMode.equals("all-or-nothing")) {
                    rollback = true;
                    break;
                } else if (transactionMode.equals("incremental")) {
                    // Continue with other deltas
                } else {
                    throw new IllegalStateException("Invalid transactionMode: " + transactionMode);
                }
            } else {
                d.apply(this, false);
                appliedDeltas.add(d);
            }
        }
        
        // Rollback if necessary
        if (rollback) {
            if (simulator != null) {
                simulator.log("Rolling back " + appliedDeltas.size() + " deltas.");
            }
            for (ClaimDelta d : appliedDeltas) {
                d.unApply(this, false);
                conflictDeltas.add(d);
            }
            appliedDeltas.clear();
        }
        
        if (scheduleEndEvent) {
            scheduleEndEvents(appliedDeltas);
        }
        
        return new CommitResult(appliedDeltas, conflictDeltas);
    }
    
    /**
     * Create an end event for each delta provided.
     * The end event will free the resources used by the task.
     */
    public void scheduleEndEvents(List<ClaimDelta> claimDeltas) {
        if (simulator == null) {
            throw new IllegalStateException("Simulator must be non-null in CellState.");
        }
            for (ClaimDelta appliedDelta : claimDeltas) {
                final ClaimDelta finalDelta = appliedDelta;
                simulator.afterDelay(appliedDelta.getDuration(), () -> {
                    finalDelta.unApply(simulator.getCellState(), false);
                    simulator.log(String.format(
                        "A task started by scheduler %s finished. " +
                        "Freeing %f cpus, %f mem. Available: %f cpus, %f mem.",
                        finalDelta.getScheduler().getName(),
                        finalDelta.getCpus(),
                        finalDelta.getMem(),
                        simulator.getCellState().getAvailableCpus(),
                        simulator.getCellState().getAvailableMem()));
                });
            }
    }
    
    /**
     * Tests if this delta causes a transaction conflict.
     * Different test scheme is used depending on conflictMode.
     */
    private boolean causesConflict(ClaimDelta delta) {
        if (conflictMode.equals("sequence-numbers")) {
            // Use machine sequence numbers to test for conflicts.
            if (delta.getMachineSeqNum() != machineSeqNums[delta.getMachineID()]) {
                if (simulator != null) {
                    simulator.log(String.format(
                        "Sequence-number conflict occurred " +
                        "(sched-%s, mach-%d, seq-num-%d, cpus-%f, mem-%f).",
                        delta.getScheduler().getName(),
                        delta.getMachineID(),
                        delta.getMachineSeqNum(),
                        delta.getCpus(),
                        delta.getMem()));
                }
                return true;
            }
            return false;
        } else if (conflictMode.equals("resource-fit")) {
            // Check if the machine is currently short of resources,
            // regardless of whether sequence nums have changed.
            if (availableCpusPerMachine(delta.getMachineID()) < delta.getCpus() ||
                availableMemPerMachine(delta.getMachineID()) < delta.getMem()) {
                if (simulator != null) {
                    simulator.log(String.format(
                        "Resource-aware conflict occurred " +
                        "(sched-%s, mach-%d, cpus-%f, mem-%f).",
                        delta.getScheduler().getName(),
                        delta.getMachineID(),
                        delta.getCpus(),
                        delta.getMem()));
                }
                return true;
            }
            return false;
        } else {
            throw new IllegalStateException("Unrecognized conflictMode: " + conflictMode);
        }
    }
}

