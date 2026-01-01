package scheduler;

import simulator.core.CellState;
import simulator.core.ClaimDelta;
import simulator.MesosSimulator;

import java.util.*;

/**
 * Mesos allocator that manages resource offers to schedulers.
 * Uses DRF (Dominant Resource Fairness) to determine which scheduler
 * should receive the next offer.
 */
public class MesosAllocator {
    private MesosSimulator simulator;
    private boolean allocating = false;
    private final Set<MesosScheduler> schedulersRequestingResources = new HashSet<>();
    private double timeSpentAllocating = 0.0;
    private long nextOfferId = 0;
    private final Map<Long, List<ClaimDelta>> offeredDeltas = new HashMap<>();
    private boolean buildAndSendOfferScheduled = false;
    
    private final double constantThinkTime;
    private final double minCpuOffer;
    private final double minMemOffer;
    private final double offerBatchInterval;
    
    public MesosAllocator(double constantThinkTime,
                          double minCpuOffer,
                          double minMemOffer,
                          double offerBatchInterval) {
        this.constantThinkTime = constantThinkTime;
        this.minCpuOffer = minCpuOffer;
        this.minMemOffer = minMemOffer;
        this.offerBatchInterval = offerBatchInterval;
    }
    
    public MesosAllocator(double constantThinkTime) {
        this(constantThinkTime, 100.0, 100.0, 1.0);
    }
    
    public void setSimulator(MesosSimulator simulator) {
        this.simulator = simulator;
    }
    
    public MesosSimulator getSimulator() {
        return simulator;
    }
    
    private void checkRegistered() {
        if (simulator == null) {
            throw new IllegalStateException(
                "You must assign a simulator to a MesosAllocator before you can use it.");
        }
    }
    
    public double getThinkTime() {
        return constantThinkTime;
    }
    
    /**
     * Request an offer from a scheduler that needs resources.
     */
    public void requestOffer(MesosScheduler needySched) {
        checkRegistered();
        simulator.log("Received an offerRequest from " + needySched.getName() + ".");
        schedulersRequestingResources.add(needySched);
        schedBuildAndSendOffer();
    }
    
    /**
     * Cancel an outstanding offer request.
     */
    public void cancelOfferRequest(MesosScheduler needySched) {
        simulator.log("Canceling the outstanding resourceRequest for scheduler " +
                     needySched.getName() + ".");
        schedulersRequestingResources.remove(needySched);
    }
    
    /**
     * Schedule building and sending an offer with batching.
     */
    private void schedBuildAndSendOffer() {
        if (!buildAndSendOfferScheduled) {
            buildAndSendOfferScheduled = true;
            simulator.afterDelay(offerBatchInterval, () -> {
                simulator.log("Building and sending a batched offer");
                buildAndSendOffer();
                buildAndSendOfferScheduled = false;
            });
        }
    }
    
    /**
     * Build and send a resource offer to a scheduler using DRF.
     */
    public void buildAndSendOffer() {
        checkRegistered();
        simulator.log("========================================================");
        simulator.log(String.format(
            "TOP OF BUILD AND SEND. CellState total occupied: " +
            "%fcpus (%.1f%%), %fmem (%.1f%%).",
            simulator.getCellState().getTotalOccupiedCpus(),
            simulator.getCellState().getTotalOccupiedCpus() / 
                simulator.getCellState().getTotalCpus() * 100.0,
            simulator.getCellState().getTotalOccupiedMem(),
            simulator.getCellState().getTotalOccupiedMem() / 
                simulator.getCellState().getTotalMem() * 100.0));
        
        // Build and send offer only if there are enough resources and schedulers want offers
        if (!schedulersRequestingResources.isEmpty() &&
            simulator.getCellState().getAvailableCpus() >= minCpuOffer &&
            simulator.getCellState().getAvailableMem() >= minMemOffer) {
            
            // Use DRF to pick a candidate scheduler
            List<MesosScheduler> sortedSchedulers = 
                drfSortSchedulers(new ArrayList<>(schedulersRequestingResources));
            
            if (!sortedSchedulers.isEmpty()) {
                MesosScheduler candidateSched = sortedSchedulers.get(0);
                
                // Create an offer by taking a snapshot of cell state
                CellState privCellState = simulator.getCellState().copy();
                Offer offer = new Offer(nextOfferId, candidateSched, privCellState);
                nextOfferId++;
                
                // Lock resources in common cell state
                List<ClaimDelta> claimDeltas = 
                    candidateSched.scheduleAllAvailable(simulator.getCellState(), true);
                
                if (!claimDeltas.isEmpty()) {
                    offeredDeltas.put(offer.getId(), claimDeltas);
                    
                    double thinkTime = getThinkTime();
                    final Offer finalOffer = offer;
                    simulator.afterDelay(thinkTime, () -> {
                        timeSpentAllocating += thinkTime;
                        simulator.log(String.format(
                            "Allocator done thinking, sending offer to %s. " +
                            "Offer contains private cell state with %f cpu, %f mem available.",
                            candidateSched.getName(),
                            finalOffer.getCellState().getAvailableCpus(),
                            finalOffer.getCellState().getAvailableMem()));
                        candidateSched.resourceOffer(finalOffer);
                    });
                }
            }
        } else {
            String reason = "";
            if (schedulersRequestingResources.isEmpty()) {
                reason = "No schedulers currently want offers.";
            }
            if (simulator.getCellState().getAvailableCpus() < minCpuOffer ||
                simulator.getCellState().getAvailableMem() < minMemOffer) {
                reason = String.format(
                    "Only %f cpus and %f mem available in common cell state " +
                    "but min offer size is %f cpus and %f mem.",
                    simulator.getCellState().getAvailableCpus(),
                    simulator.getCellState().getAvailableMem(),
                    minCpuOffer, minMemOffer);
            }
            simulator.log("Not sending an offer after all. " + reason);
        }
    }
    
    /**
     * Handle a scheduler's response to a resource offer.
     */
    public void respondToOffer(Offer offer, List<ClaimDelta> claimDeltas) {
        checkRegistered();
        simulator.log(String.format(
            "------Scheduler %s responded to offer %d with %d claimDeltas.",
            offer.getScheduler().getName(), offer.getId(), claimDeltas.size()));
        
        // Unapply saved deltas to unlock resources
        if (offeredDeltas.containsKey(offer.getId())) {
            List<ClaimDelta> savedDeltas = offeredDeltas.remove(offer.getId());
            if (savedDeltas != null) {
                for (ClaimDelta delta : savedDeltas) {
                    delta.unApply(simulator.getCellState(), true);
                }
            }
        }
        
        simulator.log("========================================================");
        simulator.log("AFTER UNAPPLYING SAVED DELTAS");
        simulator.log(String.format(
            "CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s).",
            simulator.getCellState().getTotalOccupiedCpus(),
            simulator.getCellState().getTotalOccupiedCpus() / 
                simulator.getCellState().getTotalCpus() * 100.0,
            "%",
            simulator.getCellState().getTotalOccupiedMem(),
            simulator.getCellState().getTotalOccupiedMem() / 
                simulator.getCellState().getTotalMem() * 100.0,
            "%"));
        
        simulator.log("Committing all " + claimDeltas.size() + 
                     " deltas that were part of response " + offer.getId());
        
        if (!claimDeltas.isEmpty()) {
            CellState.CommitResult commitResult = 
                simulator.getCellState().commit(claimDeltas, false);
            
            if (!commitResult.getConflictedDeltas().isEmpty()) {
                throw new IllegalStateException(
                    "Expecting no conflicts, but there were " + 
                    commitResult.getConflictedDeltas().size());
            }
            
            // Create end events for all tasks committed
            for (ClaimDelta delta : commitResult.getCommittedDeltas()) {
                final ClaimDelta finalDelta = delta;
                simulator.afterDelay(delta.getDuration(), () -> {
                    finalDelta.unApply(simulator.getCellState(), false);
                    simulator.log(String.format(
                        "A task started by scheduler %s finished. " +
                        "Freeing %f cpus, %f mem. Available: %f cpus, %f mem. " +
                        "Also, triggering a new batched offer round.",
                        finalDelta.getScheduler().getName(),
                        finalDelta.getCpus(), finalDelta.getMem(),
                        simulator.getCellState().getAvailableCpus(),
                        simulator.getCellState().getAvailableMem()));
                    schedBuildAndSendOffer();
                });
            }
        }
        
        schedBuildAndSendOffer();
    }
    
    /**
     * Sort schedulers using DRF (Dominant Resource Fairness).
     * Returns schedulers sorted by their dominant share (ascending).
     */
    private List<MesosScheduler> drfSortSchedulers(List<MesosScheduler> schedulers) {
        List<Map.Entry<MesosScheduler, Double>> schedulerDominantShares = new ArrayList<>();
        
        for (MesosScheduler scheduler : schedulers) {
            double shareOfCpus = simulator.getCellState().getOccupiedCpus()
                .getOrDefault(scheduler.getName(), 0.0);
            double shareOfMem = simulator.getCellState().getOccupiedMem()
                .getOrDefault(scheduler.getName(), 0.0);
            
            double domShare = Math.max(
                shareOfCpus / simulator.getCellState().getTotalCpus(),
                shareOfMem / simulator.getCellState().getTotalMem());
            
            String nameOfDomShare = shareOfCpus > shareOfMem ? "cpus" : "mem";
            simulator.log(String.format(
                "%s's dominant share is %s (%f%s).",
                scheduler.getName(), nameOfDomShare, domShare, "%"));
            
            schedulerDominantShares.add(new AbstractMap.SimpleEntry<>(scheduler, domShare));
        }
        
        // Sort by dominant share (ascending)
        schedulerDominantShares.sort(Comparator.comparing(Map.Entry::getValue));
        
        List<MesosScheduler> result = new ArrayList<>();
        for (Map.Entry<MesosScheduler, Double> entry : schedulerDominantShares) {
            result.add(entry.getKey());
        }
        return result;
    }
}
