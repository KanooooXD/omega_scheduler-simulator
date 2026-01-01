package scheduler;

import simulator.core.Job;
import simulator.core.ClaimDelta;
import simulator.core.CellState;
import simulator.OmegaSimulator;

import java.util.*;

/**
 * Omega scheduler implementation.
 * Uses optimistic concurrency control with a private cell state copy.
 */
public class OmegaScheduler extends BaseScheduler {
    private OmegaSimulator omegaSimulator;
    private CellState privateCellState;
    private final Map<Integer, Integer> dailySuccessTransactions = new HashMap<>();
    private final Map<Integer, Integer> dailyFailedTransactions = new HashMap<>();
    
    public OmegaScheduler(String name,
                         Map<String, Double> constantThinkTimes,
                         Map<String, Double> perTaskThinkTimes,
                         int numMachinesToBlackList) {
        super(name, constantThinkTimes, perTaskThinkTimes, numMachinesToBlackList);
        
        System.out.println(String.format("scheduler-id-info: %d, %s, %d, %s, %s",
            Thread.currentThread().getId(),
            name,
            hashCode(),
            constantThinkTimes.toString(),
            perTaskThinkTimes.toString()));
    }
    
    public void setOmegaSimulator(OmegaSimulator omegaSimulator) {
        this.omegaSimulator = omegaSimulator;
        this.simulator = omegaSimulator;
    }
    
    public OmegaSimulator getOmegaSimulator() {
        return omegaSimulator;
    }
    
    @Override
    protected void checkRegistered() {
        super.checkRegistered();
        if (omegaSimulator == null) {
            throw new IllegalStateException(
                "This scheduler has not been added to an OmegaSimulator yet.");
        }
    }
    
    /**
     * Increment daily counter for statistics.
     */
    private void incrementDailyCounter(Map<Integer, Integer> counter) {
        int index = (int) Math.floor(simulator.getCurrentTime() / 86400.0);
        counter.put(index, counter.getOrDefault(index, 0) + 1);
    }
    
    @Override
    public void addJob(Job job) {
        checkRegistered();
        
        if (job.getUnscheduledTasks() <= 0) {
            throw new IllegalArgumentException("Job must have unscheduled tasks");
        }
        
        job.setLastEnqueued(simulator.getCurrentTime());
        pendingQueue.offer(job);
        simulator.log("Scheduler " + name + " enqueued job " + job.getId() + 
                     " of workload type " + job.getWorkloadName() + ".");
        
        if (!scheduling) {
            omegaSimulator.log("Set " + name + " scheduling to TRUE to schedule job " + job.getId() + ".");
            scheduling = true;
            handleJob(pendingQueue.poll());
        }
    }
    
    /**
     * Handle a job: sync cell state, schedule it, and submit transaction.
     */
    public void handleJob(Job job) {
        job.updateTimeInQueueStats(simulator.getCurrentTime());
        syncCellState();
        double jobThinkTime = getThinkTime(job);
        
        final Job finalJob = job;
        omegaSimulator.afterDelay(jobThinkTime, () -> {
            finalJob.setNumSchedulingAttempts(finalJob.getNumSchedulingAttempts() + 1);
            finalJob.setNumTaskSchedulingAttempts(
                finalJob.getNumTaskSchedulingAttempts() + finalJob.getUnscheduledTasks());
            
            // Schedule the job in private cell state
            if (finalJob.getUnscheduledTasks() <= 0) {
                throw new IllegalStateException("Job must have unscheduled tasks");
            }
            
            List<ClaimDelta> claimDeltas = scheduleJob(finalJob, privateCellState);
            
            simulator.log(String.format(
                "Job %d (%s) finished %f seconds of scheduling thinktime; " +
                "now trying to claim resources for %d tasks with %f cpus and %f mem each.",
                finalJob.getId(), finalJob.getWorkloadName(), jobThinkTime,
                finalJob.getNumTasks(), finalJob.getCpusPerTask(), finalJob.getMemPerTask()));
            
            if (!claimDeltas.isEmpty()) {
                // Attempt to claim resources in common cell state by committing transaction
                omegaSimulator.log("Submitting a transaction for " + claimDeltas.size() + 
                                 " tasks for job " + finalJob.getId() + ".");
                
                CellState.CommitResult commitResult = 
                    omegaSimulator.getCellState().commit(claimDeltas, true);
                
                finalJob.setUnscheduledTasks(
                    finalJob.getUnscheduledTasks() - commitResult.getCommittedDeltas().size());
                omegaSimulator.log(commitResult.getCommittedDeltas().size() + 
                                 " tasks successfully committed for job " + finalJob.getId() + ".");
                
                numSuccessfulTaskTransactions += commitResult.getCommittedDeltas().size();
                numFailedTaskTransactions += commitResult.getConflictedDeltas().size();
                
                if (finalJob.getNumSchedulingAttempts() > 1) {
                    numRetriedTransactions++;
                }
                
                // Record job-level stats
                if (commitResult.getConflictedDeltas().isEmpty()) {
                    numSuccessfulTransactions++;
                    incrementDailyCounter(dailySuccessTransactions);
                    recordUsefulTimeScheduling(finalJob, jobThinkTime,
                        finalJob.getNumSchedulingAttempts() == 1);
                } else {
                    numFailedTransactions++;
                    incrementDailyCounter(dailyFailedTransactions);
                    recordWastedTimeScheduling(finalJob, jobThinkTime,
                        finalJob.getNumSchedulingAttempts() == 1);
                }
            } else {
                simulator.log("Not enough resources of the right shape were available " +
                             "to schedule even one task of job " + finalJob.getId() + 
                             ", so not submitting a transaction.");
                numNoResourcesFoundSchedulingAttempts++;
            }
            
            String jobEventType = "";
            
            // If job isn't fully scheduled, put it back in queue
            if (finalJob.getUnscheduledTasks() > 0) {
                // Give up on job if it hasn't scheduled in 100 tries or after 1000 tries
                if ((finalJob.getNumSchedulingAttempts() > 100 &&
                     finalJob.getUnscheduledTasks() == finalJob.getNumTasks()) ||
                    finalJob.getNumSchedulingAttempts() > 1000) {
                    System.out.println(String.format(
                        "Abandoning job %d (%f cpu %f mem) with %d/%d " +
                        "remaining tasks, after %d scheduling attempts.",
                        finalJob.getId(), finalJob.getCpusPerTask(), finalJob.getMemPerTask(),
                        finalJob.getUnscheduledTasks(), finalJob.getNumTasks(),
                        finalJob.getNumSchedulingAttempts()));
                    numJobsTimedOutScheduling++;
                    jobEventType = "abandoned";
                } else {
                    simulator.log("Job " + finalJob.getId() + " still has " + 
                                 finalJob.getUnscheduledTasks() + " unscheduled tasks, " +
                                 "adding it back to scheduler " + name + "'s job queue.");
                    simulator.afterDelay(1.0, () -> addJob(finalJob));
                }
            } else {
                jobEventType = "fully-scheduled";
            }
            
            omegaSimulator.log("Set " + name + " scheduling to FALSE");
            scheduling = false;
            
            // Keep trying to schedule as long as we have jobs in the queue
            if (!pendingQueue.isEmpty()) {
                scheduling = true;
                handleJob(pendingQueue.poll());
            }
        });
    }
    
    /**
     * Sync with common cell state by getting a copy.
     */
    public void syncCellState() {
        checkRegistered();
        privateCellState = omegaSimulator.getCellState().copy();
        simulator.log(name + " synced private cellstate.");
    }
    
    public CellState getPrivateCellState() {
        return privateCellState;
    }
}
