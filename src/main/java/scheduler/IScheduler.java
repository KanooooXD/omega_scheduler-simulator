package scheduler;

import simulator.core.Job;
import simulator.core.ClaimDelta;
import simulator.core.CellState;
import simulator.ClusterSimulator;

import java.util.List;

/**
 * Base interface for all schedulers in the cluster scheduling simulator.
 */
public interface IScheduler {
    /**
     * Add a job to the scheduler's queue.
     */
    void addJob(Job job);
    
    /**
     * Get the name of this scheduler.
     */
    String getName();
    
    /**
     * Get the number of jobs currently in the queue.
     */
    long getJobQueueSize();
    
    /**
     * Check if the scheduler is currently scheduling a job.
     */
    boolean isScheduling();
    
    /**
     * Schedule a job onto available resources in the cell state.
     */
    List<ClaimDelta> scheduleJob(Job job, CellState cellState);
    
    /**
     * Get the think time required to schedule a job.
     */
    double getThinkTime(Job job);
    
    /**
     * Set the simulator this scheduler is running in.
     */
    void setSimulator(ClusterSimulator simulator);
}
