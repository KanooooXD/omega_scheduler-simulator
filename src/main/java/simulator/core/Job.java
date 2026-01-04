package simulator.core;

/**
 * Represents a job in the cluster scheduling simulator.
 * A job consists of multiple tasks, all with the same resource requirements.
 */
public class Job {
    private final long id;
    private final double submitted;
    private final int numTasks;
    private double taskDuration;
    private final String workloadName;
    private final double cpusPerTask;
    private final double memPerTask;
    private final boolean isRigid;
    
    // Scheduling state
    private int unscheduledTasks;
    private double timeInQueueTillFirstScheduled = 0.0;
    private double timeInQueueTillFullyScheduled = 0.0;
    private double lastEnqueued = 0.0;
    private double lastSchedulingStartTime = 0.0;
    private long numSchedulingAttempts = 0;
    private long numTaskSchedulingAttempts = 0;
    private double usefulTimeScheduling = 0.0;
    private double wastedTimeScheduling = 0.0;
    
    public Job(long id, double submitted, int numTasks, double taskDuration,
               String workloadName, double cpusPerTask, double memPerTask, boolean isRigid) {
        this.id = id;
        this.submitted = submitted;
        this.numTasks = numTasks;
        this.taskDuration = taskDuration;
        this.workloadName = workloadName;
        this.cpusPerTask = cpusPerTask;
        this.memPerTask = memPerTask;
        this.isRigid = isRigid;
        this.unscheduledTasks = numTasks;
    }
    
    public Job(long id, double submitted, int numTasks, double taskDuration,
               String workloadName, double cpusPerTask, double memPerTask) {
        this(id, submitted, numTasks, taskDuration, workloadName, cpusPerTask, memPerTask, false);
    }
    
    // Getters
    public long getId() { return id; }
    public double getSubmitted() { return submitted; }
    public int getNumTasks() { return numTasks; }
    public double getTaskDuration() { return taskDuration; }
    public String getWorkloadName() { return workloadName; }
    public double getCpusPerTask() { return cpusPerTask; }
    public double getMemPerTask() { return memPerTask; }
    public boolean isRigid() { return isRigid; }
    public int getUnscheduledTasks() { return unscheduledTasks; }
    public double getTimeInQueueTillFirstScheduled() { return timeInQueueTillFirstScheduled; }
    public double getTimeInQueueTillFullyScheduled() { return timeInQueueTillFullyScheduled; }
    public double getLastEnqueued() { return lastEnqueued; }
    public double getLastSchedulingStartTime() { return lastSchedulingStartTime; }
    public long getNumSchedulingAttempts() { return numSchedulingAttempts; }
    public long getNumTaskSchedulingAttempts() { return numTaskSchedulingAttempts; }
    public double getUsefulTimeScheduling() { return usefulTimeScheduling; }
    public double getWastedTimeScheduling() { return wastedTimeScheduling; }
    
    // Setters
    public void setTaskDuration(double taskDuration) { this.taskDuration = taskDuration; }
    public void setUnscheduledTasks(int unscheduledTasks) { this.unscheduledTasks = unscheduledTasks; }
    public void setTimeInQueueTillFirstScheduled(double time) { this.timeInQueueTillFirstScheduled = time; }
    public void setTimeInQueueTillFullyScheduled(double time) { this.timeInQueueTillFullyScheduled = time; }
    public void setLastEnqueued(double lastEnqueued) { this.lastEnqueued = lastEnqueued; }
    public void setLastSchedulingStartTime(double time) { this.lastSchedulingStartTime = time; }
    public void setNumSchedulingAttempts(long num) { this.numSchedulingAttempts = num; }
    public void setNumTaskSchedulingAttempts(long num) { this.numTaskSchedulingAttempts = num; }
    public void setUsefulTimeScheduling(double time) { this.usefulTimeScheduling = time; }
    public void setWastedTimeScheduling(double time) { this.wastedTimeScheduling = time; }
    
    public double getCpusStillNeeded() {
        return cpusPerTask * unscheduledTasks;
    }
    
    public double getMemStillNeeded() {
        return memPerTask * unscheduledTasks;
    }
    
    /**
     * Calculate the maximum number of this job's tasks that can fit into
     * the specified resources.
     */
    public int numTasksToSchedule(double cpusAvail, double memAvail) {
        if (cpusAvail == 0.0 || memAvail == 0.0) {
            return 0;
        }
        double cpusChoppedToTaskSize = cpusAvail - (cpusAvail % cpusPerTask);
        double memChoppedToTaskSize = memAvail - (memAvail % memPerTask);
        long maxTasksThatWillFitByCpu = Math.round(cpusChoppedToTaskSize / cpusPerTask);
        long maxTasksThatWillFitByMem = Math.round(memChoppedToTaskSize / memPerTask);
        long maxTasksThatWillFit = Math.min(maxTasksThatWillFitByCpu, maxTasksThatWillFitByMem);
        return (int) Math.min(unscheduledTasks, maxTasksThatWillFit);
    }
    
    /**
     * Update time in queue statistics.
     */
    public void updateTimeInQueueStats(double currentTime) {
        // Every time part of this job is partially scheduled, add to
        // the counter tracking how long it spends in the queue till
        // its final task is scheduled.
        timeInQueueTillFullyScheduled += currentTime - lastEnqueued;
        // If this is the first scheduling done for this job, then make a note
        // about how long the job waited in the queue for this first scheduling.
        if (numSchedulingAttempts == 0) {
            timeInQueueTillFirstScheduled += currentTime - lastEnqueued;
        }
    }
    
    public Job copy() {
        Job copy = new Job(id, submitted, numTasks, taskDuration, workloadName, 
                          cpusPerTask, memPerTask, isRigid);
        copy.unscheduledTasks = this.unscheduledTasks;
        copy.timeInQueueTillFirstScheduled = this.timeInQueueTillFirstScheduled;
        copy.timeInQueueTillFullyScheduled = this.timeInQueueTillFullyScheduled;
        copy.lastEnqueued = this.lastEnqueued;
        copy.lastSchedulingStartTime = this.lastSchedulingStartTime;
        copy.numSchedulingAttempts = this.numSchedulingAttempts;
        copy.numTaskSchedulingAttempts = this.numTaskSchedulingAttempts;
        copy.usefulTimeScheduling = this.usefulTimeScheduling;
        copy.wastedTimeScheduling = this.wastedTimeScheduling;
        return copy;
    }
}

