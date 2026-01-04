package simulator.core;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that holds a list of jobs for a workload.
 */
public class Workload {
    private final String name;
    private final List<Job> jobs;
    
    public Workload(String name) {
        this.name = name;
        this.jobs = new ArrayList<>();
    }
    
    public String getName() {
        return name;
    }
    
    public List<Job> getJobs() {
        return new ArrayList<>(jobs);
    }
    
    public void addJob(Job job) {
        if (!job.getWorkloadName().equals(name)) {
            throw new IllegalArgumentException(
                "Job workload name " + job.getWorkloadName() + 
                " does not match workload name " + name);
        }
        jobs.add(job);
    }
    
    public void addJobs(List<Job> jobs) {
        for (Job job : jobs) {
            addJob(job);
        }
    }
    
    public int getNumJobs() {
        return jobs.size();
    }
    
    public double getCpus() {
        return jobs.stream()
            .mapToDouble(j -> j.getNumTasks() * j.getCpusPerTask())
            .sum();
    }
    
    public double getMem() {
        return jobs.stream()
            .mapToDouble(j -> j.getNumTasks() * j.getMemPerTask())
            .sum();
    }
    
    public Workload copy() {
        Workload newWorkload = new Workload(name);
        for (Job job : jobs) {
            newWorkload.addJob(job.copy());
        }
        return newWorkload;
    }
    
    public double getTotalJobUsefulThinkTimes() {
        return jobs.stream()
            .mapToDouble(Job::getUsefulTimeScheduling)
            .sum();
    }
    
    public double getTotalJobWastedThinkTimes() {
        return jobs.stream()
            .mapToDouble(Job::getWastedTimeScheduling)
            .sum();
    }
}

