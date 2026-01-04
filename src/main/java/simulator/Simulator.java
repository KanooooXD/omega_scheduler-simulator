package simulator;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A simple, generic, discrete event simulator.
 * Based on the discrete event simulator from "Programming In Scala".
 */
public abstract class Simulator {
    protected double currentTime = 0.0;
    protected final PriorityQueue<WorkItem> agenda = new PriorityQueue<>(
        Comparator.comparing(WorkItem::getTime).reversed());
    private final boolean logging;
    
    public Simulator(boolean logging) {
        this.logging = logging;
    }
    
    public double getCurrentTime() {
        return currentTime;
    }
    
    public int getAgendaSize() {
        return agenda.size();
    }
    
    protected void log(String message) {
        if (logging) {
            System.out.println(currentTime + " " + message);
        }
    }
    
    /**
     * Schedule an action to be executed after a delay.
     */
    public void afterDelay(double delay, Runnable action) {
        WorkItem item = new WorkItem(currentTime + delay, action);
        agenda.offer(item);
    }
    
    private void next() {
        WorkItem item = agenda.poll();
        if (item != null) {
            currentTime = item.getTime();
            item.getAction().run();
        }
    }
    
    /**
     * Run the simulation for the specified time or until completion.
     * @param runTime Optional maximum simulation time
     * @param wallClockTimeout Optional wall clock timeout in seconds
     * @return true if simulation ran till runTime or completion, false if timed out
     */
    public boolean run(Double runTime, Double wallClockTimeout) {
        afterDelay(0, () -> {
            System.out.println("*** Simulation started, time = " + currentTime + ". ***");
        });
        
        long startWallTime = System.currentTimeMillis();
        
        while (!agenda.isEmpty()) {
            if (runTime != null && currentTime > runTime) {
                break;
            }
            if (wallClockTimeout != null) {
                long currWallTime = System.currentTimeMillis();
                if ((currWallTime - startWallTime) / 1000.0 > wallClockTimeout) {
                    System.out.println(String.format(
                        "Execution timed out after %f seconds, ending simulation now.",
                        (currWallTime - startWallTime) / 1000.0));
                    return false;
                }
            }
            next();
        }
        
        System.out.println("*** Simulation finished running, time = " + currentTime + ". ***");
        return true;
    }
    
    private static class WorkItem {
        private final double time;
        private final Runnable action;
        
        public WorkItem(double time, Runnable action) {
            this.time = time;
            this.action = action;
        }
        
        public double getTime() { return time; }
        public Runnable getAction() { return action; }
    }
}

