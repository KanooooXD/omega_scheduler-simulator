package scheduler;

import simulator.core.CellState;

/**
 * Represents a resource offer made by the Mesos allocator to a scheduler.
 */
public class Offer {
    private final long id;
    private final MesosScheduler scheduler;
    private final CellState cellState;
    
    public Offer(long id, MesosScheduler scheduler, CellState cellState) {
        this.id = id;
        this.scheduler = scheduler;
        this.cellState = cellState;
    }
    
    public long getId() {
        return id;
    }
    
    public MesosScheduler getScheduler() {
        return scheduler;
    }
    
    public CellState getCellState() {
        return cellState;
    }
}
