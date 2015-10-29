package se.kth.scs.partitioning;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Hooman
 */
public class ConcurrentVertex {

    private final Set<Integer> partitions;
    private final long id;
    private final AtomicInteger pDegree;

    public ConcurrentVertex(long id, Set<Integer> partitions) {
        this.partitions = partitions;
        pDegree = new AtomicInteger();
        this.id = id;
    }

    /**
     * @return the partitions
     */
    public synchronized Set<Integer> getPartitions() {
        return new HashSet<>(partitions);
    }

    /**
     * @return the id
     */
    public long getId() {
        return id;
    }

    /**
     * @return the pDegree
     */
    public int getpDegree() {
        return pDegree.get();
    }

    /**
     * @param pDegree the pDegree to set
     */
    public void setpDegree(int pDegree) {
        this.pDegree.set(pDegree);
    }

    public synchronized void accumulate(Vertex v) {
        this.pDegree.addAndGet(v.getDegreeDelta());
        this.partitions.addAll(v.getPartitionsDelta());
    }

    public synchronized boolean containsPartition(int pid) {
        return partitions.contains(pid);
    }

    @Override
    public synchronized Vertex clone() {
        Vertex clone = new Vertex(id, getPartitions());
        clone.setpDegree(pDegree.get());
        return clone;
    }

}
