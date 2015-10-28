package se.kth.scs.partitioning.algorithms;

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
//    private int degreeDelta = 0;
//    private final Set<Integer> partitionsDelta = new HashSet<>();

    public ConcurrentVertex(long id, Set<Integer> partitions) {
        this.partitions = partitions;
        pDegree = new AtomicInteger();
        this.id = id;
    }

//    /**
//     *
//     * @param p
//     * @return true if partition does not exist and false if it already exists.
//     */
//    public synchronized boolean addPartition(int p) {
//        if (!this.containsPartition(p)) {
//            getPartitionsDelta().add(p);
//            return true;
//        }
//
//        return false;
//    }
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

//    public synchronized void incrementDegree() {
//        this.degreeDelta++;
//    }
    public synchronized void accumulate(Vertex v) {
        this.pDegree.addAndGet(v.getDegreeDelta());
        this.partitions.addAll(v.getPartitionsDelta());
    }

//    /**
//     * @return the degreeDelta
//     */
//    public synchronized int getDegreeDelta() {
//        return degreeDelta;
//    }
//    /**
//     * @return the partitionsDelta
//     */
//    public synchronized Set<Integer> getPartitionsDelta() {
//        return partitionsDelta;
//    }
    public synchronized boolean containsPartition(int pid) {
        return partitions.contains(pid);
    }

    @Override
    protected synchronized Vertex clone() {
        Vertex clone = new Vertex(id, getPartitions());
        clone.setpDegree(pDegree.get());
        return clone;
    }

}
