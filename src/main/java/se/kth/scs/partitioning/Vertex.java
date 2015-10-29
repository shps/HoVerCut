package se.kth.scs.partitioning;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Hooman
 */
public class Vertex {

    private final Set<Integer> partitions;
    private final long id;
    private int pDegree;
    private int degreeDelta = 0;
    private final Set<Integer> partitionsDelta = new HashSet<>();

    public Vertex(long id, Set<Integer> partitions) {
        this.partitions = partitions;
        this.id = id;
    }

    /**
     *
     * @param p
     * @return true if partition does not exist and false if it already exists.
     */
    public boolean addPartition(int p) {
        if (!this.containsPartition(p)) {
            getPartitionsDelta().add(p);
            return true;
        }

        return false;
    }

    /**
     * @return the partitions
     */
    public Set<Integer> getPartitions() {
        Set<Integer> ps = new HashSet<>(partitions);
        ps.addAll(partitionsDelta);
        return ps;
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
        return pDegree + degreeDelta;
    }

    /**
     * @param pDegree the pDegree to set
     */
    public void setpDegree(int pDegree) {
        this.pDegree = pDegree;
    }

    public void incrementDegree() {
        this.degreeDelta++;
    }

    /**
     * @return the degreeDelta
     */
    public int getDegreeDelta() {
        return degreeDelta;
    }

    /**
     * @return the partitionsDelta
     */
    public Set<Integer> getPartitionsDelta() {
        return partitionsDelta;
    }

    public boolean containsPartition(int pid) {
        return partitions.contains(pid) || partitionsDelta.contains(pid);
    }

}
