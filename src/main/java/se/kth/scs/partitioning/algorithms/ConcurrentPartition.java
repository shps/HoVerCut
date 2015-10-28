package se.kth.scs.partitioning.algorithms;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Hooman
 */
public class ConcurrentPartition {

    private final int id;
    private final AtomicInteger eSize;
    private final AtomicInteger vSize;

//    private int eSizeDelta = 0;
//    private int vSizeDelta = 0;
    public ConcurrentPartition(int id) {
        eSize = new AtomicInteger();
        vSize = new AtomicInteger();
        this.id = id;
    }

//    public void incrementESize() {
//        this.eSizeDelta++;
//    }
//
//    public void incrementVSize() {
//        this.vSizeDelta++;
//    }
    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    public synchronized void accumulate(Partition p) {
        eSize.addAndGet(p.getESizeDelta());
        vSize.addAndGet(p.getVSizeDelta());
    }

    /**
     * @return the eSize
     */
    public int getESize() {
        return eSize.get();
    }

    /**
     * @param eSize the eSize to set
     */
    public void setESize(int eSize) {
        this.eSize.set(eSize);
    }

    /**
     * @return the vSize
     */
    public int getVSize() {
        return vSize.get();
    }

    /**
     * @param vSize the vSize to set
     */
    public void setVSize(int vSize) {
        this.vSize.set(vSize);
    }

//    /**
//     * @return the eSizeDelta
//     */
//    public int getESizeDelta() {
//        return eSizeDelta;
//    }
//
//    /**
//     * @return the vSizeDelta
//     */
//    public int getVSizeDelta() {
//        return vSizeDelta;
//    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ConcurrentPartition)) {
            return false;
        }
        ConcurrentPartition other = (ConcurrentPartition) obj;
        return other.getId() == this.getId();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + this.id;
        return hash;
    }

    @Override
    protected synchronized Partition clone() {
        Partition clone = new Partition(id);
        clone.setESize(this.getESize());
        clone.setVSize(this.getVSize());
        return clone;
    }

}
