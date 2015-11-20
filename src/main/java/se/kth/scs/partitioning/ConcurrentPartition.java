package se.kth.scs.partitioning;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Hooman
 */
public class ConcurrentPartition {

    private final short id;
    private final AtomicInteger eSize;
    private final AtomicInteger vSize;

    public ConcurrentPartition(short id) {
        eSize = new AtomicInteger();
        vSize = new AtomicInteger();
        this.id = id;
    }

    /**
     * @return the id
     */
    public short getId() {
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

//    /**
//     * @param eSize the eSize to set
//     */
//    public void setESize(int eSize) {
//        this.eSize.set(eSize);
//    }

    /**
     * @return the vSize
     */
    public int getVSize() {
        return vSize.get();
    }

//    /**
//     * @param vSize the vSize to set
//     */
//    public void setVSize(int vSize) {
//        this.vSize.set(vSize);
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
    public synchronized Partition clone() {
        Partition clone = new Partition(id);
        clone.setESize(this.getESize());
        clone.setVSize(this.getVSize());
        return clone;
    }

}
