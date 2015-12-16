package se.kth.scs.partitioning;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to keep global state of a partition between multiple
 * loaders.
 *
 * @author Hooman
 */
public class ConcurrentPartition {

  private final short id;
  private final AtomicInteger eSize;

  public ConcurrentPartition(short id) {
    eSize = new AtomicInteger();
    this.id = id;
  }

  /**
   * @return the id
   */
  public short getId() {
    return id;
  }

  public void accumulate(Partition p) {
    eSize.addAndGet(p.getESizeDelta());
  }

  /**
   * @return the eSize
   */
  public int getESize() {
    return eSize.get();
  }

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
  public Partition clone() {
    Partition clone = new Partition(id);
    clone.setESize(this.getESize());
    return clone;
  }

}
