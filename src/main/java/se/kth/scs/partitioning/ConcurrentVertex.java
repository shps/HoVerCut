package se.kth.scs.partitioning;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used to keep a global state of a Vertex between multiple
 * loaders.
 *
 * @author Hooman
 */
public class ConcurrentVertex {

  private int partitions;
  private final int id;
  private final AtomicInteger pDegree;

  public ConcurrentVertex(final int id) {
    this(id, 0);
  }

  public ConcurrentVertex(final int id, final int partitions) {
    this.partitions = partitions;
    pDegree = new AtomicInteger();
    this.id = id;
  }

  /**
   * @return the partitions
   */
  public synchronized int getPartitions() {
    return partitions;
  }

  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * @return the pDegree
   */
  public synchronized int getpDegree() {
    return pDegree.get();
  }

  public synchronized void accumulate(final Vertex v) {
    this.pDegree.addAndGet(v.getDegreeDelta());
    this.partitions = (this.partitions | v.getPartitionsDelta());
  }

  @Override
  public synchronized Vertex clone() {
    Vertex clone = new Vertex(id, getPartitions());
    clone.setpDegree(pDegree.get());
    return clone;
  }

  /**
   * Clears partitions. Not thread-safe.
   */
  public void resetPartition() {
    this.partitions = 0;
  }
}
