package se.kth.scs.partitioning;

import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Hooman
 */
public class ConcurrentVertex {

  private int partitions;
  private final int id;
  private final AtomicInteger pDegree;

  public ConcurrentVertex(int id) {
    this(id, 0);
  }

  public ConcurrentVertex(int id, int partitions) {
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

//  /**
//   * @param pDegree the pDegree to set
//   */
//  public void setpDegree(int pDegree) {
//    this.pDegree.set(pDegree);
//  }
  public synchronized void accumulate(Vertex v) {
    this.pDegree.addAndGet(v.getDegreeDelta());
    this.partitions = (this.partitions | v.getPartitionsDelta());
  }

//  public synchronized boolean containsPartition(short pid) {
//    return ((partitions >> pid) & 1) == 1;
//  }
  @Override
  public synchronized Vertex clone() {
    Vertex clone = new Vertex(id, getPartitions());
    clone.setpDegree(pDegree.get());
    return clone;
  }

}
