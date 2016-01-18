package se.kth.scs.partitioning;

import java.util.BitSet;

/**
 * This class is used to keep track of local state of a vertex in a loader.
 *
 * @author Hooman
 */
public class Vertex {

  private BitSet partitions;
  private final int id;
  private int pDegree;
  private int degreeDelta = 0;
  private BitSet partitionsDelta;

  public Vertex(int id, BitSet partitions) {
    this.partitions = partitions;
    this.partitionsDelta = new BitSet(partitions.size());
    this.id = id;
  }

  public Vertex(int id) {
    this(id, new BitSet());
  }

  /**
   *
   * @param p
   * @return true if partition does not exist and false if it already exists.
   */
  public boolean addPartition(short p) {
    if (!this.containsPartition(p)) {
      partitionsDelta.set(p);
      setPartitionsDelta(partitionsDelta);
      return true;
    }

    return false;
  }

  /**
   * @return the partitions
   */
  public BitSet getPartitions() {
    BitSet bs = new BitSet();
    bs.or(partitions);
    bs.or(partitionsDelta);
    return bs;
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
  public BitSet getPartitionsDelta() {
    return partitionsDelta;
  }

  public boolean containsPartition(short pid) {
    return partitions.get(pid) | partitionsDelta.get(pid);
  }

  /**
   * @param degreeDelta the degreeDelta to set
   */
  public void setDegreeDelta(int degreeDelta) {
    this.degreeDelta = degreeDelta;
  }

  /**
   * @param partitionsDelta the partitionsDelta to set
   */
  public void setPartitionsDelta(BitSet partitionsDelta) {
    this.partitionsDelta = partitionsDelta;
  }

  /**
   * @param partitions the partitions to set
   */
  public void setPartitions(BitSet partitions) {
    this.partitions = partitions;
  }
}
