package se.kth.scs.partitioning;

/**
 * This class is used to keep track of local state of a vertex in
 * a loader.
 *
 * @author Hooman
 */
public class Vertex {

  private int partitions;
  private final int id;
  private int pDegree;
  private int degreeDelta = 0;
  private int partitionsDelta;

  public Vertex(int id, int partitions) {
    this.partitions = partitions;
    this.id = id;
  }

  public Vertex(int id) {
    this(id, 0);
  }

  /**
   *
   * @param p
   * @return true if partition does not exist and false if it already exists.
   */
  public boolean addPartition(short p) {
    if (!this.containsPartition(p)) {
      setPartitionsDelta(partitionsDelta | (1 << p));
      return true;
    }

    return false;
  }

  /**
   * @return the partitions
   */
  public int getPartitions() {
    return (partitions | partitionsDelta);
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
  public int getPartitionsDelta() {
    return partitionsDelta;
  }

  public boolean containsPartition(short pid) {
    return (((partitions | partitionsDelta) >> pid) & 1) == 1;
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
  public void setPartitionsDelta(int partitionsDelta) {
    this.partitionsDelta = partitionsDelta;
  }

  /**
   * @param partitions the partitions to set
   */
  public void setPartitions(int partitions) {
    this.partitions = partitions;
  }
}
