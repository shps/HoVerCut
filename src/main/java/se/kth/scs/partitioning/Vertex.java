package se.kth.scs.partitioning;

/**
 *
 * @author Hooman
 */
public class Vertex {

  private byte partitions;
  private final long id;
  private int pDegree;
  private int degreeDelta = 0;
  private byte partitionsDelta;

  public Vertex(long id, byte partitions) {
    this.partitions = partitions;
    this.id = id;
  }

  public Vertex(long id) {
    this(id, (byte) 0);
  }

  /**
   *
   * @param p
   * @return true if partition does not exist and false if it already exists.
   */
  public boolean addPartition(int p) {
    if (!this.containsPartition(p)) {
      setPartitionsDelta((byte) (partitionsDelta | (1 << p)));
      return true;
    }

    return false;
  }

  /**
   * @return the partitions
   */
  public byte getPartitions() {
    return (byte) (partitions | partitionsDelta);
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
  public byte getPartitionsDelta() {
    return partitionsDelta;
  }

  public boolean containsPartition(int pid) {
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
  public void setPartitionsDelta(byte partitionsDelta) {
    this.partitionsDelta = partitionsDelta;
  }

  /**
   * @param partitions the partitions to set
   */
  public void setPartitions(byte partitions) {
    this.partitions = partitions;
  }

}
