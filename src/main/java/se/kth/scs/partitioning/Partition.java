package se.kth.scs.partitioning;

/**
 *
 * @author Hooman
 */
public class Partition {

  private final short id;
  private int eSize = 0;
  private int eSizeDelta = 0;

  public Partition(short id) {
    this.id = id;
  }

  public void incrementESize() {
    this.eSizeDelta++;
  }

  /**
   * @return the id
   */
  public short getId() {
    return id;
  }

  /**
   * @return the eSize
   */
  public int getESize() {
    return eSize + eSizeDelta;
  }

  /**
   * @param eSize the eSize to set
   */
  public void setESize(int eSize) {
    this.eSize = eSize;
  }

  /**
   * @return the eSizeDelta
   */
  public int getESizeDelta() {
    return eSizeDelta;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Partition)) {
      return false;
    }
    Partition other = (Partition) obj;
    return other.getId() == this.getId();
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 73 * hash + this.id;
    return hash;
  }

  /**
   * @param eSizeDelta the eSizeDelta to set
   */
  public void seteSizeDelta(int eSizeDelta) {
    this.eSizeDelta = eSizeDelta;
  }
}
