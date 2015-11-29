package se.kth.scs.partitioning;

/**
 * The implementation of a graph edge.
 *
 * @author Hooman
 */
public class Edge {

  private final int src;
  private final int dst;

  public Edge(int src, int dst) {
    this.src = src;
    this.dst = dst;
  }

  /**
   * @return the src
   */
  public int getSrc() {
    return src;
  }

  /**
   * @return the dst
   */
  public int getDst() {
    return dst;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Edge)) {
      return false;
    }

    final Edge other = (Edge) o;
    if (this.src != other.src) {
      return this.src == other.dst && this.dst == other.src;
    }
    return this.dst == other.dst;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public String toString() {
    String s;
    if (src < dst) {
      s = src + "," + dst;
    } else {
      s = dst + "," + src;
    }
    return s;
  }
}
