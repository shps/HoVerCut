package se.kth.scs.utils;

/**
 *
 * @author Hooman
 */
public class PartitioningResult {

  public final float avgReplicationFactor;
  public final int maxVertexCardinality;
  public final int maxEdgeCardinality;
  public final float loadRelativeStandardDeviation;
  public final int window;
  public final int task;
  public final long seed;
  public final float totalTime;

  public PartitioningResult(final float avgReplicationFactor,
    final int maxVertexCardinality,
    final int maxEdgeCardinality,
    final float loadRelativeStandardDeviation,
    final int window,
    final int task,
    final long seed,
    final float totalTime) {
    this.avgReplicationFactor = avgReplicationFactor;
    this.maxVertexCardinality = maxVertexCardinality;
    this.maxEdgeCardinality = maxEdgeCardinality;
    this.loadRelativeStandardDeviation = loadRelativeStandardDeviation;
    this.window = window;
    this.task = task;
    this.seed = seed;
    this.totalTime = totalTime;
  }

  @Override
  public int hashCode() {
    String s = String.format("%d,%d", task, window);
    return s.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final PartitioningResult other = (PartitioningResult) obj;
    if (this.window != other.window) {
      return false;
    }
    return this.task == other.task;
  }

}
