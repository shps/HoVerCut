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
  public final int experimentNumber;
  public final int restreaming;
  public final int window;
  public final int task;
  public final long seed;
  public final float totalTime;

  public PartitioningResult(final float avgReplicationFactor,
    final int maxVertexCardinality,
    final int maxEdgeCardinality,
    final float loadRelativeStandardDeviation,
    final int experimentNumber,
    final int restreaming,
    final int window,
    final int task,
    final long seed,
    final float totalTime) {
    this.avgReplicationFactor = avgReplicationFactor;
    this.maxVertexCardinality = maxVertexCardinality;
    this.maxEdgeCardinality = maxEdgeCardinality;
    this.loadRelativeStandardDeviation = loadRelativeStandardDeviation;
    this.experimentNumber = experimentNumber;
    this.restreaming = restreaming;
    this.window = window;
    this.task = task;
    this.seed = seed;
    this.totalTime = totalTime;
  }

  @Override
  public int hashCode() {
    String s = String.format("%d,%d,%d", task, window, restreaming);
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
    if (this.restreaming != other.restreaming) {
      return false;
    }
    if (this.window != other.window) {
      return false;
    }
    return this.task == other.task;
  }

}
