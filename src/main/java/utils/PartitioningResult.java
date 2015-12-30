package utils;

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
  public final long totalTime;

  public PartitioningResult(final float avgReplicationFactor,
    final int maxVertexCardinality,
    final int maxEdgeCardinality,
    final float loadRelativeStandardDeviation,
    final int experimentNumber,
    final int restreaming,
    final int window,
    final int task,
    final long seed,
    final long totalTime) {
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

}
