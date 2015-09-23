package se.kth.scs.partitioning;

import java.util.Collection;
import java.util.HashMap;

/**
 *
 * @author Hooman
 */
public class PartitionsStatistics {

  private final float avgReplicationFactor;
  private final int maxVertexCardinality;
  private final int maxEdgeCardinality;
  private final float loadRelativeStandardDeviation;

  /**
   * Eagerly calculates some metrics about a list of partitions.
   *
   * @param partitions
   */
  public PartitionsStatistics(Partition[] partitions) {
    avgReplicationFactor = calculateReplicationFactor(partitions);
    loadRelativeStandardDeviation = calculateRelativeStandardDeviation(partitions);
    //find max edge and vertex cardinality.
    int maxV = 0;
    int maxE = 0;
    for (Partition p : partitions) {
      if (p.vertexSize() > maxV) {
        maxV = p.vertexSize();
      }
      if (p.edgeSize() > maxE) {
        maxE = p.edgeSize();
      }
    }

    maxVertexCardinality = maxV;
    maxEdgeCardinality = maxE;
  }

  /**
   * Average number of replicas per vertex.
   *
   * @return
   */
  public float replicationFactor() {
    return avgReplicationFactor;
  }

  private float calculateReplicationFactor(Partition[] partitions) {
    HashMap<Long, Integer> vReplicas = new HashMap<>();
    for (Partition p : partitions) {
      for (Long vertexId : p.getVertices()) {
        if (!vReplicas.containsKey(vertexId)) {
          vReplicas.put(vertexId, 1);
        } else {
          vReplicas.put(vertexId, vReplicas.get(vertexId) + 1);
        }
      }
    }

    int sum = 0;
    Collection<Integer> rFactors = vReplicas.values();
    for (Integer rf : rFactors) {
      sum += rf;
    }

    float averageReplicationFactor = (float) sum / (float) rFactors.size();

    return averageReplicationFactor;
  }

  private float calculateRelativeStandardDeviation(Partition[] partitions) {
    int sum = 0;
    for (Partition p : partitions) {
      sum += p.edgeSize();
    }
    int n = partitions.length;
    float mean = (float) sum / (float) n;

    float sumSqr = 0;
    for (Partition p : partitions) {
      sumSqr += Math.pow(p.edgeSize() - mean, 2);
    }

    float sdv = (float) Math.sqrt(sumSqr / (float) (n - 1));

    return sdv * 100 / mean;
  }

  /**
   * Relative standard deviation of the number of edges hosted in the partitions.
   *
   * @return
   */
  public float loadRelativeStandardDeviation() {
    return loadRelativeStandardDeviation;
  }

  /**
   * The number of edges in the Partition with the max edge cardinality.
   *
   * @return
   */
  public int maxEdgeCardinality() {
    return maxEdgeCardinality;
  }

  /**
   * * The number of vertices in the Partition with the max vertex cardinality.
   *
   * @return
   */
  public int maxVertexCardinality() {
    return maxVertexCardinality;
  }
}
