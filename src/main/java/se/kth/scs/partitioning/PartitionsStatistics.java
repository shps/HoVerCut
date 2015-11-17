package se.kth.scs.partitioning;

import java.util.List;
import java.util.Map;

/**
 *
 * @author Hooman
 */
public class PartitionsStatistics {

  private final float avgReplicationFactor;
  private final int maxVertexCardinality;
  private final int maxEdgeCardinality;
  private final float loadRelativeStandardDeviation;
  private final int nVertices;
  private final int nEdges;
  private final int[] nEdgePartitions;
  private final int[] nVertexPartitions;
  private final Map<Long, Vertex> vertices;

  /**
   * Eagerly calculates some metrics about a list of partitions.
   *
   * @param state
   */
  public PartitionsStatistics(PartitionState state) {
    vertices = state.getAllVertices();
    List<Partition> partitions = state.getAllPartitions();
    nEdgePartitions = new int[partitions.size()];
    nVertexPartitions = new int[partitions.size()];

    int totalReplicas = 0;
    nVertices = vertices.size();
    for (Vertex v : vertices.values()) {
      int ps = v.getPartitions();
      for (int i = 0; i < partitions.size(); i++) {
        if ((ps & 1) == 1) {
          totalReplicas++;
        }
        ps = ps >> 1;
      }
    }
    avgReplicationFactor = calculateReplicationFactor(totalReplicas, nVertices);
    loadRelativeStandardDeviation = calculateRelativeStandardDeviation(partitions);
    //find max edge and vertex cardinality.
    int maxV = 0;
    int maxE = 0;
    int i = 0;
    int eSize = 0;
    for (Partition p : partitions) {
      nEdgePartitions[i] = p.getESize();
      nVertexPartitions[i] = p.getVSize();
      eSize += p.getESize();
      if (p.getVSize() > maxV) {
        maxV = p.getVSize();
      }
      if (p.getESize() > maxE) {
        maxE = p.getESize();
      }
      i++;
    }
    nEdges = eSize;
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

  private float calculateReplicationFactor(int totalReplicas, int nVertices) {
    float averageReplicationFactor = (float) totalReplicas / (float) nVertices;

    return averageReplicationFactor;
  }

  private float calculateRelativeStandardDeviation(List<Partition> partitions) {
    int sum = 0;
    int n = partitions.size();
    for (Partition p : partitions) {
      sum += p.getESize();
    }
    float mean = (float) sum / (float) n;

    float sumSqr = 0;
    for (Partition p : partitions) {
      sumSqr += Math.pow(p.getESize() - mean, 2);
    }

    float sdv = (float) Math.sqrt(sumSqr / (float) (n - 1));

    return sdv * 100 / mean;
  }

  /**
   * Relative standard deviation of the number of edges hosted in the
   * partitions.
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
   * ** The number of vertices in the Partition with the max vertex
   * cardinality.
   *
   * @return
   */
  public int maxVertexCardinality() {
    return maxVertexCardinality;
  }

  /**
   * @return the nVertices
   */
  public int getNVertices() {
    return nVertices;
  }

  /**
   * @return the nEdges
   */
  public int getNEdges() {
    return nEdges;
  }

  /**
   * @return the nEdgePartitions
   */
  public int[] getNEdgesPartitions() {
    return nEdgePartitions;
  }

  /**
   * @return the nEdgePartitions
   */
  public int[] getNVerticesPartitions() {
    return nVertexPartitions;
  }

  /**
   * @return the vertices
   */
  public Map<Long, Vertex> getVertices() {
    return vertices;
  }
}
