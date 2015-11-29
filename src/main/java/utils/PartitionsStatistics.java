package utils;

import java.util.List;
import java.util.Map;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.Vertex;

/**
 * This class provides useful statistics about partitions.
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
  private final Map<Integer, Vertex> vertices;

  /**
   * Eagerly calculates some metrics about a list of partitions.
   *
   * @param state
   * @param expectedVertices
   */
  public PartitionsStatistics(PartitionState state, int expectedVertices) {
    vertices = state.getAllVertices(expectedVertices);
    List<Partition> partitions = state.getAllPartitions();
    state.releaseTaskResources();
    nEdgePartitions = new int[partitions.size()];
    nVertexPartitions = new int[partitions.size()];

    int totalReplicas = 0;
    nVertices = vertices.size();
    for (Vertex v : vertices.values()) {
      int ps = v.getPartitions();
      for (Partition p : partitions) {
        if ((ps & 1) == 1) {
          totalReplicas++;
          nVertexPartitions[p.getId()] += 1;
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
      eSize += p.getESize();
      if (p.getESize() > maxE) {
        maxE = p.getESize();
      }
      if (nVertexPartitions[p.getId()] > maxV) {
        maxV = nVertexPartitions[p.getId()];
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
  public Map<Integer, Vertex> getVertices() {
    return vertices;
  }
}
