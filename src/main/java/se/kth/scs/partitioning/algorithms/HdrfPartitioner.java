package se.kth.scs.partitioning.algorithms;

import java.util.HashMap;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.Partition;

/**
 * This class is an implementation of the HDRF partitioning algorithm. Paper: Petroni, Fabio, et al. "HDRF: Stream-Based
 * Partitioning for Power-Law Graphs."
 *
 * @author Hooman
 */
public class HdrfPartitioner {

  public static final double DEFAULT_LAMBDA = 0.5;
  public static final double DEFAULT_EPSILON = 0.01; // TODO: investigate about the default value of epsilon.

  /**
   * HDRF (High-Degree Replicated First) partitioning.
   *
   * @param edges input edges.
   * @param k number of partitions
   * @return
   */
  public static Partition[] partition(List<Tuple3<Long, Long, Double>> edges, int k) {
    return partition(edges, k, DEFAULT_LAMBDA, DEFAULT_EPSILON);
  }

  /**
   * HDRF (High-Degree Replicated First) partitioning.
   *
   * @param edges input edges.
   * @param k number of partitions
   * @param lambda
   * @param epsilon
   * @return
   */
  public static Partition[] partition(List<Tuple3<Long, Long, Double>> edges, int k, double lambda, double epsilon) {
    final HashMap<Long, Integer> vDegrees = new HashMap<>(); // Holds partial degree of each vertex.
    final Partition[] partitions = new Partition[k];
    for (int i = 0; i < k; i++) {
      partitions[i] = new Partition();
    }

    for (Tuple3<Long, Long, Double> t : edges) {
      long v1 = t.f0;
      long v2 = t.f1;

      int deltaV1 = updateVertexDegree(vDegrees, v1);
      int deltaV2 = updateVertexDegree(vDegrees, v2);
      double thetaV1 = (double) deltaV1 / (double) (deltaV1 + deltaV2);
      double thetaV2 = 1 - thetaV1;

      // Compute C score for each partition.
      double maxChdrf = Long.MIN_VALUE;
      Partition maxPartition = null;
      int maxSize = Integer.MIN_VALUE;
      int minSize = Integer.MAX_VALUE;

      for (Partition p : partitions) {
        double cRep = computeCReplication(p, v1, v2, thetaV1, thetaV2);
        double cBal = computeCBalance(p, maxSize, minSize, lambda, epsilon);

        double cHdrf = cRep + cBal;
        if (cHdrf > maxChdrf) {
          maxChdrf = cHdrf;
          maxPartition = p;
        }
      }
      maxPartition.addEdge(t);
      // This loop can be avoided. However, for readability purpose!
      for (Partition p : partitions) {
        int pSize = p.edgeSize();
        if (pSize > maxSize) {
          maxSize = p.edgeSize();
        }

        if (pSize < minSize) {
          minSize = pSize;
        }
      }
    }

    return partitions;
  }

  private static int updateVertexDegree(HashMap<Long, Integer> vDegrees, long v) {
    if (!vDegrees.containsKey(v)) {
      vDegrees.put(v, 1);
    } else {
      vDegrees.put(v, vDegrees.get(v) + 1);
    }

    return vDegrees.get(v);
  }

  private static double computeCReplication(Partition p, long v1, long v2, double thetaV1, double thetaV2) {
    return g(p, v1, thetaV1) + g(p, v2, thetaV2);
  }

  private static double g(Partition p, long v1, double thetaV1) {
    if (!p.containsVertex(v1)) {
      return 0;
    }

    return 1 + (1 - thetaV1);
  }

  private static double computeCBalance(Partition p, int maxSize, int minSize, double lambda, double epsilon) {
    return lambda * (maxSize - p.edgeSize()) / (epsilon + maxSize - minSize);
  }

}
