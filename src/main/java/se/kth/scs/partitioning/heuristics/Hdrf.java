package se.kth.scs.partitioning.heuristics;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Hooman
 */
public class Hdrf implements Heuristic {

  private final double lambda;
  private final double epsilon;

  public Hdrf(double lambda, double epsilon) {
    this.epsilon = epsilon;
    this.lambda = lambda;
  }

  @Override
  public Partition allocateNextEdge(Vertex v1, Vertex v2, List<Partition> partitions) {
    int deltaV1 = v1.getpDegree();
    int deltaV2 = v2.getpDegree();
    if (deltaV1 == 0 || deltaV2 == 0) {
      System.err.println("Warning, vertex degree is zero!");
    }
    double thetaV1 = (double) deltaV1 / (double) (deltaV1 + deltaV2);
    double thetaV2 = 1 - thetaV1;

    // Compute C score for each partition.
    double maxScore = Long.MIN_VALUE;
//    Partition maxPartition = null;

    int maxSize = Integer.MIN_VALUE;
    int minSize = Integer.MAX_VALUE;

    for (Partition p : partitions) {
      if (p.getESize() > maxSize) {
        maxSize = p.getESize();
      }
      if (p.getESize() < minSize) {
        minSize = p.getESize();
      }
    }

    List<Partition> pList = new LinkedList<>();
    for (Partition p : partitions) {
      double sRep = computeReplicationScore(p, v1, v2, thetaV1, thetaV2);
      double sBal = computeBalanceScore(p, maxSize, minSize, lambda, epsilon);

      double score = sRep + sBal;
      if (score >= maxScore) {
        if (score > maxScore)
        {
          pList.clear();
        }
        maxScore = score;
//        maxPartition = p;
        pList.add(p);
      }
    }

    Random r = new Random();
    int index = r.nextInt(pList.size());
    return pList.get(index);
  }

  public static double computeReplicationScore(Partition p, Vertex v, Vertex u, double thetaV, double thetaU) {
    return g(p, v, thetaV) + g(p, u, thetaU);
  }

  private static double g(Partition p, Vertex v, double thetaV) {
    if (!v.containsPartition(p.getId())) {
      return 0;
    }

    return 1 + (1 - thetaV);
  }

  private static double computeBalanceScore(Partition p, int maxSize, int minSize, double lambda, double epsilon) {
    int edgeSize = p.getESize();
    return lambda * (maxSize - edgeSize) / (epsilon + maxSize - minSize);
  }

}
