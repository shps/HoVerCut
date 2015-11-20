package se.kth.scs.partitioning.algorithms;

import se.kth.scs.partitioning.Partition;
import java.util.List;
import se.kth.scs.partitioning.Edge;

/**
 * The implementation of the Greedy Vertex-Cuts algorithm.
 *
 * Reference: Gonzalez, Joseph E., et al. "PowerGraph: Distributed Graph-Parallel Computation on Natural Graphs." OSDI.
 * Vol. 12. No. 1. 2012.
 *
 * @author Hooman
 */
public class GreedyPartitioner {

  public static Partition[] partition(List<Edge> edges, int k) {
    final Partition[] partitions = new Partition[k];
    for (short i = 0; i < k; i++) {
      partitions[i] = new Partition(i);
    }
    
    for (Edge e : edges) {
      
      
    }

    return partitions;
  }

//  private static List<Partition> find
}
