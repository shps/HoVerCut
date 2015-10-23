package se.kth.scs.partitioning.algorithms;

import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.Partition;

/**
 * The implementation of the Greedy Vertex-Cuts algorithm.
 *
 * Reference: Gonzalez, Joseph E., et al. "PowerGraph: Distributed Graph-Parallel Computation on Natural Graphs." OSDI.
 * Vol. 12. No. 1. 2012.
 *
 * @author Hooman
 */
public class GreedyPartitioner {

  public static Partition[] partition(List<Tuple3<Long, Long, Double>> edges, int k) {
    final Partition[] partitions = new Partition[k];
    for (int i = 0; i < k; i++) {
      partitions[i] = new Partition(i);
    }
    
    for (Tuple3<Long, Long, Double> e : edges) {
      
      
    }

    return partitions;
  }

//  private static List<Partition> find
}
