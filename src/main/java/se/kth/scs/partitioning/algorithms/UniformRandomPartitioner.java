package se.kth.scs.partitioning.algorithms;

import java.security.SecureRandom;
import java.util.Collection;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.Partition;

/**
 *
 * @author Hooman
 */
public class UniformRandomPartitioner {

  /**
   * Uniform random vertex-cut partitioner.
   *
   * @param edges input edges.
   * @param k number of partitions
   * @return
   */
  public static Partition[] partition(Collection<Tuple3<Long, Long, Double>> edges, int k) {
    final Partition[] partitions = new Partition[k];
    for (int i = 0; i < k; i++) {
      partitions[i] = new Partition();
    }

    SecureRandom r = new SecureRandom();
    for (Tuple3<Long, Long, Double> e : edges) {
      int p = r.nextInt(k);
      partitions[p].addEdge(e);
    }

    return partitions;
  }

}
