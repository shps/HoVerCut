package se.kth.scs.partitioning.algorithms;

import java.security.SecureRandom;
import java.util.Collection;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 *
 * @author Hooman
 */
public class UniformRandomPartitioner {

    /**
     * Uniform random vertex-cut partitioner.
     *
     * @param state
     * @param edges input edges.
     * @return
     */
    public static URState partition(URState state, Collection<Tuple3<Long, Long, Double>> edges) {
        int k = state.getNumberOfPartitions();
        SecureRandom r = new SecureRandom();
        for (Tuple3<Long, Long, Double> e : edges) {
            int p = r.nextInt(k);
//            state.addEdgeToPartition(p, e);
            //TODO: Implement this partitioner.
        }

        return state;
    }

}
