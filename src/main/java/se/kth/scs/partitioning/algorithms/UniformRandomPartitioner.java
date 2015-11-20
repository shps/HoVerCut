package se.kth.scs.partitioning.algorithms;

import java.security.SecureRandom;
import java.util.Collection;
import se.kth.scs.partitioning.Edge;

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
    public static URState partition(URState state, Collection<Edge> edges) {
        int k = state.getNumberOfPartitions();
        SecureRandom r = new SecureRandom();
        for (Edge e : edges) {
            int p = r.nextInt(k);
//            state.addEdgeToPartition(p, e);
            //TODO: Implement this partitioner.
        }

        return state;
    }

}
