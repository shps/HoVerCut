package se.kth.scs.partitioning.algorithms;

import java.util.Collection;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * This class is an implementation of the HDRF partitioning algorithm. Paper:
 * Petroni, Fabio, et al. "HDRF: Stream-Based Partitioning for Power-Law
 * Graphs."
 *
 * @author Hooman
 */
public class HdrfPartitioner {

    public static final double DEFAULT_LAMBDA = 1;
    public static final double DEFAULT_EPSILON = 1;

    /**
     * HDRF (High-Degree Replicated First) partitioning.
     *
     * @param state
     * @param edges input edges.
     * @return
     */
    public static HdrfState partition(HdrfState state, List<Tuple3<Long, Long, Double>> edges) {
        return partition(state, edges, DEFAULT_LAMBDA, DEFAULT_EPSILON);
    }

    /**
     * HDRF (High-Degree Replicated First) partitioning.
     *
     * @param state
     * @param edges input edges.
     * @param lambda
     * @param epsilon
     * @return
     */
    public static HdrfState partition(HdrfState state, Collection<Tuple3<Long, Long, Double>> edges, double lambda, double epsilon) {

        int counter = 1;
        for (Tuple3<Long, Long, Double> t : edges) {
            System.out.println(String.format("%d\tAllocating %d -> %d.", counter, t.f0, t.f1));
            allocateNextEdge(t, state, lambda, epsilon);
            counter++;
        }

        System.out.println("******** Partitioning Finished **********");
        return state;
    }

    private static void allocateNextEdge(Tuple3<Long, Long, Double> e, HdrfState state, double lambda, double epsilon) {
        int maxSize;
        int minSize;
        long v1 = e.f0;
        long v2 = e.f1;

        int deltaV1 = state.updateVertexDegree(v1);
        int deltaV2 = state.updateVertexDegree(v2);
        double thetaV1 = (double) deltaV1 / (double) (deltaV1 + deltaV2);
        double thetaV2 = 1 - thetaV1;
        state.applyState();

        // Compute C score for each partition.
        double maxChdrf = Long.MIN_VALUE;
        int maxPartition = Integer.MIN_VALUE;

        maxSize = state.getMaxPartitionEdgeSize();
        minSize = state.getMinPartitionEdgeSize();

        for (int p = 0; p < state.getNumberOfPartitions(); p++) {
            double cRep = computeCReplication(state, p, v1, v2, thetaV1, thetaV2);
            double cBal = computeCBalance(state, p, maxSize, minSize, lambda, epsilon);

            double cHdrf = cRep + cBal;
            if (cHdrf > maxChdrf) {
                maxChdrf = cHdrf;
                maxPartition = p;
            }
        }
        state.addEdgeToPartition(maxPartition, e);

        state.applyState();
    }

    public static double computeCReplication(HdrfState state, int p, long v1, long v2, double thetaV1, double thetaV2) {
        return g(state, p, v1, thetaV1) + g(state, p, v2, thetaV2);
    }

    private static double g(HdrfState state, int p, long v1, double thetaV1) {
        if (!state.partitionContainsVertex(p, v1)) {
            return 0;
        }

        return 1 + (1 - thetaV1);
    }

    private static double computeCBalance(HdrfState state, int p, int maxSize, int minSize, double lambda, double epsilon) {
        int edgeSize = state.getPartitionEdgeSize(p);
        return lambda * (maxSize - edgeSize) / (epsilon + maxSize - minSize);
    }
}
