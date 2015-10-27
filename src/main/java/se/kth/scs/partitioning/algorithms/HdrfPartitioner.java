package se.kth.scs.partitioning.algorithms;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
     * @param lambda
     * @param epsilon
     * @param windowSize
     * @return
     */
    public static PartitionState partitionWithWindow(PartitionState state, Collection<Tuple3<Long, Long, Double>> edges, double lambda, double epsilon, int windowSize) {

        int counter = 1;
        List<Tuple3<Long, Long, Double>> edgeWindow = new LinkedList<>();
        Set<Long> vertices = new HashSet();
        for (Tuple3<Long, Long, Double> e : edges) {
            System.out.println(String.format("%d Received %d -> %d.", counter, e.f0, e.f1));
            edgeWindow.add(e);
            vertices.add(e.f0);
            vertices.add(e.f1);
            if (counter % windowSize == 0) {
                allocateNextWindow(edgeWindow, vertices, state, lambda, epsilon);
                edgeWindow.clear();
                vertices.clear();
            }
            counter++;
        }

        if (!edgeWindow.isEmpty()) {
            allocateNextWindow(edgeWindow, vertices, state, lambda, epsilon);
            edgeWindow.clear();
            vertices.clear();
        }

        System.out.println("******** Partitioning Finished **********");
        return state;
    }

//    private static void allocateNextEdge(Tuple3<Long, Long, Double> e, HdrfState state, double lambda, double epsilon) {
//        int maxSize;
//        int minSize;
//        long v1 = e.f0;
//        long v2 = e.f1;
//
//        int deltaV1 = state.updateVertexDegree(v1);
//        int deltaV2 = state.updateVertexDegree(v2);
//        double thetaV1 = (double) deltaV1 / (double) (deltaV1 + deltaV2);
//        double thetaV2 = 1 - thetaV1;
//        state.applyState();
//
//        // Compute C score for each partition.
//        double maxChdrf = Long.MIN_VALUE;
//        int maxPartition = Integer.MIN_VALUE;
//
//        maxSize = state.getMaxPartitionEdgeSize();
//        minSize = state.getMinPartitionEdgeSize();
//
//        for (int p = 0; p < state.getNumberOfPartitions(); p++) {
//            double cRep = computeCReplication(state, p, v1, v2, thetaV1, thetaV2);
//            double cBal = computeCBalance(state, p, maxSize, minSize, lambda, epsilon);
//
//            double cHdrf = cRep + cBal;
//            if (cHdrf > maxChdrf) {
//                maxChdrf = cHdrf;
//                maxPartition = p;
//            }
//        }
//        state.addEdgeToPartition(maxPartition, e);
//
//        state.applyState();
//    }
    private static Partition allocateNextEdge(Vertex v1, Vertex v2, List<Partition> partitions, double lambda, double epsilon) {

        int deltaV1 = v1.getpDegree();
        int deltaV2 = v2.getpDegree();
        double thetaV1 = (double) deltaV1 / (double) (deltaV1 + deltaV2);
        double thetaV2 = 1 - thetaV1;

        // Compute C score for each partition.
        double maxChdrf = Long.MIN_VALUE;
        Partition maxPartition = null;

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

        for (Partition p : partitions) {
            double cRep = computeCReplication(p, v1, v2, thetaV1, thetaV2);
            double cBal = computeCBalance(p, maxSize, minSize, lambda, epsilon);

            double cHdrf = cRep + cBal;
            if (cHdrf > maxChdrf) {
                maxChdrf = cHdrf;
                maxPartition = p;
            }
        }

        //MaxPartition should not be null
        if (v1.addPartition(maxPartition.getId())) {
            maxPartition.incrementVSize();
        }
        if (v2.addPartition(maxPartition.getId())) {
            maxPartition.incrementVSize();
        }
        maxPartition.incrementESize();

        return maxPartition;
    }

    public static double computeCReplication(Partition p, Vertex v1, Vertex v2, double thetaV1, double thetaV2) {
        return g(p, v1, thetaV1) + g(p, v2, thetaV2);
    }

    private static double g(Partition p, Vertex v1, double thetaV1) {
        if (v1.containsPartition(p.getId())) {
            return 0;
        }

        return 1 + (1 - thetaV1);
    }

    private static double computeCBalance(Partition p, int maxSize, int minSize, double lambda, double epsilon) {
        int edgeSize = p.getESize();
        return lambda * (maxSize - edgeSize) / (epsilon + maxSize - minSize);
    }

    private static void allocateNextWindow(List<Tuple3<Long, Long, Double>> edgeWindow, Set<Long> vIds, PartitionState state, double lambda, double epsilon) {
        Map<Long, Vertex> vertices = state.getVertices(vIds);
        List<Partition> partitions = state.getAllPartitions();

        for (Tuple3<Long, Long, Double> e : edgeWindow) {
            Vertex u = vertices.get(e.f0);
            Vertex v = vertices.get(e.f1);
            if (u == null) {
                u = new Vertex(e.f0, new HashSet<Integer>());
                vertices.put(u.getId(), u);
            }
            if (v == null) {
                v = new Vertex(e.f1, new HashSet<Integer>());
                vertices.put(v.getId(), v);
            }
            u.incrementDegree();
            v.incrementDegree();
            allocateNextEdge(u, v, partitions, lambda, epsilon);
        }

        //TODO: Inconsistency if update partitions and vertices separately.
        state.putPartitions(partitions);
        state.putVertices(vertices.values());
    }
}
