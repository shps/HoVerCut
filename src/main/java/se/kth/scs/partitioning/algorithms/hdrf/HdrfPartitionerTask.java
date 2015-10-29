package se.kth.scs.partitioning.algorithms.hdrf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.Partition;
import se.kth.scs.partitioning.PartitionState;
import se.kth.scs.partitioning.Vertex;

/**
 *
 * @author Ganymedian
 */
public class HdrfPartitionerTask implements Runnable {

    private final Set<Tuple3<Long, Long, Double>> edges;
    private final double lambda;
    private final double epsilon;
    private final int windowSize;
    private final PartitionState state;

    public HdrfPartitionerTask(PartitionState state, Set<Tuple3<Long, Long, Double>> edges, double lambda, double epsilon, int windowSize) {
        this.edges = edges;
        this.lambda = lambda;
        this.epsilon = epsilon;
        this.windowSize = windowSize;
        this.state = state;
    }

    /**
     * HDRF (High-Degree Replicated First) partitioning.
     *
     * @return
     */
    public PartitionState partitionWithWindow() {

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

        System.out.println("******** Task Finished **********");
        return state;
    }

//    /**
//     * Partitions without using any window to buffer vertices.
//     *
//     * @return
//     */
//    public PartitionState partition() {
//
//        int counter = 1;
//        for (Tuple3<Long, Long, Double> e : edges) {
//            System.out.println(String.format("%d Received %d -> %d.", counter, e.f0, e.f1));
//            Vertex u = state.getVertex(e.f0);
//            Vertex v = state.getVertex(e.f1);
//            List<Partition> partitions = state.getAllPartitions();
//            if (u == null) {
//                u = new Vertex(e.f0, new HashSet<Integer>());
//            }
//            if (v == null) {
//                v = new Vertex(e.f1, new HashSet<Integer>());
//            }
//            u.incrementDegree();
//            v.incrementDegree();
//            allocateNextEdge(u, v, partitions, lambda, epsilon);
//            //TODO: Inconsistency if update partitions and vertices separately.
//            state.putPartitions(partitions);
//            Collection<Vertex> vertices = new ArrayList<>(2);
//            vertices.add(u);
//            vertices.add(v);
//            state.putVertices(vertices);
//            counter++;
//        }
//
//        System.out.println("******** Partitioning Finished **********");
//        return state;
//    }
    private void allocateNextWindow(List<Tuple3<Long, Long, Double>> edgeWindow, Set<Long> vIds, PartitionState state, double lambda, double epsilon) {
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

    private Partition allocateNextEdge(Vertex v1, Vertex v2, List<Partition> partitions, double lambda, double epsilon) {

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
        if (!v1.containsPartition(p.getId())) {
            return 0;
        }

        return 1 + (1 - thetaV1);
    }

    private static double computeCBalance(Partition p, int maxSize, int minSize, double lambda, double epsilon) {
        int edgeSize = p.getESize();
        return lambda * (maxSize - edgeSize) / (epsilon + maxSize - minSize);
    }

    @Override
    public void run() {
        partitionWithWindow();
        state.releaseTaskResources();
    }

}
