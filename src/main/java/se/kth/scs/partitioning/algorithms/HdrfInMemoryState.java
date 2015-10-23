package se.kth.scs.partitioning.algorithms;

import java.util.Collection;
import java.util.HashMap;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.Partition;

/**
 *
 * @author Hooman
 */
public class HdrfInMemoryState implements HdrfState {

    final HashMap<Long, Integer> vDegrees = new HashMap<>(); // Holds partial degree of each vertex.
    final Partition[] partitions;
    private final int k;

    public HdrfInMemoryState(int k) {
        this.k = k;
        partitions = new Partition[k];
        for (int i = 0; i < k; i++) {
            partitions[i] = new Partition(i);
        }
    }

    @Override
    public int updateVertexDegree(long v) {
        if (!vDegrees.containsKey(v)) {
            vDegrees.put(v, 1);
        } else {
            vDegrees.put(v, vDegrees.get(v) + 1);
        }

        return vDegrees.get(v);
    }

//    @Override
//    public double computeCReplication(int p, long v1, long v2, double thetaV1, double thetaV2) {
//        Partition partition = partitions[p];
//        return g(partition, v1, thetaV1) + g(partition, v2, thetaV2);
//    }
//
//    private double g(Partition p, long v1, double thetaV1) {
//        if (!p.containsVertex(v1)) {
//            return 0;
//        }
//
//        return 1 + (1 - thetaV1);
//    }
//    @Override
//    public double computeCBalance(int p, int maxSize, int minSize, double lambda, double epsilon) {
//        Partition partition = partitions[p];
//        return lambda * (maxSize - partition.edgeSize()) / (epsilon + maxSize - minSize);
//    }
    @Override
    public void addEdgeToPartition(int p, Tuple3<Long, Long, Double> edge) {
        partitions[p].addEdge(edge);
    }

    @Override
    public int getMaxPartitionEdgeSize() {
        int maxSize = 0;
        // This loop can be avoided. However, for readability purpose!
        for (Partition p : partitions) {
            int pSize = p.edgeSize();
            if (pSize > maxSize) {
                maxSize = p.edgeSize();
            }
        }

        return maxSize;
    }

    @Override
    public int getMinPartitionEdgeSize() {
        int minSize = 0;
        // This loop can be avoided. However, for readability purpose!
        for (Partition p : partitions) {
            int pSize = p.edgeSize();
            if (pSize < minSize) {
                minSize = pSize;
            }
        }

        return minSize;
    }

//    @Override
//    public Partition[] getPartitions() {
//        return partitions.clone();
//    }
    @Override
    public boolean partitionContainsVertex(int p, long v) {
        return partitions[p].containsVertex(v);
    }

    @Override
    public int getPartitionEdgeSize(int p) {
        return partitions[p].edgeSize();
    }

    @Override
    public int getPartitionVertexSize(int p) {
        return partitions[p].vertexSize();
    }

    @Override
    public int getMaxPartitionVertexSize() {
        int maxV = 0;
        for (Partition p : partitions) {
            if (p.vertexSize() > maxV) {
                maxV = p.vertexSize();
            }
        }

        return maxV;
    }

    @Override
    public int getTotalNumberOfReplicas() {
        HashMap<Long, Integer> vReplicas = new HashMap<>();
        for (Partition p : partitions) {
            for (Long vertexId : p.getVertices()) {
                if (!vReplicas.containsKey(vertexId)) {
                    vReplicas.put(vertexId, 1);
                } else {
                    vReplicas.put(vertexId, vReplicas.get(vertexId) + 1);
                }
            }
        }

        int sum = 0;
        Collection<Integer> rFactors = vReplicas.values();
        for (Integer rf : rFactors) {
            sum += rf;
        }
        return sum;
    }

    @Override
    public int getTotalNumberOfVertices() {
        return vDegrees.keySet().size();
    }

    @Override
    public int getTotalNumberOfEdges() {
        int sum = 0;
        for (Partition p : partitions) {
            sum += p.edgeSize();
        }

        return sum;
    }

    @Override
    public int getNumberOfPartitions() {
        return k;
    }

    @Override
    public void applyState() {
        
    }

    @Override
    public void releaseResources() {
        
    }
}
