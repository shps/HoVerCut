package se.kth.scs.partitioning.algorithms;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.flink.api.java.tuple.Tuple3;
import se.kth.scs.partitioning.Partition;

/**
 *
 * @author Hooman
 */
public class URState implements PartitionState {
    
    // TODO: Merge this class with HdrfInMemoryState class and remove HdrfState interface.

    private final Partition[] partitions;
    private final int k;

    public URState(int k) {
        this.k = k;
        partitions = new Partition[k];
        for (int i = 0; i < k; i++) {
            partitions[i] = new Partition(i);
        }
    }

    @Override
    public int getPartitionVertexSize(int p) {
        return partitions[p].vertexSize();
    }

    @Override
    public int getPartitionEdgeSize(int p) {
        return partitions[p].edgeSize();
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
        HashSet<Long> vertices = new HashSet<>();
        for (Partition p : partitions) {
            vertices.addAll(Arrays.asList(p.getVertices()));
        }

        return vertices.size();
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

    @Override
    public void addEdgeToPartition(int p, Tuple3<Long, Long, Double> e) {
        partitions[p].addEdge(e);
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

}
