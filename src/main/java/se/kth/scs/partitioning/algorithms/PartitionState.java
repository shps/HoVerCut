package se.kth.scs.partitioning.algorithms;

import org.apache.flink.api.java.tuple.Tuple3;

/**
 *
 * @author Hooman
 */
public interface PartitionState {

    public int getPartitionVertexSize(int p);

    public int getPartitionEdgeSize(int p);

    public int getTotalNumberOfReplicas();

    public int getTotalNumberOfVertices();

    public int getTotalNumberOfEdges();

    public int getNumberOfPartitions();

    public void addEdgeToPartition(int p, Tuple3<Long, Long, Double> e);

    public int getMaxPartitionVertexSize();

    public int getMaxPartitionEdgeSize();

    public void applyState();

    public void releaseResources();
}
