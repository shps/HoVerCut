package se.kth.scs.partitioning.algorithms;

/**
 *
 * @author Hooman
 */
public interface HdrfState extends PartitionState {

    public int updateVertexDegree(long v);

    public int getMinPartitionEdgeSize();

    public boolean partitionContainsVertex(int p, long v);
}
